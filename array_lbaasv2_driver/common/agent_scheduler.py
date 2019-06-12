#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from oslo_log import log as logging

from neutron_lbaas import agent_scheduler
from neutron_lbaas.extensions import lbaas_agentschedulerv2

LOG = logging.getLogger(__name__)


class ArrayScheduler(agent_scheduler.ChanceScheduler):
    """Finds an available agent."""

    def __init__(self):
        """Initialze with the ChanceScheduler base class."""
        super(ArrayScheduler, self).__init__()

    def get_lbaas_agent_hosting_loadbalancer(self, plugin, context,
                                             loadbalancer_id):
        """Return the agent that is hosting the loadbalancer."""
        LOG.debug('Getting agent for loadbalancer %s' % loadbalancer_id)

        with context.session.begin(subtransactions=True):
            # returns {'agent': agent_dict}
            lbaas_agent = plugin.db.get_agent_hosting_loadbalancer(
                context,
                loadbalancer_id
            )
            # if the agent bound to this loadbalancer is alive, return it
            if lbaas_agent is not None:
                if (not lbaas_agent['agent']['alive'] or
                        not lbaas_agent['agent']['admin_state_up']):

                    reassigned_agent = self.rebind_loadbalancers(
                        context, plugin, lbaas_agent['agent'])
                    if reassigned_agent:
                        lbaas_agent = {'agent': reassigned_agent}

            return lbaas_agent

    def rebind_loadbalancers(self, context, plugin, current_agent):
        agents = self.get_agents(context, plugin, active=True)
        if agents:
            reassigned_agent = agents[0]
            bindings = \
                context.session.query(
                    agent_scheduler.LoadbalancerAgentBinding).filter_by(
                        agent_id=current_agent['id']).all()
            for binding in bindings:
                binding.agent_id = reassigned_agent['id']
                context.session.add(binding)
            LOG.debug("%s Loadbalancers bound to agent %s now bound to %s" %
                      (len(bindings),
                       current_agent['id'],
                       reassigned_agent['id']))
            return reassigned_agent
        else:
            return None

    def get_dead_agents(self, context, plugin):
        return_agents = []
        all_agents = self.get_agents(context, plugin, active=None)

        for agent in all_agents:

            if not plugin.db.is_eligible_agent(active=True, agent=agent):
                agent_dead = plugin.db.is_agent_down(
                    agent['heartbeat_timestamp'])
                if not agent['admin_state_up'] or agent_dead:
                    return_agents.append(agent)
        return return_agents

    def scrub_dead_agents(self, context, plugin):
        dead_agents = self.get_dead_agents(context, plugin)
        for agent in dead_agents:
            self.rebind_loadbalancers(context, plugin, agent)

    def get_agents(self, context, plugin, active=None):
        """Get an active agents."""
        with context.session.begin(subtransactions=True):
            candidates = []
            try:
                candidates = plugin.db.get_lbaas_agents(context, active=active)
            except Exception as ex:
                LOG.error("Exception retrieving agent candidates for "
                          "scheduling: {}".format(ex))

        return candidates

    def get_agents_hosts(self, context, plugin):
        """Get an active agents."""
        with context.session.begin(subtransactions=True):
            candidates = []
            try:
                candidates = plugin.db.get_lbaas_agents(context)
            except Exception as ex:
                LOG.error("Exception retrieving agent candidates for "
                          "scheduling: {}".format(ex))
        return candidates

    def schedule(self, plugin, context, loadbalancer_id, env=None):
        """Schedule the loadbalancer to an active loadbalancer agent.

        If there is no enabled agent hosting it.
        """

        with context.session.begin(subtransactions=True):
            loadbalancer = plugin.db.get_loadbalancer(context, loadbalancer_id)
            # If the loadbalancer is hosted on an active agent
            # already, return that agent or one in its env
            lbaas_agent = self.get_lbaas_agent_hosting_loadbalancer(
                plugin,
                context,
                loadbalancer.id
            )

            if lbaas_agent:
                lbaas_agent = lbaas_agent['agent']
                LOG.debug(' Assigning task to agent %s.'
                          % (lbaas_agent['id']))
                return lbaas_agent

            # There is no existing loadbalancer agent binding.
            # Find all active agent candidates in this env.
            candidates = self.get_agents(
                context,
                plugin,
                active=True
            )

            LOG.debug("candidate agents: %s", candidates)
            if len(candidates) == 0:
                LOG.error('No array lbaas agents are active for env %s' % env)
                raise lbaas_agentschedulerv2.NoActiveLbaasAgent(
                    loadbalancer_id=loadbalancer.id)

            # We have active candidates to choose from.
            # Qualify them by tenant affinity and then capacity.
            chosen_agent = None

            for candidate in candidates:
                # Do we already have this tenant assigned to this
                # agent candidate? If we do and it has capacity
                # then assign this loadbalancer to this agent.
                assigned_lbs = plugin.db.list_loadbalancers_on_lbaas_agent(
                    context, candidate['id'])
                for assigned_lb in assigned_lbs:
                    if loadbalancer.tenant_id == assigned_lb.tenant_id:
                        chosen_agent = candidate
                        break

            # If we don't have an agent with capacity associated
            # with our tenant_id, let's pick an agent based on
            # the group with the lowest capacity score.
            if not chosen_agent:
                pass

            # If there are no agents with available capacity, raise exception
            if not chosen_agent:
                LOG.warn('No capacity left on any agents in env: %s' % env)
                raise lbaas_agentschedulerv2.NoEligibleLbaasAgent(
                    loadbalancer_id=loadbalancer.id)

            binding = agent_scheduler.LoadbalancerAgentBinding()
            binding.agent = chosen_agent
            binding.loadbalancer_id = loadbalancer.id
            context.session.add(binding)

            LOG.debug(('Loadbalancer %(loadbalancer_id)s is scheduled to '
                       'lbaas agent %(agent_id)s'),
                      {'loadbalancer_id': loadbalancer.id,
                       'agent_id': chosen_agent['id']})

            return chosen_agent
