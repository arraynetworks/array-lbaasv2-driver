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

import json

from oslo_log import log as logging

from neutron_lbaas import agent_scheduler
from neutron_lbaas.extensions import lbaas_agentschedulerv2
from neutron.agent.common import utils as agent_utils

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

    def rebind_loadbalancers(self, context, plugin, current_agent, environment=None):
        # TODO: Here, it should be active array agents
        agents = self.get_array_agent_candidates(context, plugin, environment)
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
        all_agents = self.get_all_agents(context, plugin, active=None)

        for agent in all_agents:

            if not plugin.db.is_eligible_agent(active=True, agent=agent):
                agent_dead = agent_utils.is_agent_down(
                    agent['heartbeat_timestamp'])
                if not agent['admin_state_up'] or agent_dead:
                    return_agents.append(agent)
        return return_agents


    def scrub_dead_agents(self, context, plugin, environment=None):
        dead_agents = self.get_dead_agents(context, plugin)
        for agent in dead_agents:
            self.rebind_loadbalancers(context, plugin, agent, environment)


    def deserialize_agent_configurations(self, agent_conf):
        if not isinstance(agent_conf, dict):
            try:
                agent_conf = json.loads(agent_conf)
            except ValueError as ve:
                LOG.error("Can't decode JSON %s : %s"
                          % (agent_conf, ve.message))
                return {}
        return agent_conf


    def get_array_agent_candidates(self, context, plugin, environment):
        active_agents = self.get_all_agents(context, plugin, active=True)
        device_driver = "array"
        with context.session.begin(subtransactions=True):
            return_candidates = []
            try:
                candidates = plugin.db.get_lbaas_agent_candidates(device_driver, active_agents)
                for candidate in candidates:
                    ac = self.deserialize_agent_configurations(
                        candidate['configurations'])
                    if 'environment' in ac:
                        if environment and ac['environment'] == environment:
                            return_candidates.append(candidate)
                    else:
                        if not environment:
                            return_candidates.append(candidate)
            except Exception as ex:
                LOG.error("Exception retrieving agent candidates for "
                          "scheduling: {}".format(ex))
        return return_candidates


    def get_all_agents(self, context, plugin, active=None):
        with context.session.begin(subtransactions=True):
            candidates = []
            try:
                candidates = plugin.db.get_lbaas_agents(context, active=active)
            except Exception as ex:
                LOG.error("Exception retrieving agent candidates for "
                          "scheduling: {}".format(ex))

        return candidates


    def schedule(self, context, plugin, loadbalancer_id, environment=None):
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
            candidates = self.get_array_agent_candidates(
                context,
                plugin,
                environment
            )

            LOG.debug("candidate agents: %s", candidates)
            if len(candidates) == 0:
                LOG.error('No array lbaas agents are active.')
                raise lbaas_agentschedulerv2.NoActiveLbaasAgent(
                    loadbalancer_id=loadbalancer.id)

            chosen_agent = candidates[0]

            # If there are no agents with available capacity, raise exception
            if not chosen_agent:
                LOG.warn('No capacity left on any agents')
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
