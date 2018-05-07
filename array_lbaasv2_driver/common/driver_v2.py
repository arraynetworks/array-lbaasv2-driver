#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

import sys

from oslo_config import cfg
from oslo_log import helpers as log_helpers
from oslo_log import log as logging
from oslo_utils import importutils

from neutron.common import rpc as n_rpc
from neutron.db import agents_db
from neutron.plugins.common import constants as plugin_constants
from neutron_lib import constants as lb_const

from neutron_lbaas.db.loadbalancer import models
from neutron_lbaas.extensions import lbaas_agentschedulerv2

from arraylbaasv2driver.common import plugin_rpc
from arraylbaasv2driver.common import agent_rpc
from arraylbaasv2driver.common import constants_v2
from arraylbaasv2driver.common import exceptions as array_exc

LOG = logging.getLogger(__name__)

OPTS = [
    cfg.StrOpt(
        'loadbalancer_scheduler_driver',
        default=(
            'neutron_lbaas.agent_scheduler.ChanceScheduler'
        ),
        help=('Driver to use for scheduling '
              'pool to a default loadbalancer agent')
    )
]

cfg.CONF.register_opts(OPTS)


class ArrayDriverV2(object):
    """Array Networks LBaaSv2 Driver."""

    def __init__(self, plugin=None, driver = None):
        """Driver initialization."""
        if not plugin or not driver:
            LOG.error('Required LBaaS Driver and Core Driver Missing')
            sys.exit(1)

        self.plugin = plugin
        self.conn = None

        self.loadbalancer = LoadBalancerManager(self)
        self.listener = ListenerManager(self)
        self.pool = PoolManager(self)
        self.member = MemberManager(self)
        self.healthmonitor = HealthMonitorManager(self)
        self.l7policy = L7PolicyManager(self)
        self.l7rule = L7RuleManager(self)

        # what scheduler to use for pool selection
        self.scheduler = importutils.import_object(
            cfg.CONF.loadbalancer_scheduler_driver)

        self.agent_rpc = agent_rpc.LBaaSv2AgentRPC(self)

        self.agent_endpoints = [
            plugin_rpc.ArrayLoadBalancerCallbacks(driver),
            agents_db.AgentExtRpcCallback(self.plugin.db)
        ]

        self.plugin.agent_notifiers.update(
            {lb_const.AGENT_TYPE_LOADBALANCER: self.agent_rpc})

        self.start_rpc_listeners()

    def start_rpc_listeners(self):
        # other agent based plugin driver might already set callbacks on plugin
        if hasattr(self.plugin, 'agent_callbacks'):
            return

        self.conn = n_rpc.create_connection()
        self.conn.create_consumer(constants_v2.TOPIC_PROCESS_ON_HOST_V2,
                                  self.agent_endpoints,
                                  fanout=False)
        return self.conn.consume_in_threads()


    def _handle_driver_error(self, context, loadbalancer,
                             loadbalancer_id, status):
        pass

class BaseManager(object):
    '''Parent for all managers defined in this module.'''

    def __init__(self, driver):
        self.driver = driver
        self.api_dict = None
        self.loadbalancer = None

    def _call_rpc(self, context, entity, rpc_method):
        '''Perform operations common to create and delete for managers.'''

        try:
            agent_host = self._setup_crud(context, entity)
            rpc_callable = getattr(self.driver.agent_rpc, rpc_method)
            rpc_callable(context, self.api_dict, agent_host)
        except (lbaas_agentschedulerv2.NoEligibleLbaasAgent,
                lbaas_agentschedulerv2.NoActiveLbaasAgent) as e:
            LOG.error("Exception: %s: %s" % (rpc_method, e))
        except Exception as e:
            LOG.error("Exception: %s: %s" % (rpc_method, e))
            raise e

    def _setup_crud(self, context, entity):
        '''Setup CRUD operations for managers to make calls to agent.

        :param context: auth context for performing CRUD operation
        :param entity: neutron lbaas entity -- target of the CRUD operation
        :returns: tuple -- (agent object, service dict)
        :raises: ArrayNoAttachedLoadbalancerException
        '''

        if entity.attached_to_loadbalancer() and self.loadbalancer:
            agent = self._schedule_agent_create_service(context)
            if agent is None:
                return None
            else:
                return agent['host']

        raise array_exc.ArrayNoAttachedLoadbalancerException()

    def _schedule_agent_create_service(self, context):
        '''Schedule agent and build service--used for most managers.

        :param context: auth context for performing crud operation
        :returns: agent object
        '''

        agent = self.driver.scheduler.schedule(
            self.driver.plugin,
            context,
            self.loadbalancer,
            "array"
        )
        return agent


class LoadBalancerManager(BaseManager):
    """LoadBalancerManager class handles Neutron LBaaS CRUD."""

    @log_helpers.log_method_call
    def create(self, context, loadbalancer):
        """Create a loadbalancer."""
        driver = self.driver
        self.loadbalancer = loadbalancer
        try:
            agent = self._schedule_agent_create_service(context)

            driver.agent_rpc.create_loadbalancer(
                context, loadbalancer, agent['host'])
        except (lbaas_agentschedulerv2.NoEligibleLbaasAgent,
                lbaas_agentschedulerv2.NoActiveLbaasAgent) as e:
            LOG.error("Exception: loadbalancer create: %s" % e)
            driver.plugin.db.update_status(
                context,
                models.LoadBalancer,
                loadbalancer.id,
                plugin_constants.ERROR)
        except Exception as e:
            LOG.error("Exception: loadbalancer create: %s" % e.message)
            raise e

    @log_helpers.log_method_call
    def update(self, context, old_loadbalancer, loadbalancer):
        """Update a loadbalancer."""
        driver = self.driver
        self.loadbalancer = loadbalancer
        try:
            driver.agent_rpc.update_loadbalancer(
                context,
                old_loadbalancer,
                loadbalancer,
                None
            )
        except (lbaas_agentschedulerv2.NoEligibleLbaasAgent,
                lbaas_agentschedulerv2.NoActiveLbaasAgent) as e:
            LOG.error("Exception: loadbalancer update: %s" % e)
            driver._handle_driver_error(context,
                                        models.LoadBalancer,
                                        loadbalancer.id,
                                        plugin_constants.ERROR)
        except Exception as e:
            LOG.error("Exception: loadbalancer update: %s" % e.message)
            raise e

    @log_helpers.log_method_call
    def delete(self, context, loadbalancer):
        """Delete a loadbalancer."""
        driver = self.driver
        self.loadbalancer = loadbalancer
        try:
            driver.agent_rpc.delete_loadbalancer(
                context, loadbalancer, None)

        except (lbaas_agentschedulerv2.NoEligibleLbaasAgent,
                lbaas_agentschedulerv2.NoActiveLbaasAgent) as e:
            LOG.error("Exception: loadbalancer delete: %s" % e)
            driver.plugin.db.delete_loadbalancer(context, loadbalancer.id)
        except Exception as e:
            LOG.error("Exception: loadbalancer delete: %s" % e)
            raise e

    @log_helpers.log_method_call
    def refresh(self, context, loadbalancer):
        """Refresh a loadbalancer."""
        pass

    @log_helpers.log_method_call
    def stats(self, context, loadbalancer):
        driver = self.driver
        try:
            driver.agent_rpc.update_loadbalancer_stats(
                context,
                loadbalancer,
                None
            )
        except (lbaas_agentschedulerv2.NoEligibleLbaasAgent,
                lbaas_agentschedulerv2.NoActiveLbaasAgent) as e:
            LOG.error("Exception: update_loadbalancer_stats: %s" % e.message)
            driver._handle_driver_error(context,
                                        models.LoadBalancer,
                                        loadbalancer.id,
                                        plugin_constants.ERROR)
        except Exception as e:
            LOG.error("Exception: update_loadbalancer_stats: %s" % e.message)
            raise e


class ListenerManager(BaseManager):
    """ListenerManager class handles Neutron LBaaS listener CRUD."""

    @log_helpers.log_method_call
    def create(self, context, listener):
        """Create a listener."""

        self.loadbalancer = listener.loadbalancer
        self.api_dict = listener.to_dict()
        LOG.debug("create listener: --%s--" % self.api_dict)
        self._call_rpc(context, listener, 'create_listener')

    @log_helpers.log_method_call
    def update(self, context, old_listener, listener):
        """Update a listener."""

        driver = self.driver
        self.loadbalancer = listener.loadbalancer
        try:
            agent_host = self._setup_crud(context, listener)
            driver.agent_rpc.update_listener(
                context,
                old_listener.to_dict(),
                listener.to_dict(),
                agent_host
            )
        except Exception as e:
            LOG.error("Exception: listener update: %s" % e.message)
            raise e

    @log_helpers.log_method_call
    def delete(self, context, listener):
        """Delete a listener."""

        self.loadbalancer = listener.loadbalancer
        self.api_dict = listener.to_dict()
        LOG.debug("create listener: --%s--" % self.api_dict)
        self._call_rpc(context, listener, 'delete_listener')


class PoolManager(BaseManager):
    """PoolManager class handles Neutron LBaaS pool CRUD."""

    def _get_pool_dict(self, pool):
        pool_dict = pool.to_dict(
            listener=False,
            listeners=False,
            #loadbalancer=False,
            healthmonitor=False,
            members=False,
            l7_policies=False)
        pool_dict['provisioning_status'] = pool.provisioning_status
        pool_dict['operating_status'] = pool.operating_status
        return pool_dict

    @log_helpers.log_method_call
    def create(self, context, pool):
        """Create a pool."""

        self.loadbalancer = pool.loadbalancer
        self.api_dict = self._get_pool_dict(pool)
        self._call_rpc(context, pool, 'create_pool')

    @log_helpers.log_method_call
    def update(self, context, old_pool, pool):
        """Update a pool."""

        driver = self.driver
        self.loadbalancer = pool.loadbalancer
        try:
            agent_host = self._setup_crud(context, pool)
            driver.agent_rpc.update_pool(
                context,
                self._get_pool_dict(old_pool),
                self._get_pool_dict(pool),
                agent_host
            )
        except Exception as e:
            LOG.error("Exception: pool update: %s" % e.message)
            raise e

    @log_helpers.log_method_call
    def delete(self, context, pool):
        """Delete a pool."""

        self.loadbalancer = pool.loadbalancer
        self.api_dict = self._get_pool_dict(pool)
        self._call_rpc(context, pool, 'delete_pool')


class MemberManager(BaseManager):
    """MemberManager class handles Neutron LBaaS pool member CRUD."""

    def _get_member_dict(self, member):
        member_dict = member.to_dict(
            listener=False,
            listeners=False,
            #loadbalancer=False,
            healthmonitor=False,
            members=False,
            l7_policies=False)
        return member_dict

    @log_helpers.log_method_call
    def create(self, context, member):
        """Create a member."""

        self.loadbalancer = member.pool.loadbalancer
        self.api_dict = self._get_member_dict(member)
        LOG.debug("create member: --%s--" % self.api_dict)
        self._call_rpc(context, member, 'create_member')

    @log_helpers.log_method_call
    def update(self, context, old_member, member):
        """Update a member."""

        driver = self.driver
        self.loadbalancer = member.pool.loadbalancer
        try:
            agent_host = self._setup_crud(context, member)
            driver.agent_rpc.update_member(
                context,
                self._get_member_dict(old_member),
                self._get_member_dict(member),
                agent_host
            )
        except Exception as e:
            LOG.error("Exception: member update: %s" % e.message)
            raise e

    @log_helpers.log_method_call
    def delete(self, context, member):
        """Delete a member."""
        self.loadbalancer = member.pool.loadbalancer
        driver = self.driver
        try:
            agent_host = self._setup_crud(context, member)
            driver.agent_rpc.delete_member(
                context, self._get_member_dict(member), agent_host)
        except Exception as e:
            LOG.error("Exception: member delete: %s" % e.message)
            raise e


class HealthMonitorManager(BaseManager):
    """HealthMonitorManager class handles Neutron LBaaS monitor CRUD."""

    def _get_hm_dict(self, hm):
        hm_dict = hm.to_dict(
            listener=False,
            listeners=False,
            #loadbalancer=False,
            healthmonitor=False,
            members=False,
            l7_policies=False)
        return hm_dict

    @log_helpers.log_method_call
    def create(self, context, health_monitor):
        """Create a health monitor."""

        self.loadbalancer = health_monitor.pool.loadbalancer
        self.api_dict = self._get_hm_dict(health_monitor)
        self._call_rpc(context, health_monitor, 'create_health_monitor')

    @log_helpers.log_method_call
    def update(self, context, old_health_monitor, health_monitor):
        """Update a health monitor."""

        driver = self.driver
        self.loadbalancer = health_monitor.pool.loadbalancer
        try:
            agent_host = self._setup_crud(context, health_monitor)
            driver.agent_rpc.update_health_monitor(
                context,
                self._get_hm_dict(old_health_monitor),
                self._get_hm_dict(health_monitor),
                agent_host
            )
        except Exception as e:
            LOG.error("Exception: health monitor update: %s" % e.message)
            raise e

    @log_helpers.log_method_call
    def delete(self, context, health_monitor):
        """Delete a health monitor."""

        self.loadbalancer = health_monitor.pool.loadbalancer
        self.api_dict = self._get_hm_dict(health_monitor)
        self._call_rpc(context, health_monitor, 'delete_health_monitor')


class L7PolicyManager(BaseManager):
    """L7PolicyManager class handles Neutron LBaaS L7 Policy CRUD."""

    @log_helpers.log_method_call
    def create(self, context, policy):
        """Create an L7 policy."""

        self.loadbalancer = policy.listener.loadbalancer
        self.api_dict = policy.to_dict(listener=False, rules=False)
        self._call_rpc(context, policy, 'create_l7policy')

    @log_helpers.log_method_call
    def update(self, context, old_policy, policy):
        """Update a policy."""

        driver = self.driver
        self.loadbalancer = policy.listener.loadbalancer
        try:
            agent_host = self._setup_crud(context, policy)
            driver.agent_rpc.update_l7policy(
                context,
                old_policy.to_dict(listener=False),
                policy.to_dict(listener=False),
                agent_host
            )
        except Exception as e:
            LOG.error("Exception: l7policy update: %s" % e.message)
            raise e

    @log_helpers.log_method_call
    def delete(self, context, policy):
        """Delete a policy."""

        self.loadbalancer = policy.listener.loadbalancer
        self.api_dict = policy.to_dict(listener=False, rules=False)
        self._call_rpc(context, policy, 'delete_l7policy')


class L7RuleManager(BaseManager):
    """L7RuleManager class handles Neutron LBaaS L7 Rule CRUD."""

    @log_helpers.log_method_call
    def create(self, context, rule):
        """Create an L7 rule."""

        self.loadbalancer = rule.policy.listener.loadbalancer
        self.api_dict = rule.to_dict(policy=False)
        self._call_rpc(context, rule, 'create_l7rule')

    @log_helpers.log_method_call
    def update(self, context, old_rule, rule):
        """Update a rule."""

        driver = self.driver
        self.loadbalancer = rule.policy.listener.loadbalancer
        try:
            agent_host = self._setup_crud(context, rule)
            driver.agent_rpc.update_l7rule(
                context,
                old_rule.to_dict(policy=False),
                rule.to_dict(policy=False),
                agent_host
            )
        except Exception as e:
            LOG.error("Exception: l7rule update: %s" % e.message)
            raise e

    @log_helpers.log_method_call
    def delete(self, context, rule):
        """Delete a rule."""

        self.loadbalancer = rule.policy.listener.loadbalancer
        self.api_dict = rule.to_dict(policy=False)
        self._call_rpc(context, rule, 'delete_l7rule')
