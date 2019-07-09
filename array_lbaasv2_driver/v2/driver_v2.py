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

import array_lbaasv2_driver
from array_lbaasv2_driver.common.driver_v2 import ArrayDriverV2

from oslo_log import log as logging

from neutron_lbaas.drivers import driver_base

VERSION = "1.0.0"
LOG = logging.getLogger(__name__)


class ArrayLBaaSV2Driver(driver_base.LoadBalancerBaseDriver):

    def __init__(self, plugin, environment = None):
        super(ArrayLBaaSV2Driver, self).__init__(plugin)

        self.load_balancer = LoadBalancerManager(self)
        self.listener = ListenerManager(self)
        self.pool = PoolManager(self)
        self.member = MemberManager(self)
        self.health_monitor = HealthMonitorManager(self)
        self.l7policy = L7PolicyManager(self)
        self.l7rule = L7RuleManager(self)

        LOG.debug("ArrayLBaaSV2Driver: initializing, version=%s, impl=%s"
                  % (VERSION, array_lbaasv2_driver.__version__))

        self.array = ArrayDriverV2(plugin, self, environment)


class LoadBalancerManager(driver_base.BaseLoadBalancerManager):

    def create(self, context, lb):
        self.driver.array.loadbalancer.create(context, lb)

    def update(self, context, old_lb, lb):
        self.driver.array.loadbalancer.update(context, old_lb, lb)

    def delete(self, context, lb):
        self.driver.array.loadbalancer.delete(context, lb)

    def refresh(self, context, lb):
        self.driver.array.loadbalancer.refresh(context, lb)

    def stats(self, context, lb):
        return self.driver.array.loadbalancer.stats(context, lb)


class ListenerManager(driver_base.BaseListenerManager):

    def create(self, context, listener):
        self.driver.array.listener.create(context, listener)

    def update(self, context, old_listener, listener):
        self.driver.array.listener.update(context, old_listener, listener)

    def delete(self, context, listener):
        self.driver.array.listener.delete(context, listener)


class PoolManager(driver_base.BasePoolManager):

    def create(self, context, pool):
        self.driver.array.pool.create(context, pool)

    def update(self, context, old_pool, pool):
        self.driver.array.pool.update(context, old_pool, pool)

    def delete(self, context, pool):
        self.driver.array.pool.delete(context, pool)


class MemberManager(driver_base.BaseMemberManager):

    def create(self, context, member):
        self.driver.array.member.create(context, member)

    def update(self, context, old_member, member):
        self.driver.array.member.update(context, old_member, member)

    def delete(self, context, member):
        self.driver.array.member.delete(context, member)


class HealthMonitorManager(driver_base.BaseHealthMonitorManager):

    def create(self, context, health_monitor):
        self.driver.array.healthmonitor.create(context, health_monitor)

    def update(self, context, old_health_monitor, health_monitor):
        self.driver.array.healthmonitor.update(context, old_health_monitor,
                                   health_monitor)

    def delete(self, context, health_monitor):
        self.driver.array.healthmonitor.delete(context, health_monitor)


class L7PolicyManager(driver_base.BaseL7PolicyManager):

    def create(self, context, l7policy):
        self.driver.array.l7policy.create(context, l7policy)

    def update(self, context, old_l7policy, l7policy):
        self.driver.array.l7policy.update(context, old_l7policy, l7policy)

    def delete(self, context, l7policy):
        self.driver.array.l7policy.delete(context, l7policy)


class L7RuleManager(driver_base.BaseL7RuleManager):

    def create(self, context, l7rule):
        self.driver.array.l7rule.create(context, l7rule)

    def update(self, context, old_l7rule, l7rule):
        self.driver.array.l7rule.update(context, old_l7rule, l7rule)

    def delete(self, context, l7rule):
        self.driver.array.l7rule.delete(context, l7rule)
