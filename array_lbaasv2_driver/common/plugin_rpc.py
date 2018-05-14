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
import logging

from neutron_lib import constants as n_const
from neutron_lbaas.services.loadbalancer import data_models

from array_lbaasv2_driver.common import db

LOG = logging.getLogger(__name__)


class ArrayLoadBalancerCallbacks(object):
    """Callbacks made by the agent to update the data model."""

    RPC_API_VERSION = '1.0'

    # class properties
    OBJ_TYPE_LB = "loadbalancer"
    OBJ_TYPE_LISTENER = "listener"
    OBJ_TYPE_POOL = "pool"
    OBJ_TYPE_MEMBER = "member"
    OBJ_TYPE_HM = "hm"

    def __init__(self, driver):
        LOG.debug('Apv status callbacks RPC subscriber initialized')
        self.driver = driver

        self._table = {
            "loadbalancer.success": self.driver.load_balancer.successful_completion,
            "loadbalancer.delete": self.driver.load_balancer.successful_completion,
            "loadbalancer.fail": self.driver.load_balancer.failed_completion,

            "listener.success": self.driver.listener.successful_completion,
            "listener.delete": self.driver.listener.successful_completion,
            "listener.fail": self.driver.listener.failed_completion,

            "pool.success": self.driver.pool.successful_completion,
            "pool.delete": self.driver.pool.successful_completion,
            "pool.fail": self.driver.pool.failed_completion,

            "member.success": self.driver.member.successful_completion,
            "member.delete": self.driver.member.successful_completion,
            "member.fail": self.driver.member.failed_completion,

            "hm.success": self.driver.health_monitor.successful_completion,
            "hm.delete": self.driver.health_monitor.successful_completion,
            "hm.fail": self.driver.health_monitor.failed_completion,

            "loadbalancer.model": data_models.LoadBalancer,
            "listener.model": data_models.Listener,
            "pool.model": data_models.Pool,
            "member.model": data_models.Member,
            "hm.model": data_models.HealthMonitor,
        }

    def _successful_completion(self, context, obj_type, obj, delete=False,
            lb_create=False):
        success = self._table.get(obj_type+".success", None)
        model = self._table.get(obj_type+".model", None)
        if success:
            if not isinstance(obj, model):
                obj = model.from_dict(obj)
            success(context, obj, delete, lb_create)
            if lb_create and isinstance(obj, data_models.LoadBalancer):
                self.driver.plugin.db.update_loadbalancer(
                    context, obj.id, {'vip_address': obj.vip_address,
                                      'vip_port_id': obj.vip_port_id})
        else:
            LOG.error('Invalid obj_type: %s', obj_type)

    def _deleting_completion(self, context, obj_type, obj):
        delete = self._table.get(obj_type+".delete", None)
        model = self._table.get(obj_type+".model", None)
        if delete:
            if not isinstance(obj, model):
                obj = model.from_dict(obj)
            delete(context, obj, delete=True)
            if obj == obj.root_loadbalancer:
                self.driver.plugin.db._core_plugin.delete_port(context, obj.vip_port_id)
        else:
            LOG.error('Invalid obj_type: %s', obj_type)

    def _failed_completion(self, context, obj_type, obj):
        failed = self._table.get(obj_type+".fail", None)
        model = self._table.get(obj_type+".model", None)
        if failed:
            if not isinstance(obj, model):
                obj = model.from_dict(obj)
            failed(context, obj)
        else:
            LOG.error('Invalid obj_type: %s', obj_type)

    def lb_successful_completion(self, context, obj, delete=False, lb_create=False):
        self._successful_completion(context, self.OBJ_TYPE_LB, obj, delete, lb_create)

    def lb_deleting_completion(self, context, obj):
        self._deleting_completion(context, self.OBJ_TYPE_LB, obj)

    def lb_failed_completion(self, context, obj):
        self._failed_completion(context, self.OBJ_TYPE_LB, obj)

    def listener_successful_completion(self, context, obj):
        self._successful_completion(context, self.OBJ_TYPE_LISTENER, obj) 

    def listener_deleting_completion(self, context, obj):
        self._deleting_completion(context, self.OBJ_TYPE_LISTENER, obj)

    def listener_failed_completion(self, context, obj):
        self._failed_completion(context, self.OBJ_TYPE_LISTENER, obj)

    def pool_successful_completion(self, context, obj):
        self._successful_completion(context, self.OBJ_TYPE_POOL, obj) 

    def pool_deleting_completion(self, context, obj):
        self._deleting_completion(context, self.OBJ_TYPE_POOL, obj)

    def pool_failed_completion(self, context, obj):
        self._failed_completion(context, self.OBJ_TYPE_POOL, obj)

    def member_successful_completion(self, context, obj):
        self._successful_completion(context, self.OBJ_TYPE_MEMBER, obj) 

    def member_deleting_completion(self, context, obj):
        self._deleting_completion(context, self.OBJ_TYPE_MEMBER, obj)

    def member_failed_completion(self, context, obj):
        self._failed_completion(context, self.OBJ_TYPE_MEMBER, obj)

    def hm_successful_completion(self, context, obj):
        self._successful_completion(context, self.OBJ_TYPE_HM, obj) 

    def hm_deleting_completion(self, context, obj):
        self._deleting_completion(context, self.OBJ_TYPE_HM, obj)

    def hm_failed_completion(self, context, obj):
        self._failed_completion(context, self.OBJ_TYPE_HM, obj)

    def create_port_on_subnet(self, context, subnet_id, name,
            fixed_address_count=1):
        subnet = self.driver.plugin.db._core_plugin.get_subnet(context, subnet_id)
        fixed_ip = {'subnet_id': subnet['id']}
        if fixed_address_count > 1:
            fixed_ips = []
            for i in range(0, fixed_address_count):
                fixed_ips.append(fixed_ip)
        else:
            fixed_ips = [fixed_ip]
        port_data = {
            'tenant_id': subnet['tenant_id'],
            'name': name,
            'network_id': subnet['network_id'],
            'mac_address': n_const.ATTR_NOT_SPECIFIED,
            'admin_state_up': True,
            'device_id': '',
            'device_owner': '',
            'fixed_ips': fixed_ips
        }
        return self.driver.plugin.db._core_plugin.create_port(context, {'port': port_data})

    def get_subnet(self, context, subnet_id):
        return self.driver.plugin.db._core_plugin.get_subnet(context, subnet_id)

    def get_port(self, context, port_id):
        return self.driver.plugin.db._core_plugin.get_port(context, port_id)

    def get_loadbalancer(self, context, loadbalancer_id):
        lb = self.driver.plugin.db.get_loadbalancer(context, loadbalancer_id)
        return lb.to_dict(stats=False)

    def get_vlan_id_by_port_cmcc(self, context, port_id):
        vlan_tag = db.get_vlan_id_by_port_cmcc(context, port_id)
        if not vlan_tag:
            vlan_tag = '-1'
        ret = {'vlan_tag': str(vlan_tag)}
        return ret

