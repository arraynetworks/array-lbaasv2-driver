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
from neutron_lib.api.definitions import portbindings
from neutron_lib import constants as n_constants
from neutron_lbaas.db.loadbalancer import models

from array_lbaasv2_driver.common import db
from array_lbaasv2_driver.common import utils
from array_lbaasv2_driver.db import repository
from oslo_config import cfg

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
    OBJ_TYPE_L7RULE = "l7rule"
    OBJ_TYPE_L7POLICY = "l7policy"

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

            "l7rule.success": self.driver.l7rule.successful_completion,
            "l7rule.delete": self.driver.l7rule.successful_completion,
            "l7rule.fail": self.driver.l7rule.failed_completion,

            "l7policy.success": self.driver.l7policy.successful_completion,
            "l7policy.delete": self.driver.l7policy.successful_completion,
            "l7policy.fail": self.driver.l7policy.failed_completion,

            "loadbalancer.model": data_models.LoadBalancer,
            "listener.model": data_models.Listener,
            "pool.model": data_models.Pool,
            "member.model": data_models.Member,
            "hm.model": data_models.HealthMonitor,
            "l7policy.model": data_models.L7Policy,
            "l7rule.model": data_models.L7Rule,
        }
        self.interfaces = self.get_interfaces()
        self.next_interface = self.createCounter(len(self.interfaces))
        # self.next_vlan_tag = self.createCounter(256)  #need to be replaced by huawei vlan tag

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

    def l7rule_successful_completion(self, context, obj):
        self._successful_completion(context, self.OBJ_TYPE_L7RULE, obj)

    def l7rule_deleting_completion(self, context, obj):
        self._deleting_completion(context, self.OBJ_TYPE_L7RULE, obj)

    def l7rule_failed_completion(self, context, obj):
        self._failed_completion(context, self.OBJ_TYPE_L7RULE, obj)

    def l7policy_successful_completion(self, context, obj):
        self._successful_completion(context, self.OBJ_TYPE_L7POLICY, obj)

    def l7policy_deleting_completion(self, context, obj):
        self._deleting_completion(context, self.OBJ_TYPE_L7POLICY, obj)

    def l7policy_failed_completion(self, context, obj):
        self._failed_completion(context, self.OBJ_TYPE_L7POLICY, obj)

    def create_port_on_subnet(self, context, subnet_id, name, host,
        device_id, fixed_address_count=1):
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
            'device_id': device_id,
            'device_owner': n_const.DEVICE_OWNER_LOADBALANCERV2,
            'fixed_ips': fixed_ips
        }
        port_data[portbindings.HOST_ID] = host
        port_data[portbindings.VNIC_TYPE] = "normal"
        port_data[portbindings.PROFILE] = {}
        return self.driver.plugin.db._core_plugin.create_port(context, {'port': port_data})

    def get_subnet(self, context, subnet_id):
        return self.driver.plugin.db._core_plugin.get_subnet(context, subnet_id)

    def get_network(self, context, network_id):
        return self.driver.plugin.db._core_plugin.get_network(context, network_id)

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

    def get_vlan_id_by_port_huawei(self, context, port_id):
        agent_hosts = []
        candidates = self.driver.plugin.db.get_lbaas_agents(context, active=True)
        for candidate in candidates:
            agent_hosts.append(candidate['host'])

        # vlan_tag = db.get_segment_id_by_port_huawei(context, port_id, agent_hosts)
        vlan_tag = 4  #need to be replace by the previous line of code
        if not vlan_tag:
            vlan_tag = '-1'
        ret = {'vlan_tag': str(vlan_tag)}
        return ret

    def get_vapv_by_lb_id(self, context, vip_id):
        array_db = repository.ArrayLBaaSv2Repository()
        vapv = array_db.get(context.session, lb_id=vip_id)
        if not vapv:
            return None
        return vapv

    def get_va_name_by_lb_id(self, context, vip_id):
        array_db = repository.ArrayLBaaSv2Repository()
        vapv_name = array_db.get_va_name_by_lb_id(context.session, vip_id)
        if not vapv_name:
            return None
        ret = {'vapv_name': str(vapv_name)}
        return ret

    def generate_vapv(self, context):
        ret = None
        vapv_name = utils.generate_vapv(context)
        if vapv_name:
            ret = {'vapv_name': str(vapv_name)}
        return ret

    def generate_tags(self, context):
        ret = None
        vlan_tag = utils.generate_tags(context)
        if vlan_tag:
            ret = {'vlan_tag': vlan_tag}
        return ret

    def create_vapv(self, context, vapv_name, lb_id, subnet_id,
        in_use_lb, pri_port_id, sec_port_id, cluster_id):
        vapv = utils.create_vapv(context, vapv_name, lb_id,
            subnet_id, in_use_lb, pri_port_id, sec_port_id,
            cluster_id)
        return vapv

    def delete_vapv(self, context, vapv_name):
        utils.delete_vapv(context, vapv_name)

    def delete_port(self, context, port_id=None, mac_address=None):
        """Delete port."""
        if port_id:
            self.driver.plugin.db._core_plugin.delete_port(context, port_id)
        elif mac_address:
            filters = {'mac_address': [mac_address]}
            ports = self.driver.plugin.db._core_plugin.get_ports(
                context,
                filters=filters
            )
            for port in ports:
                self.driver.plugin.db._core_plugin.delete_port(
                    context,
                    port['id']
                )

    def delete_port_by_name(self, context, port_name=None):
        """Delete port by name."""
        if port_name:
            filters = {'name': [port_name]}
            try:
                ports = self.driver.plugin.db._core_plugin.get_ports(
                    context,
                    filters=filters
                )
                for port in ports:
                    self.driver.plugin.db._core_plugin.delete_port(
                        context,
                        port['id']
                    )
            except Exception as e:
                LOG.error("failed to delete port: %s", e.message)

    def update_member_status(self, context, member_id=None,
        provisioning_status=None, operating_status=None):
        LOG.debug("-----enter update_member_status-----%s: %s" % (member_id, operating_status))
        with context.session.begin(subtransactions=True):
            try:
                member = self.driver.plugin.db.get_pool_member(
                    context,
                    member_id
                )
                if (member.provisioning_status !=
                        n_constants.PENDING_DELETE):
                    LOG.debug("----------will update_status--------------")
                    self.driver.plugin.db.update_status(
                        context,
                        models.MemberV2,
                        member_id,
                        provisioning_status,
                        operating_status
                    )
            except Exception as e:
                LOG.error('Exception: update_member_status: %s',
                          e.message)

    def get_members_status_on_agent(self, context, agent_host_name):
        lb_members = {}
        plugin = self.driver.plugin
        with context.session.begin(subtransactions=True):
            active_agents = self.driver.array.scheduler.get_array_agent_candidates(
                context,
                plugin,
                self.driver.array.environment
            )
            for agent in active_agents:
                if agent['host'] == agent_host_name:
                    agent_lbs = plugin.db.list_loadbalancers_on_lbaas_agent(
                        context,
                        agent['id']
                    )
                    for lb in agent_lbs:
                        lb_dict = {}
                        if lb.pools:
                            for pool in lb.pools:
                                if pool.members and pool.healthmonitor:
                                    for mem in pool.members:
                                        lb_dict[mem.id] = mem.operating_status
                        if lb_dict:
                            lb_members[lb.id] = lb_dict
            return lb_members

    def get_cluster_id_by_subnet_id(self, context, subnet_id):
        array_db = repository.ArrayLBaaSv2Repository()
        cluster_id = array_db.get_clusterids_by_subnet(context.session, subnet_id)
        if not cluster_id:
            return None
        return cluster_id

    def get_available_internal_ip(self, context, seg_name, seg_ip):
        array_db = repository.ArrayIPPoolsRepository()
        if seg_name and seg_ip:
            ip_pool = array_db.get_one_available_entry(context.session, seg_name, seg_ip)
            if ip_pool:
                array_db.update(context.session, ip_pool.id,
                    seg_name = seg_name, seg_ip = seg_ip,
                    inter_ip = ip_pool.inter_ip, used = True)
                return ip_pool.inter_ip
            else:
                return None
        else:
            return None

    def get_internal_ip_by_lb(self, context, seg_name, seg_ip):
        array_db = repository.ArrayIPPoolsRepository()
        if seg_name and seg_ip:
            internal_ip = array_db.get_used_internal_ip(context.session, seg_name, seg_ip)
            if internal_ip:
                return internal_ip
            else:
                return None
        else:
            return None

    def createCounter(self, x):
        f = [0]
        def increase():
            f[0] = f[0] + 1
            if f[0] == x + 1:
               f[0] = 1
            return f[0]
        return increase

    def get_interfaces(self):
        interfaces = cfg.CONF.arraynetworks.array_interfaces
        if interfaces:
            LOG.debug("get the interfaces(%s) from configuration", interfaces)
            return interfaces.split(",")
        else:
            return []

    def get_interface(self, context):
        if len(self.interfaces) > 0:
            return self.interfaces[self.next_interface() - 1]
        else:
            LOG.error("Failed to get interface from interfaces(%s)", self.interfaces)
            return None
