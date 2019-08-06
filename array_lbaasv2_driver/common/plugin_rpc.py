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
from oslo_log import helpers as log_helpers
from oslo_log import log as logging

from neutron_lib import constants as n_const
from neutron_lbaas.services.loadbalancer import data_models
from neutron_lib.api.definitions import portbindings
from neutron_lib import constants as n_constants
from neutron_lbaas.db.loadbalancer import models

from array_lbaasv2_driver.common import db
from array_lbaasv2_driver.common import utils
from array_lbaasv2_driver.db import repository
from oslo_config import cfg
from oslo_config.cfg import ConfigParser
import os

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

    def __init__(self, driver, environment):
        LOG.debug('Apv status callbacks RPC subscriber initialized')
        self.driver = driver
        self.environment = environment
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

    @log_helpers.log_method_call
    def lb_successful_completion(self, context, obj, delete=False, lb_create=False):
        self._successful_completion(context, self.OBJ_TYPE_LB, obj, delete, lb_create)

    @log_helpers.log_method_call
    def lb_deleting_completion(self, context, obj):
        self._deleting_completion(context, self.OBJ_TYPE_LB, obj)

    @log_helpers.log_method_call
    def lb_failed_completion(self, context, obj):
        self._failed_completion(context, self.OBJ_TYPE_LB, obj)

    @log_helpers.log_method_call
    def listener_successful_completion(self, context, obj):
        self._successful_completion(context, self.OBJ_TYPE_LISTENER, obj) 

    @log_helpers.log_method_call
    def listener_deleting_completion(self, context, obj):
        self._deleting_completion(context, self.OBJ_TYPE_LISTENER, obj)

    @log_helpers.log_method_call
    def listener_failed_completion(self, context, obj):
        self._failed_completion(context, self.OBJ_TYPE_LISTENER, obj)

    @log_helpers.log_method_call
    def pool_successful_completion(self, context, obj):
        self._successful_completion(context, self.OBJ_TYPE_POOL, obj)

    @log_helpers.log_method_call
    def pool_deleting_completion(self, context, obj):
        self._deleting_completion(context, self.OBJ_TYPE_POOL, obj)

    @log_helpers.log_method_call
    def pool_failed_completion(self, context, obj):
        self._failed_completion(context, self.OBJ_TYPE_POOL, obj)

    @log_helpers.log_method_call
    def member_successful_completion(self, context, obj):
        self._successful_completion(context, self.OBJ_TYPE_MEMBER, obj)

    @log_helpers.log_method_call
    def member_deleting_completion(self, context, obj):
        self._deleting_completion(context, self.OBJ_TYPE_MEMBER, obj)

    @log_helpers.log_method_call
    def member_failed_completion(self, context, obj):
        self._failed_completion(context, self.OBJ_TYPE_MEMBER, obj)

    @log_helpers.log_method_call
    def hm_successful_completion(self, context, obj):
        self._successful_completion(context, self.OBJ_TYPE_HM, obj)

    @log_helpers.log_method_call
    def hm_deleting_completion(self, context, obj):
        self._deleting_completion(context, self.OBJ_TYPE_HM, obj)

    @log_helpers.log_method_call
    def hm_failed_completion(self, context, obj):
        self._failed_completion(context, self.OBJ_TYPE_HM, obj)

    @log_helpers.log_method_call
    def l7rule_successful_completion(self, context, obj):
        self._successful_completion(context, self.OBJ_TYPE_L7RULE, obj)

    @log_helpers.log_method_call
    def l7rule_deleting_completion(self, context, obj):
        self._deleting_completion(context, self.OBJ_TYPE_L7RULE, obj)

    @log_helpers.log_method_call
    def l7rule_failed_completion(self, context, obj):
        self._failed_completion(context, self.OBJ_TYPE_L7RULE, obj)

    @log_helpers.log_method_call
    def l7policy_successful_completion(self, context, obj):
        self._successful_completion(context, self.OBJ_TYPE_L7POLICY, obj)

    @log_helpers.log_method_call
    def l7policy_deleting_completion(self, context, obj):
        self._deleting_completion(context, self.OBJ_TYPE_L7POLICY, obj)

    @log_helpers.log_method_call
    def l7policy_failed_completion(self, context, obj):
        self._failed_completion(context, self.OBJ_TYPE_L7POLICY, obj)

    @log_helpers.log_method_call
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

    @log_helpers.log_method_call
    def get_subnet(self, context, subnet_id):
        return self.driver.plugin.db._core_plugin.get_subnet(context, subnet_id)

    @log_helpers.log_method_call
    def get_network(self, context, network_id):
        return self.driver.plugin.db._core_plugin.get_network(context, network_id)

    @log_helpers.log_method_call
    def get_port(self, context, port_id):
        return self.driver.plugin.db._core_plugin.get_port(context, port_id)

    @log_helpers.log_method_call
    def get_loadbalancer(self, context, loadbalancer_id):
        lb = self.driver.plugin.db.get_loadbalancer(context, loadbalancer_id)
        return lb.to_dict(stats=False)

    @log_helpers.log_method_call
    def get_vlan_id_by_port_cmcc(self, context, port_id):
        vlan_tag = db.get_vlan_id_by_port_cmcc(context, port_id)
        if not vlan_tag:
            vlan_tag = '-1'
        ret = {'vlan_tag': str(vlan_tag)}
        return ret

    @log_helpers.log_method_call
    def get_vlan_id_by_port_huawei(self, context, port_id):
        agent_hosts = []
        # FIXME: when invoking get_lbaas_agents, it SHOULD specify active=True
        # After then, we should record the vlan tag into array_lbaasv2 table when
        # create_vapv, and read the value in delete_loadbalancer
        candidates = self.driver.plugin.db.get_lbaas_agents(context)
        for candidate in candidates:
            agent_hosts.append(candidate['host'])

        vlan_tag = db.get_segment_id_by_port_huawei(context, port_id, agent_hosts)
        if not vlan_tag:
            vlan_tag = '-1'
        ret = {'vlan_tag': str(vlan_tag)}
        return ret

    @log_helpers.log_method_call
    def get_excepted_vapvs(self, context):
        array_db = repository.ArrayLBaaSv2Repository()
        vapv = array_db.get_excepted_vapvs(context.session)
        if not vapv:
            return None
        return vapv

    @log_helpers.log_method_call
    def update_excepted_vapv_by_name(self, context, va_name):
        array_db = repository.ArrayLBaaSv2Repository()
        array_db.update_excepted_vapv_by_name(context.session,
            va_name)

    @log_helpers.log_method_call
    def get_vapv_by_lb_id(self, context, vip_id):
        array_db = repository.ArrayLBaaSv2Repository()
        vapv = array_db.get(context.session, lb_id=vip_id)
        if not vapv:
            return None
        return vapv

    @log_helpers.log_method_call
    def get_va_name_by_lb_id(self, context, vip_id):
        array_db = repository.ArrayLBaaSv2Repository()
        vapv_name = array_db.get_va_name_by_lb_id(context.session, vip_id)
        if not vapv_name:
            return None
        ret = {'vapv_name': str(vapv_name)}
        return ret

    @log_helpers.log_method_call
    def generate_vapv(self, context):
        ret = None
        vapv_name = utils.generate_vapv(context)
        if vapv_name:
            ret = {'vapv_name': str(vapv_name)}
        return ret

    @log_helpers.log_method_call
    def get_segment_name_by_lb_id(self, context, vip_id):
        ret = {'segment_name': ""}
        array_db = repository.ArrayLBaaSv2Repository()
        vapv_name = array_db.get_segment_name_by_lb_id(context.session, vip_id)
        if vapv_name:
            ret = {'segment_name': str(vapv_name)}
        return ret

    @log_helpers.log_method_call
    def generate_ha_group_id(self, context, lb_id, subnet_id,
        tenant_id, segment_name):
        with context.session.begin(subtransactions=True):
            group_id = utils.generate_ha_group_id(context, lb_id,
                subnet_id, tenant_id, segment_name)
            if group_id is None:
                return None
            return {'group_id': group_id}

    @log_helpers.log_method_call
    def create_vapv(self, context, vapv_name, lb_id, subnet_id,
        in_use_lb, pri_port_id, sec_port_id, cluster_id):
        vapv = utils.create_vapv(context, vapv_name, lb_id,
            subnet_id, in_use_lb, pri_port_id, sec_port_id,
            cluster_id)
        return vapv

    @log_helpers.log_method_call
    def delete_vapv(self, context, vapv_name):
        utils.delete_vapv(context, vapv_name)

    @log_helpers.log_method_call
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

    @log_helpers.log_method_call
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

    @log_helpers.log_method_call
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

    @log_helpers.log_method_call
    def scrub_dead_agents(self, context):
        LOG.debug('scrubing dead agent bindings(%s)' % self.driver.array.environment)
        with context.session.begin(subtransactions=True):
            try:
                self.driver.array.scheduler.scrub_dead_agents(
                    context, self.driver.plugin, self.driver.array.environment)
            except Exception as exc:
                LOG.error('scrub dead agents exception: %s' % str(exc))
                return False
        return True

    @log_helpers.log_method_call
    def get_loadbalancer_ids(self, context):
        with context.session.begin(subtransactions=True):
            lbs = self.driver.plugin.db.get_loadbalancers(context)
            return [(lb.id, lb.vip_subnet_id, lb.vip_address, lb.vip_port_id) for lb in lbs]

    @log_helpers.log_method_call
    def check_subnet_used(self, context, subnet_id, lb_id_filter=None,
        member_id_filter=None):
        count = 0
        with context.session.begin(subtransactions=True):
            lbs = self.driver.plugin.db.get_loadbalancers(context)
            for lb in lbs:
                if lb_id_filter and lb_id_filter == lb.id:
                    continue
                if lb.vip_subnet_id == subnet_id:
                    count = 1
                    break

            if count == 1:
                ret = {'count': count}
                return ret

            members = self.driver.plugin.db.get_pool_members(context)
            for member in members:
                if member_id_filter and lb_id_filter == member.id:
                    continue
                if member.subnet_id == subnet_id:
                    count = 1
                    break
            ret = {'count': count}
            return ret


    @log_helpers.log_method_call
    def get_segment_used(self, context, segment_name, lb_id_filter=None):
        ret = {'count': 0}
        with context.session.begin(subtransactions=True):
            array_db = repository.ArrayLBaaSv2Repository()
            lb_ids = array_db.get_lb_ids_by_segment_name(context.session, segment_name)
            LOG.debug("get_segment_used: current lb ids: %s", lb_ids)
            if lb_id_filter:
                lb_ids.remove(lb_id_filter)
            ret = {'count': len(lb_ids)}
        return ret

    @log_helpers.log_method_call
    def get_port_by_name(self, context, port_name=None):
        """Get port by name."""
        if port_name:
            with context.session.begin(subtransactions=True):
                filters = {'name': [port_name]}
                ret_ports = self.driver.plugin.db._core_plugin.get_ports(
                    context,
                    filters=filters
                )
                LOG.debug("get_port_by_name: --%s-- returned" % ret_ports)
                return ret_ports


    @log_helpers.log_method_call
    def get_vlan_tag_by_port_name(self, context, port_name):
        """Get port by name."""
        vlan_tag = '-1'
        if port_name:
            with context.session.begin(subtransactions=True):
                filters = {'name': [port_name]}
                ports = self.driver.plugin.db._core_plugin.get_ports(
                    context, filters=filters)
                if ports:
                    return self.get_vlan_id_by_port_huawei(context, ports[0]['id'])
                else:
                    LOG.debug("Failed to get the port by port_name(%s)" % port_name)
        else:
            LOG.debug("The port_name cann't be NULL.")
        ret = {'vlan_tag': vlan_tag}
        return ret


    @log_helpers.log_method_call
    def get_active_agents(self, context):
        plugin = self.driver.plugin
        active_agents = []
        with context.session.begin(subtransactions=True):
            active_agents = self.driver.array.scheduler.get_array_agent_candidates(
                context,
                plugin,
                self.driver.array.environment
            )
        return active_agents

    @log_helpers.log_method_call
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

    @log_helpers.log_method_call
    def get_cluster_id_by_lb_id(self, context, lb_id):
        array_db = repository.ArrayLBaaSv2Repository()
        cluster_id = array_db.get_clusterids_by_id(context.session, lb_id)
        if not cluster_id:
            return None
        return cluster_id

    @log_helpers.log_method_call
    def get_available_internal_ip(self, context, seg_name, seg_ip, use_for_nat=False):
        array_db = repository.ArrayIPPoolsRepository()
        if seg_name and seg_ip:
            ip_pool = array_db.get_one_available_entry(context.session, seg_name, seg_ip, use_for_nat)
            if ip_pool:
                array_db.update(context.session, ip_pool.id,
                    seg_name = seg_name, seg_ip = seg_ip,
                    inter_ip = ip_pool.inter_ip, used = True, use_for_nat=use_for_nat)
                return ip_pool.inter_ip
            else:
                return None
        else:
            return None

    @log_helpers.log_method_call
    def get_internal_ip_by_lb(self, context, seg_name, seg_ip, use_for_nat=False):
        array_db = repository.ArrayIPPoolsRepository()
        if seg_name and seg_ip:
            return array_db.get_used_internal_ip(context.session, seg_name, seg_ip, use_for_nat)
        else:
            return None

    @log_helpers.log_method_call
    def createCounter(self, x):
        f = [0]
        def increase():
            f[0] = f[0] + 1
            if f[0] == x + 1:
               f[0] = 1
            return f[0]
        return increase

    @log_helpers.log_method_call
    def get_array_interfaces(self):
        interfaces = None
        if self.environment:
            array_conf = "/etc/neutron/conf.d/neutron-server/arraynetworks"
            array_conf = array_conf + "-" + self.environment + ".conf"
            if os.access(array_conf, os.F_OK):
                try:
                    conf = ConfigParser(array_conf, {})
                    conf.parse()
                    interfaces = conf.sections['arraynetworks']['array_interfaces'][0]
                except Exception as exc:
                    LOG.error("get arraynetworks configuration exception: %s" % str(exc))
                    raise e
            else:
                LOG.error("Failed to access the arraynetworks configuration(%s)", array_conf)
        else:
            interfaces = cfg.CONF.arraynetworks.array_interfaces
        if interfaces:
            LOG.debug("get the interfaces(%s) from configuration", interfaces)
            interfaces_map = interfaces.split(",")
            bonds_dic = {}
            for interface_map in interfaces_map:
                bond_map = interface_map.strip()
                index = bond_map.index(":")
                bond = bond_map[:index]
                if bonds_dic.has_key(bond):
                    bonds_dic[bond].append(bond_map[index + 1:])
                else:
                   ports = []
                   ports.append(bond_map[index + 1:])
                   bonds_dic[bond] = ports
            return bonds_dic
        else:
            return {}

    @log_helpers.log_method_call
    def get_interfaces(self):
        interfaces_dic = self.get_array_interfaces()
        if interfaces_dic:
            return interfaces_dic.keys()
        else:
            return []

    @log_helpers.log_method_call
    def get_interface_port(self, context, bond):
        interfaces_dic = self.get_array_interfaces()
        if interfaces_dic and interfaces_dic.has_key(bond):
            return interfaces_dic[bond]
        else:
            LOG.error("Failed to get interface port from interfaces_dic(%s)", interfaces_dic)
            return []

    @log_helpers.log_method_call
    def get_all_interfaces(self, context):
        return self.interfaces

    @log_helpers.log_method_call
    def get_interface(self, context):
        if len(self.interfaces) > 0:
            interface = self.interfaces[self.next_interface() - 1]
            LOG.debug("get the interface(%s) from get_interface", interface)
            return interface
        else:
            LOG.error("Failed to get interface from interfaces(%s)", self.interfaces)
            return None
