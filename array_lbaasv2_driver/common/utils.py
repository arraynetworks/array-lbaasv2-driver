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
from oslo_config import cfg

from array_lbaasv2_driver.db import repository

LOG = logging.getLogger(__name__)

ARRAY_OPTS = [
    cfg.StrOpt(
        'array_interfaces',
        help=('APV Interface')
    )
]

try:
    cfg.CONF.register_opts(ARRAY_OPTS, "arraynetworks")
except Exception as e:
    LOG.debug("Failed to register opt(array_interface), maybe has been registered.")

vapv_pool = []

def generate_vapv(context):
    global vapv_pool

    if not vapv_pool:
        interfaces = cfg.CONF.arraynetworks.array_interfaces.split(',')
        for interface in interfaces:
            for i in range(1, 33):
                va_name = "%s_va%02d" % (interface, i)
                vapv_pool.append(va_name)

    array_db = repository.ArrayLBaaSv2Repository()
    exist_vapvs = array_db.get_all_vapvs(context.session)

    LOG.debug("----------%s----------", vapv_pool)
    LOG.debug("----------%s----------", exist_vapvs)
    #diff_vas = list(set(vapv_pool).difference(set(exist_vapvs)))
    diff_vas = [i for i in vapv_pool + exist_vapvs if i not in vapv_pool or i not in exist_vapvs]
    LOG.debug("----------%s----------", diff_vas)
    if len(diff_vas) > 0:
        return diff_vas[0]
    return None


def generate_ha_group_id(context, lb_id, subnet_id, tenant_id, segment_name):
    ha_group_ids = range(0, 260)

    array_db = repository.ArrayLBaaSv2Repository()
    exist_ids = array_db.get_all_ids(context.session)

    diff_ids = [i for i in ha_group_ids + exist_ids if i not in ha_group_ids or i not in exist_ids]
    LOG.debug("----------%s----------", diff_ids)
    if len(diff_ids) > 0:
        group_id = diff_ids[0]
        array_db.create(context.session,
            project_id=tenant_id,
            in_use_lb=1, lb_id=lb_id,
            subnet_id=subnet_id, hostname=lb_id[:10],
            sec_port_id=None, pri_port_id=segment_name,
            cluster_id=group_id)
        return group_id
    return None


def get_vlan_by_subnet_id(context, subnet_id):
    all_vlan_tags = range(1, 4095)

    vlan_mapping_db = repository.ArrayVlanMappingRepository()
    vlan_information = vlan_mapping_db.get_vlan_information_by_subnet(context.session, subnet_id)

    if len(vlan_information) != 0:
        return vlan_information[0]
    else:
        exist_tags = vlan_mapping_db.get_all_tags(context.session)
        LOG.debug("Exist tags: ----------%s----------", exist_tags)
        diff_tags = [i for i in all_vlan_tags + exist_tags if i not in all_vlan_tags or i not in exist_tags]
        if len(diff_tags) > 0:
            vlan_tag = diff_tags[0]
            vlan_mapping_db.create(context.session,
                subnet_id=subnet_id, vlan_tag=vlan_tag)
            return (vlan_tag, None)
    return (None, None)


def create_vapv(context, vapv_name, lb_id, subnet_id, in_use_lb,
                pri_port_id, sec_port_id, cluster_id):
    array_db = repository.ArrayLBaaSv2Repository()
    return array_db.create(context.session,
        in_use_lb=in_use_lb, lb_id=lb_id,
        subnet_id=subnet_id, hostname=vapv_name,
        sec_port_id=sec_port_id, pri_port_id=pri_port_id,
        cluster_id=cluster_id)


def delete_vapv(context, vapv_name):
    try:
        array_db = repository.ArrayLBaaSv2Repository()
        array_db.delete(context.session, hostname=vapv_name)
    except Exception as e:
        LOG.debug("Failed to delete array_lbaasv2(%s)", e.message)


def init_internal_ip_pool(context):
    array_db = repository.ArrayIPPoolsRepository()
    pool = array_db.exists(context.session, 1)
    if pool:
        LOG.debug("array_ip_pool already exists and will not create it again")
        return
    #ipv4
    nums = range(0, 255)
    for num in nums:
        internal_ip = "3.1." + str(num) + ".0"
        array_db.create(context.session, inter_ip=internal_ip, used=False, ipv4=True, use_for_nat=False)
        internal_ip = "3.3." + "0." + str(num)
        array_db.create(context.session, inter_ip=internal_ip, used=False, ipv4=True, use_for_nat=True)
        internal_ip = "3.2." + str(num) + ".0"
        array_db.create(context.session, inter_ip=internal_ip, used=False, ipv4=True, use_for_nat=False)
        internal_ip = "3.4." + "0." + str(num)
        array_db.create(context.session, inter_ip=internal_ip, used=False, ipv4=True, use_for_nat=True)
    #IPV6
    nums = range(0, 512)
    for num in nums:
        internal_ip = "1234:0:" + str(num) + "::0"
        array_db.create(context.session, inter_ip=internal_ip, used=False, ipv4=False, use_for_nat=False)
        internal_ip = "1235::" + str(num)
        array_db.create(context.session, inter_ip=internal_ip, used=False, ipv4=False, use_for_nat=True)
