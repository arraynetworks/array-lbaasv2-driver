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
vlan_tags = []
mock_vlan_tags = []

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


def generate_tags(context):
    global vlan_tags

    if not vlan_tags:
        for i in range(1, 260):
            vlan_tags.append(i)

    array_db = repository.ArrayLBaaSv2Repository()
    exist_tags = array_db.get_all_tags(context.session)

    LOG.debug("----------%s----------", vlan_tags)
    LOG.debug("----------%s----------", exist_tags)
    diff_tags = [i for i in vlan_tags + exist_tags if i not in vlan_tags or i not in exist_tags]
    LOG.debug("----------%s----------", diff_tags)
    if len(diff_tags) > 0:
        return diff_tags[0]
    return None


def generate_mock_vlan_tags(context, port_id):
    global mock_vlan_tags

    if not mock_vlan_tags:
        for i in range(1, 260):
            mock_vlan_tags.append(i)

    array_db = repository.ArrayVlanTagsRepository()
    exist_tags = array_db.get_all_vlan_tags(context.session)

    LOG.debug("----------mock_vlan_tags(%s)----------", mock_vlan_tags)
    LOG.debug("----------mock_exist_tags(%s)----------", exist_tags)
    diff_tags = [i for i in mock_vlan_tags + exist_tags if i not in mock_vlan_tags or i not in exist_tags]
    LOG.debug("----------mock_diff_tags(%s)----------", diff_tags)
    if len(diff_tags) > 0:
        vlan_tag = diff_tags[0]
        array_db.create(context.session, port_id=port_id, vlan_tag=vlan_tag)
        return vlan_tag
    return None

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

def delete_vlan_by_port(context, port_id):
    try:
        array_db = repository.ArrayLBaaSv2Repository()
        array_db.delete(context.session, port_id=port_id)
    except Exception as e:
        LOG.debug("Failed to delete array_vlan_tags(%s)", e.message)

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