#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

from neutron.db import models_v2
from sqlalchemy.ext.declarative import declarative_base
import sqlalchemy as sa
try:
    from neutron_lib.db import model_base
except ImportError:
    pass

HAS_ID = None
if hasattr(models_v2, 'HasId'):
    HAS_ID = models_v2.HasId
else:
    HAS_ID = model_base.HasId

HAS_TENANT = None
if hasattr(models_v2, 'HasTenant'):
    HAS_TENANT = models_v2.HasTenant
elif hasattr(model_base, 'HasTenant'):
    HAS_TENANT = model_base.HasTenant
elif hasattr(model_base, 'HasProject'):
    HAS_TENANT = model_base.HasProject

BaseTable = declarative_base()

class ArrayLBaaSv2(BaseTable, HAS_ID, HAS_TENANT):
    """Represents an Array load balancer."""

    __tablename__ = "array_lbaasv2"

    in_use_lb = sa.Column(sa.Integer(), nullable=False)
    subnet_id = sa.Column(sa.String(36), nullable=False)
    lb_id = sa.Column(sa.String(36), nullable=False)
    hostname = sa.Column(sa.String(12), nullable=False)
    pri_port_id = sa.Column(sa.String(64), nullable=True)
    sec_port_id = sa.Column(sa.String(64), nullable=True)
    cluster_id = sa.Column(sa.Integer(), nullable=True)

    def to_dict(self, **kwargs):
        ret = {}
        for attr in self.__dict__:
            if attr.startswith('_') or not kwargs.get(attr, True):
                continue
            if isinstance(getattr(self, attr), list):
                ret[attr] = []
                for item in self.__dict__[attr]:
                    ret[attr] = item
            elif isinstance(self.__dict__[attr], unicode):
                ret[attr.encode('utf8')] = self.__dict__[attr].encode('utf8')
            else:
                ret[attr] = self.__dict__[attr]
        return ret

    def from_dict(cls, model_dict):
        return cls(**model_dict)


class ArrayAPVIPPOOL(BaseTable, HAS_ID, HAS_TENANT):
    """Represents an Array load balancer."""

    __tablename__ = "array_ip_pool"

    seg_name = sa.Column(sa.String(64), nullable=True)
    seg_ip = sa.Column(sa.String(36), nullable=True)
    inter_ip = sa.Column(sa.String(36), nullable=False)
    used = sa.Column(sa.Boolean, default=False, nullable=False)
    ipv4 = sa.Column(sa.Boolean, default=True, nullable=False)

    def to_dict(self, **kwargs):
        ret = {}
        for attr in self.__dict__:
            if attr.startswith('_') or not kwargs.get(attr, True):
                continue
            if isinstance(getattr(self, attr), list):
                ret[attr] = []
                for item in self.__dict__[attr]:
                    ret[attr] = item
            elif isinstance(self.__dict__[attr], unicode):
                ret[attr.encode('utf8')] = self.__dict__[attr].encode('utf8')
            else:
                ret[attr] = self.__dict__[attr]
        return ret

    def from_dict(cls, model_dict):
        return cls(**model_dict)

class ArrayVLANTAGS(BaseTable, HAS_ID, HAS_TENANT):
    """Represents an Array load balancer."""

    __tablename__ = "array_vlan_tags"

    port_id = sa.Column(sa.String(40), nullable=True)
    vlan_tag = sa.Column(sa.Integer(), nullable=False)

    def to_dict(self, **kwargs):
        ret = {}
        for attr in self.__dict__:
            if attr.startswith('_') or not kwargs.get(attr, True):
                continue
            if isinstance(getattr(self, attr), list):
                ret[attr] = []
                for item in self.__dict__[attr]:
                    ret[attr] = item
            elif isinstance(self.__dict__[attr], unicode):
                ret[attr.encode('utf8')] = self.__dict__[attr].encode('utf8')
            else:
                ret[attr] = self.__dict__[attr]
        return ret

    def from_dict(cls, model_dict):
        return cls(**model_dict)
