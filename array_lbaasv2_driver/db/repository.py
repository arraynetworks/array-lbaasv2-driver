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

import models
import IPy

class BaseRepository(object):
    model_class = None

    def create(self, session, **model_kwargs):
        """Base create method for a database entity.

        :param session: A Sql Alchemy database session.
        :param model_kwargs: Attributes of the model to insert.
        :returns: octavia.common.data_model
        """
        with session.begin(subtransactions=True):
            model = self.model_class(**model_kwargs)
            session.add(model)
        return model.to_dict()

    def delete(self, session, **filters):
        """Deletes an entity from the database.

        :param session: A Sql Alchemy database session.
        :param filters: Filters to decide which entity should be deleted.
        :returns: None
        :raises: sqlalchemy.orm.exc.NoResultFound
        """
        model = session.query(self.model_class).filter_by(**filters).one()
        with session.begin(subtransactions=True):
            session.delete(model)
            session.flush()

    def delete_batch(self, session, ids=None):
        """Batch deletes by entity ids."""
        ids = ids or []
        for id in ids:
            self.delete(session, id=id)

    def update(self, session, id, **model_kwargs):
        """Updates an entity in the database.

        :param session: A Sql Alchemy database session.
        :param model_kwargs: Entity attributes that should be updates.
        :returns: octavia.common.data_model
        """
        with session.begin(subtransactions=True):
            session.query(self.model_class).filter_by(
                id=id).update(model_kwargs)

    def get(self, session, **filters):
        """Retrieves an entity from the database.

        :param session: A Sql Alchemy database session.
        :param filters: Filters to decide which entity should be retrieved.
        :returns: octavia.common.data_model
        """
        model = session.query(self.model_class).filter_by(**filters).first()
        if not model:
            return
        return model.to_dict()

    def exists(self, session, id):
        """Determines whether an entity exists in the database by its id.

        :param session: A Sql Alchemy database session.
        :param id: id of entity to check for existence.
        :returns: octavia.common.data_model
        """
        return bool(session.query(self.model_class).filter_by(id=id).first())


class ArrayLBaaSv2Repository(BaseRepository):
    model_class = models.ArrayLBaaSv2

    def get_va_name_by_lb_id(self, session, lb_id):
        vapv = session.query(self.model_class).filter_by(lb_id=lb_id).first()
        if vapv:
            return vapv.hostname
        return None

    def get_excepted_vapvs(self, session):
        vapvs = session.query(self.model_class).all()
        res_vapvs = []
        for vapv in vapvs:
            if vapv.in_use_lb == 10 or vapv.in_use_lb == 11:
                res_vapvs.append(vapv.to_dict())
        return res_vapvs

    def update_excepted_vapv_by_name(self, session, va_name):
        updated = {}
        updated['in_use_lb'] = 1
        with session.begin(subtransactions=True):
            session.query(self.model_class).filter_by(
                hostname=va_name).update(updated)

    def get_va_by_tenant_id(self, session, tenant_id):
        vapv = session.query(self.model_class).filter_by(tenant_id=tenant_id).first()
        if vapv:
            return vapv.hostname
        return None

    def get_all_vapvs(self, session):
        vapvs = session.query(self.model_class).all()
        return [vapv.hostname for vapv in vapvs]

    def get_all_tags(self, session):
        vapvs = session.query(self.model_class).all()
        return [vapv.cluster_id for vapv in vapvs]

    def get_clusterids_by_id(self, session, lb_id):
        vapvs = session.query(self.model_class).filter_by(lb_id=lb_id)
        return [vapv.cluster_id for vapv in vapvs]

class ArrayIPPoolsRepository(BaseRepository):
    model_class = models.ArrayAPVIPPOOL

    def get_one_available_entry(self, session, seg_name, seg_ip, use_for_nat):
        is_ipv4 = IPy.IP(seg_ip).version() == 4
        ip_pool = session.query(self.model_class).filter_by(used=False, ipv4=is_ipv4, use_for_nat=use_for_nat).first()
        if ip_pool:
            return ip_pool
        return None

    def get_used_internal_ip(self, session, seg_name, seg_ip, use_for_nat):
        ip_pool = session.query(self.model_class).filter_by(seg_name=seg_name, seg_ip=seg_ip, use_for_nat=use_for_nat).first()
        if ip_pool:
            return ip_pool.inter_ip
        return None

class ArrayVlanTagsRepository(BaseRepository):
    model_class = models.ArrayVLANTAGS

    def get_vlan_by_port(self, session, port_id):
        vlan_tags = session.query(self.model_class).filter_by(port_id=port_id).first()
        if vlan_tags:
            return vlan_tags.vlan_tag
        return None

    def get_all_vlan_tags(self, session):
        vlan_tags = session.query(self.model_class).all()
        return [vlan.vlan_tag for vlan in vlan_tags]

