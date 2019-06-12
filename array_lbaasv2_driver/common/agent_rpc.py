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

from oslo_log import helpers as log_helpers
from oslo_log import log as logging
import oslo_messaging as messaging

from neutron.common import rpc
from neutron_lbaas.services.loadbalancer import data_models

from array_lbaasv2_driver.common import constants_v2

LOG = logging.getLogger(__name__)


class DataModelSerializer(object):

    def serialize_entity(self, ctx, entity):
        if isinstance(entity, data_models.BaseDataModel):
            return entity.to_dict(stats=False)
        else:
            return entity


class LBaaSv2AgentRPC(object):

    def __init__(self, driver=None):
        self.driver = driver
        self.topic = constants_v2.TOPIC_LOADBALANCER_AGENT_V2
        self._create_rpc_publisher()

    def _create_rpc_publisher(self):
        target = messaging.Target(topic=self.topic,
                                  version=constants_v2.BASE_RPC_API_VERSION)
        self._client = rpc.get_client(target,
                                      serializer=DataModelSerializer(),
                                      version_cap=None)

    def make_msg(self, method, **kwargs):
        return {'method': method,
                'namespace': constants_v2.RPC_API_NAMESPACE,
                'args': kwargs}

    def call(self, context, msg, **kwargs):
        return self.__call_rpc_method(
            context, msg, rpc_method='call', **kwargs)

    def cast(self, context, msg, **kwargs):
        self.__call_rpc_method(context, msg, rpc_method='cast', **kwargs)

    def fanout_cast(self, context, msg, **kwargs):
        kwargs['fanout'] = True
        self.__call_rpc_method(context, msg, rpc_method='cast', **kwargs)

    def __call_rpc_method(self, context, msg, **kwargs):
        options = dict(
            ((opt, kwargs[opt])
             for opt in ('fanout', 'timeout', 'topic', 'version', 'server')
             if kwargs.get(opt))
        )
        if msg['namespace']:
            options['namespace'] = msg['namespace']

        if options:
            callee = self._client.prepare(**options)
        else:
            callee = self._client

        func = getattr(callee, kwargs['rpc_method'])
        return func(context, msg['method'], **msg['args'])

    @log_helpers.log_method_call
    def create_loadbalancer(self, context, loadbalancer, host):
        topic = '%s.%s' % (self.topic, host)
        return self.cast(
            context,
            self.make_msg(
                'create_loadbalancer',
                obj=loadbalancer
            ),
            topic=topic)

    @log_helpers.log_method_call
    def update_loadbalancer(
            self,
            context,
            old_loadbalancer,
            loadbalancer,
            host
    ):
        topic = '%s.%s' % (self.topic, host)
        return self.cast(
            context,
            self.make_msg(
                'update_loadbalancer',
                old_obj=old_loadbalancer,
                obj=loadbalancer
            ),
            topic=topic)

    @log_helpers.log_method_call
    def delete_loadbalancer(self, context, loadbalancer, host):
        topic = '%s.%s' % (self.topic, host)
        return self.cast(
            context,
            self.make_msg(
                'delete_loadbalancer',
                obj=loadbalancer
            ),
            topic=topic)

    @log_helpers.log_method_call
    def update_loadbalancer_stats(
            self,
            context,
            loadbalancer,
            host
    ):
        topic = '%s.%s' % (self.topic, host)
        return self.cast(
            context,
            self.make_msg(
                'update_loadbalancer_stats',
                obj=loadbalancer
            ),
            topic=topic)

    @log_helpers.log_method_call
    def create_listener(self, context, listener, host):
        topic = '%s.%s' % (self.topic, host)
        return self.cast(
            context,
            self.make_msg(
                'create_listener',
                obj=listener
            ),
            topic=topic)

    @log_helpers.log_method_call
    def update_listener(self, context, old_listener, listener, host):
        topic = '%s.%s' % (self.topic, host)
        return self.cast(
            context,
            self.make_msg(
                'update_listener',
                old_obj=old_listener,
                obj=listener
            ),
            topic=topic)

    @log_helpers.log_method_call
    def delete_listener(self, context, listener, host):
        topic = '%s.%s' % (self.topic, host)
        return self.cast(
            context,
            self.make_msg(
                'delete_listener',
                obj=listener
            ),
            topic=topic)

    @log_helpers.log_method_call
    def create_pool(self, context, pool, host):
        topic = '%s.%s' % (self.topic, host)
        return self.cast(
            context,
            self.make_msg(
                'create_pool',
                obj=pool
            ),
            topic=topic)

    @log_helpers.log_method_call
    def update_pool(self, context, old_pool, pool, host):
        topic = '%s.%s' % (self.topic, host)
        return self.cast(
            context,
            self.make_msg(
                'update_pool',
                old_obj=old_pool,
                obj=pool
            ),
            topic=topic)

    @log_helpers.log_method_call
    def delete_pool(self, context, pool, host):
        topic = '%s.%s' % (self.topic, host)
        return self.cast(
            context,
            self.make_msg(
                'delete_pool',
                obj=pool
            ),
            topic=topic)

    @log_helpers.log_method_call
    def create_member(self, context, member, host):
        topic = '%s.%s' % (self.topic, host)
        return self.cast(
            context,
            self.make_msg(
                'create_member',
                obj=member
            ),
            topic=topic)

    @log_helpers.log_method_call
    def update_member(self, context, old_member, member, host):
        topic = '%s.%s' % (self.topic, host)
        return self.cast(
            context,
            self.make_msg(
                'update_member',
                old_obj=old_member,
                obj=member
            ),
            topic=topic)

    @log_helpers.log_method_call
    def delete_member(self, context, member, host):
        topic = '%s.%s' % (self.topic, host)
        return self.cast(
            context,
            self.make_msg(
                'delete_member',
                obj=member
            ),
            topic=topic)

    @log_helpers.log_method_call
    def create_health_monitor(self, context, health_monitor, host):
        topic = '%s.%s' % (self.topic, host)
        return self.cast(
            context,
            self.make_msg(
                'create_health_monitor',
                obj=health_monitor
            ),
            topic=topic)

    @log_helpers.log_method_call
    def update_health_monitor(
            self,
            context,
            old_health_monitor,
            health_monitor,
            host
    ):
        topic = '%s.%s' % (self.topic, host)
        return self.cast(
            context,
            self.make_msg(
                'update_health_monitor',
                old_obj=old_health_monitor,
                obj=health_monitor
            ),
            topic=topic)

    @log_helpers.log_method_call
    def delete_health_monitor(self, context, health_monitor, host):
        topic = '%s.%s' % (self.topic, host)
        return self.cast(
            context,
            self.make_msg(
                'delete_health_monitor',
                obj=health_monitor
            ),
            topic=topic)

    @log_helpers.log_method_call
    def create_l7policy(self, context, l7policy, host):
        topic = '%s.%s' % (self.topic, host)
        return self.cast(
            context,
            self.make_msg(
                'create_l7policy',
                obj=l7policy
            ),
            topic=topic)

    @log_helpers.log_method_call
    def update_l7policy(self, context, old_l7policy, l7policy, host):
        topic = '%s.%s' % (self.topic, host)
        return self.cast(
            context,
            self.make_msg(
                'update_l7policy',
                old_obj=old_l7policy,
                obj=l7policy
            ),
            topic=topic)

    @log_helpers.log_method_call
    def delete_l7policy(self, context, l7policy, host):
        topic = '%s.%s' % (self.topic, host)
        return self.cast(
            context,
            self.make_msg(
                'delete_l7policy',
                obj=l7policy
            ),
            topic=topic)

    @log_helpers.log_method_call
    def create_l7rule(self, context, l7rule, host):
        topic = '%s.%s' % (self.topic, host)
        return self.cast(
            context,
            self.make_msg(
                'create_l7rule',
                obj=l7rule
            ),
            topic=topic)

    @log_helpers.log_method_call
    def update_l7rule(self, context, old_l7rule, l7rule, host):
        topic = '%s.%s' % (self.topic, host)
        return self.cast(
            context,
            self.make_msg(
                'update_l7rule',
                old_obj=old_l7rule,
                obj=l7rule
            ),
            topic=topic)

    @log_helpers.log_method_call
    def delete_l7rule(self, context, l7rule, host):
        topic = '%s.%s' % (self.topic, host)
        return self.cast(
            context,
            self.make_msg(
                'delete_l7rule',
                obj=l7rule
            ),
            topic=topic)
