#!/usr/bin/bash

environment=$1

existing_provider_path="array_lbaasv2_driver"
new_provider_path="array_lbaasv2_driver_$environment"
existing_db="array_lbaasv2"
new_db="array_lbaasv2_$environment"
existing_ip_pool_db="array_ip_pool"
new_ip_pool_db="array_ip_pool_$environment"
existing_array_vlan_tags_db="array_vlan_tags" #need delete
new_array_vlan_tags_db="array_vlan_tags_$environment" #need delete

pushd /usr/lib/python2.7/site-packages > /dev/null

cp -r $existing_provider_path $new_provider_path

find $new_provider_path -name "*.pyc" -delete
find $new_provider_path -name "*.pyo" -delete

sed -i "s/array_lbaasv2_driver/${new_provider_path}/" $new_provider_path/common/agent_rpc.py
sed -i "s/array_lbaasv2_driver/${new_provider_path}/" $new_provider_path/common/driver_v2.py
sed -i "s/array_lbaasv2_driver/${new_provider_path}/" $new_provider_path/common/utils.py
sed -i "s/array_lbaasv2_driver/${new_provider_path}/" $new_provider_path/common/plugin_rpc.py
sed -i "s/array_lbaasv2_driver/${new_provider_path}/" $new_provider_path/v2/driver_v2.py

sed -i "s/${existing_db}/${new_db}/" $new_provider_path/db/models.py
sed -i "s/${existing_ip_pool_db}/${new_ip_pool_db}/" $new_provider_path/db/models.py
sed -i "s/${existing_array_vlan_tags_db}/${new_array_vlan_tags_db}/" $new_provider_path/db/models.py #need delete

popd
