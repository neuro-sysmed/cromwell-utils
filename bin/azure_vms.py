    #!/usr/bin/env python3


import os

import tabulate

from azure.identity import AzureCliCredential
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.network import NetworkManagementClient
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.compute import ComputeManagementClient
from azure.common.credentials import ServicePrincipalCredentials


# Acquire a credential object using CLI-based authentication.
credential = AzureCliCredential()

# Retrieve subscription ID from environment variable.
#Subscription_Id = os.environ["AZURE_SUBSCRIPTION_ID"]
Subscription_Id = '5a9e26a0-6897-44d6-963e-fae2a2061f27'

compute_client = ComputeManagementClient(credential, Subscription_Id)
resource_client = ResourceManagementClient(credential, Subscription_Id)
network_client  = NetworkManagementClient(credential, Subscription_Id)

vm_list = compute_client.virtual_machines.list_all()
# vm_list = compute_client.virtual_machines.list('resource_group_name')

vms = [['name', 'type',  'size', 'location', 'codes']]

vm_general = vm_list.next()

for vm_general in vm_list:
    general_view = vm_general.id.split("/")
    resource_group = general_view[4]
    vm_name = general_view[-1]
    vm = compute_client.virtual_machines.get(resource_group, vm_name, expand='instanceView')

    codes = []
    for stat in vm.instance_view.statuses:
        codes.append( stat.code )
    codes = ", ".join(codes)
    vms.append([vm.name, vm.type, vm.hardware_profile.vm_size, vm.location ])


print(tabulate.tabulate(vms, headers="firstrow", tablefmt='psql'))