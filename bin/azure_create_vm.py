#!/usr/bin/env python3

from azure.identity import AzureCliCredential
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.network import NetworkManagementClient
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.compute import ComputeManagementClient
from azure.common.credentials import ServicePrincipalCredentials
import os

# Acquire a credential object using CLI-based authentication.
credential = AzureCliCredential()

# Retrieve subscription ID from environment variable.
Subscription_Id = os.environ["AZURE_SUBSCRIPTION_ID"]




# get subnets:
for p in network_client.subnets.list('FOR-NEURO-SYSMED-UTV-NETWORK','FOR-NEURO-SYSMED-UTV-VNET'):
    print( p.as_dict())

    