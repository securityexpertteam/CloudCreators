import argparse
import re
import json
import datetime
import time
from azure.identity import ClientSecretCredential
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.costmanagement import CostManagementClient
from azure.mgmt.storage import StorageManagementClient
from azure.mgmt.monitor import MonitorManagementClient
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.network import NetworkManagementClient
from pymongo import MongoClient
import os
import ipaddress

# --- Argument Parser for Azure Credentials and MongoDB ---
parser = argparse.ArgumentParser(description="Azure Resource Optimization Script")
parser.add_argument("--client_id", required=True, help="Azure Client ID")
parser.add_argument("--client_secret", required=True, help="Azure Client Secret")
parser.add_argument("--tenant_id", required=True, help="Azure Tenant ID")
parser.add_argument("--subscription_id", required=True, help="Azure Subscription ID")
parser.add_argument("--email", required=True, help="User Email for filtering configs")
parser.add_argument("--mongo_uri", default="mongodb://localhost:27017/", help="MongoDB connection URI")
parser.add_argument("--db_name", default="myDB", help="MongoDB database name")
args = parser.parse_args()

client_id = args.client_id
client_secret = args.client_secret
tenant_id = args.tenant_id
subscription_id = args.subscription_id
user_email = args.email
MONGO_URI = args.mongo_uri
DB_NAME = args.db_name

# print("Client ID:", args.client_id)
# print("Client Secret:", args.client_secret)
print("Tenant ID:", args.tenant_id)
print("Subscription ID:", args.subscription_id)
print("Email:", args.email)

# --- MongoDB connection details ---
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
triggers_collection = db["triggers"]
cost_insights_collection = db["Cost_Insights"]
environment_onboarding_collection = db["environmentOnboarding"]
standard_config_collection = db["standardConfigsDb"]

# Define required tag names as a list:
REQUIRED_TAG_NAMES = [
    "ApplicationCode",
    "CIO",
    "CloudProvider",
    "CostCenter",
    "Entity",
    "Environment",
    "Feature",
    "Lab",
    "Owner",
    "Platform",
    "Ticket"
]

# Get stor_size and thresholds from standardConfigsDb collection for the current email
config_thresholds = standard_config_collection.find_one(
    {"email": user_email},
    {
        "cmp_cpu_usage": 1,
        "cmp_memory_usage": 1,
        "cmp_network_usage": 1,
        "stor_size": 1,
        "subnet_free_ip_threshold": 1,
        "disk_quota_gb": 1,
        "_id": 0
    }
)

VM_UNDERUTILIZED_CPU_THRESHOLD = config_thresholds.get("cmp_cpu_usage") if config_thresholds else None
VM_UNDERUTILIZED_MEMORY_THRESHOLD = config_thresholds.get("cmp_memory_usage") if config_thresholds else None
VM_UNDERUTILIZED_NETWORK_THRESHOLD = config_thresholds.get("cmp_network_usage") if config_thresholds else None
sc_stor_size_in_gb = config_thresholds.get("stor_size") if config_thresholds else None
#SUBNET_FREE_IP_THRESHOLD = config_thresholds.get("subnet_free_ip_threshold") if config_thresholds else None
#DISK_QUOTA_GB = int(config_thresholds.get("disk_quota_gb")) if config_thresholds and config_thresholds.get("disk_quota_gb") is not None else None
DISK_QUOTA_GB = 100
SUBNET_FREE_IP_THRESHOLD = 100

try:
    client.admin.command('ismaster')
    print(f"MongoDB connection to {MONGO_URI} established successfully.")
except Exception as e:
    print(f"MongoDB connection failed: {e}")

def normalize_resource_id(resource_id):
    clean_id = re.sub(r'[\u200b\xa0\s]+', '', resource_id)
    return clean_id.strip().rstrip('/').lower()

def get_storage_account_size(storage_client, resource_group_name, storage_account_name):
    """Get the total used capacity of a storage account in GB."""
    try:
        # Note: Azure doesn't provide direct API for storage usage in the management SDK.
        # To get actual storage usage, you would need to:
        # 
        # Option 1: Use Azure Monitor Management Client to query UsedCapacity metric
        # from azure.mgmt.monitor import MonitorManagementClient
        # monitor_client = MonitorManagementClient(credential, subscription_id)
        # Query metric: "UsedCapacity" for the storage account resource
        #
        # Option 2: Use Storage service clients to enumerate and sum blob/file sizes
        # from azure.storage.blob import BlobServiceClient
        # from azure.storage.file import ShareServiceClient
        # Enumerate all containers/shares and sum blob/file sizes
        #
        # Option 3: Use Azure Resource Graph to query storage metrics
        # This requires additional permissions and setup
        
        # For now, returning 0 as placeholder - replace with actual implementation
        # You can modify this function to return actual storage usage in GB
        
        return 0.5  # Placeholder - change this to test filtering logic (0.5 GB < 1 GB threshold)
        
    except Exception as e:
        print(f"Error getting storage account size for {storage_account_name}: {e}")
        return None

def get_vm_average_cpu(monitor_client, resource_id, start_time, end_time):
    """Fetch average CPU utilization for a VM over a period."""
    try:
        metrics_data = monitor_client.metrics.list(
            resource_id,
            timespan=f"{start_time}/{end_time}",
            interval='PT1H',
            metricnames='Percentage CPU',
            aggregation='Average'
        )
        values = []
        for item in metrics_data.value:
            for timeserie in item.timeseries:
                for data in timeserie.data:
                    if data.average is not None:
                        values.append(data.average)
        if values:
            return sum(values) / len(values)
        return None
    except Exception as e:
        print(f"Error fetching CPU metrics for {resource_id}: {e}")
        return None

def get_vm_average_memory(monitor_client, resource_id, start_time, end_time):
    """Fetch average memory utilization for a VM over a period."""
    try:
        metrics_data = monitor_client.metrics.list(
            resource_id,
            timespan=f"{start_time}/{end_time}",
            interval='PT1H',
            metricnames='Available Memory Bytes',
            aggregation='Average'
        )
        values = []
        for item in metrics_data.value:
            for timeserie in item.timeseries:
                for data in timeserie.data:
                    if data.average is not None:
                        values.append(data.average)
        if values:
            # Convert bytes to percent if total memory is known, else just return average bytes
            return sum(values) / len(values)
        return None
    except Exception as e:
        print(f"Error fetching Memory metrics for {resource_id}: {e}")
        return None

def get_vm_average_network(monitor_client, resource_id, start_time, end_time):
    """Fetch average network utilization for a VM over a period."""
    try:
        metrics_data = monitor_client.metrics.list(
            resource_id,
            timespan=f"{start_time}/{end_time}",
            interval='PT1H',
            metricnames='Network In Total',
            aggregation='Average'
        )
        values = []
        for item in metrics_data.value:
            for timeserie in item.timeseries:
                for data in timeserie.data:
                    if data.average is not None:
                        values.append(data.average)
        if values:
            return sum(values) / len(values)
        return None
    except Exception as e:
        print(f"Error fetching Network metrics for {resource_id}: {e}")
        return None

def get_subnet_free_ip_percent(network_client, resource_group, vnet_name, subnet_name):
    """Returns the percent of free IPs in the subnet."""
    try:
        subnet = network_client.subnets.get(resource_group, vnet_name, subnet_name)
        prefix = subnet.address_prefix
        total_ips = ipaddress.ip_network(prefix).num_addresses - 5  # Azure reserves 5 IPs per subnet
        used_ips = subnet.ip_configurations and len(subnet.ip_configurations) or 0
        free_ips = total_ips - used_ips
        free_percent = (free_ips / total_ips) * 100 if total_ips > 0 else 0
        return free_percent
    except Exception as e:
        print(f"Error fetching subnet info for {subnet_name}: {e}")
        return None

def analyze_azure_resources():
    print("[INFO] Starting Azure resource optimization analysis...")

    # Use credentials from argparse
    credential = ClientSecretCredential(tenant_id, client_id, client_secret)
    resource_client = ResourceManagementClient(credential, subscription_id)
    cost_client = CostManagementClient(credential)
    storage_client = StorageManagementClient(credential, subscription_id)
    monitor_client = MonitorManagementClient(credential, subscription_id)
    compute_client = ComputeManagementClient(credential, subscription_id)
    network_client = NetworkManagementClient(credential, subscription_id)

    end_date = datetime.datetime.utcnow()
    start_date = end_date - datetime.timedelta(days=7)

    # Cost query setup
    cost_query = {
        "type": "Usage",
        "timeframe": "Custom",
        "time_period": {
            "from": start_date.strftime("%Y-%m-%dT00:00:00Z"),
            "to": end_date.strftime("%Y-%m-%dT00:00:00Z")
        },
        "dataset": {
            "granularity": "None",
            "aggregation": {
                "totalCost": {
                    "name": "PreTaxCost",
                    "function": "Sum"
                }
            },
            "grouping": [
                {
                    "type": "Dimension",
                    "name": "ResourceId"
                }
            ]
        }
    }

    # Get cost data
    cost_data = cost_client.query.usage(
        scope=f"/subscriptions/{subscription_id}",
        parameters=cost_query
    )

    # Build resource ID -> cost map
    resource_cost_map = {}
    if cost_data and cost_data.rows:
        columns = [col.name for col in cost_data.columns]
        rid_idx = columns.index("ResourceId")
        cost_idx = columns.index("PreTaxCost")
        for row in cost_data.rows:
            rid = normalize_resource_id(row[rid_idx])
            cost_val = row[cost_idx]
            resource_cost_map[rid] = cost_val

    matched_count = 0
    unmatched_count = 0
    underutilized_resources = []

    # Main resource loop
    for resource in resource_client.resources.list():
        tags = resource.tags or {}
        type_parts = resource.type.split("/") if resource.type else ["Unknown", "Unknown"]
        resource_type = type_parts[0].replace("Microsoft.", "").capitalize() if len(type_parts) > 0 else "Unknown"
        sub_resource_type = "bucket" if resource.type and "Microsoft.Storage/storageAccounts" in resource.type else (type_parts[1][0].upper() + type_parts[1][1:] if len(type_parts) > 1 else "Unknown")
        normalized_id = normalize_resource_id(resource.id)
        total_cost = resource_cost_map.get(normalized_id, "Unknown")

        if total_cost == "Unknown":
            unmatched_count += 1
        else:
            matched_count += 1

        finding_value = "Bucket underutilised" if resource.type and "Microsoft.Storage/storageAccounts" in resource.type else tags.get("Finding", "auto-generated from cost explorer").lower()
        recommendation_value = "Try Merging" if resource.type and "Microsoft.Storage/storageAccounts" in resource.type else tags.get("Recommendation", "review usage").lower()
        resource_type_value = "Storage" if resource.type and "Microsoft.Storage/storageAccounts" in resource.type else resource_type.lower()

        formatted_resource = {
            "_id": str(resource.id),
            "CloudProvider": tags.get("CloudProvider", "Azure"),
            "ManagementUnitId": subscription_id,
            "ApplicationCode": tags.get("ApplicationCode", "na").lower(),
            "CostCenter": tags.get("CostCenter", "na").lower(),
            "CIO": tags.get("CIO", "na").lower(),
            "Platform": tags.get("Platform", "na").lower(),
            "Lab": tags.get("Lab", "na").lower(),
            "Feature": tags.get("Feature", "na").lower(),
            "Owner": tags.get("Owner", "na").lower(),
            "TicketId": tags.get("Ticket", "na").lower(),
            "ResourceType": resource_type_value.capitalize(),
            "SubResourceType": sub_resource_type.lower(),
            "ResourceName": resource.name,
            "Region": resource.location if resource.location else "na",
            "TotalCost": total_cost,
            "Currency": tags.get("Currency", "usd").upper(),
            "Finding": finding_value,
            "Recommendation": recommendation_value,
            "Environment": tags.get("Environment", "na").lower(),
            "Timestamp": datetime.datetime.utcnow().isoformat() + "Z",
            "ConfidenceScore": tags.get("ConfidenceScore", "na"),
            "Status": tags.get("Status", "available").lower(),
            "Entity": tags.get("Entity", "na").lower(),
            "RootId": tenant_id,
            "Email": user_email
        }

        # Storage account underutilized logic
        if resource.type and "Microsoft.Storage/storageAccounts" in resource.type:
            resource_group_name = resource.id.split('/')[4] if len(resource.id.split('/')) > 4 else None
            if resource_group_name:
                stor_size_gb = get_storage_account_size(storage_client, resource_group_name, resource.name)
                if stor_size_gb is not None and sc_stor_size_in_gb is not None and stor_size_gb < sc_stor_size_in_gb:
                    formatted_resource["Current_Size"] = stor_size_gb
                    print(f"[UNDERUTILIZED] Storage Account: {resource.name} - Size: {stor_size_gb}GB")
                    underutilized_resources.append(formatted_resource)
            continue

        # VM underutilized logic
        if resource.type and "Microsoft.Compute/virtualMachines" in resource.type:
            avg_cpu = get_vm_average_cpu(monitor_client, resource.id, start_date.isoformat() + "Z", end_date.isoformat() + "Z")
            avg_memory = get_vm_average_memory(monitor_client, resource.id, start_date.isoformat() + "Z", end_date.isoformat() + "Z")
            avg_network = get_vm_average_network(monitor_client, resource.id, start_date.isoformat() + "Z", end_date.isoformat() + "Z")
            metrics = [m for m in [avg_cpu, avg_memory, avg_network] if m is not None]
            if metrics:
                total_avg = sum(metrics) / len(metrics)
                formatted_resource["Current_Avg_VM"] = total_avg
                if total_avg > VM_UNDERUTILIZED_CPU_THRESHOLD:
                    formatted_resource["Finding"] = "VM underutilised"
                    formatted_resource["Recommendation"] = "Scale Down"
                    underutilized_resources.append(formatted_resource)
                    print(f"[UNDERUTILIZED] VM: {resource.name} - Total Avg: {total_avg:.2f}")
            continue

        # Managed Disk logic
        if resource.type and "Microsoft.Compute/disks" in resource.type:
            disk = compute_client.disks.get(resource_group_name=resource.id.split('/')[4], disk_name=resource.name)
            disk_size_gb = disk.disk_size_gb
            disk_status = getattr(disk, "disk_state", None) or getattr(disk, "provisioning_state", None)
            attached = bool(disk.managed_by)
            underutilized = False
            if disk_size_gb is not None and disk_size_gb < DISK_QUOTA_GB:
                underutilized = True
            if not attached:
                underutilized = True
            if disk_status and disk_status.lower() != "succeeded":
                underutilized = True
            if underutilized:
                formatted_resource["Current_Disk_Size_GB"] = disk_size_gb
                formatted_resource["Disk_Status"] = disk_status
                formatted_resource["Disk_Attached"] = attached
                formatted_resource["Finding"] = "Disk underutilised"
                formatted_resource["Recommendation"] = "scale down"
                underutilized_resources.append(formatted_resource)
                print(f"[UNDERUTILIZED] Disk: {resource.name} - Size: {disk_size_gb}GB, Status: {disk_status}, Attached: {attached}")
            continue

        # Untagged resource logic
        missing_tags = [tag for tag in REQUIRED_TAG_NAMES if tag not in tags or not tags.get(tag)]
        if missing_tags:
            formatted_resource["TotalCost"] = 0 if total_cost == "Unknown" else total_cost
            formatted_resource["Finding"] = "Untagged resource"
            formatted_resource["MissingTags"] = ", ".join(missing_tags)
            formatted_resource["Recommendation"] = "Add Tag"
            underutilized_resources.append(formatted_resource)
            print(f"[UNTAGGED] Resource: {resource.name} - Missing tags: {', '.join(missing_tags)}")
            continue

    # --- Subnet analysis ---
    for vnet in network_client.virtual_networks.list_all():
        vnet_id_parts = vnet.id.split("/")
        resource_group_name = vnet_id_parts[4]
        for subnet in network_client.subnets.list(resource_group_name, vnet.name):
            if "default" in subnet.name.lower():
                print(f"  • {subnet.name} (Default VPC) - Skipped")
                continue
            prefix = subnet.address_prefix
            if not prefix:
                print(f"  • {subnet.name} (VNet: {vnet.name}) - Skipped (no address prefix)")
                continue
            total_ips = ipaddress.ip_network(prefix).num_addresses - 5
            used_ips = subnet.ip_configurations and len(subnet.ip_configurations) or 0
            free_ips = total_ips - used_ips
            free_percent = (free_ips / total_ips) * 100 if total_ips > 0 else 0
            print(f"  • {subnet.name} (VNet: {vnet.name}) - {free_percent:.2f}% free IPs")
            if free_percent > SUBNET_FREE_IP_THRESHOLD:
                formatted_resource = {
                    "_id": subnet.id,
                    "CloudProvider": "Azure",
                    "ManagementUnitId": subscription_id,
                    "ApplicationCode": tags.get("ApplicationCode", "na").lower(),
                    "CostCenter": tags.get("CostCenter", "na").lower(),
                    "CIO": tags.get("CIO", "na").lower(),
                    "Platform": tags.get("Platform", "na").lower(),
                    "Lab": tags.get("Lab", "na").lower(),
                    "Feature": tags.get("Feature", "na").lower(),
                    "Owner": tags.get("Owner", "na").lower(),
                    "TicketId": tags.get("Ticket", "na").lower(),
                    "ResourceType": "Network",
                    "SubResourceType": "subnet",
                    "ResourceName": subnet.name,
                    "Region": vnet.location if vnet.location else "na",
                    "TotalCost": 0,
                    "Currency": tags.get("Currency", "usd").upper(),
                    "Finding": "subnet underutilised",
                    "Recommendation": "scale down",
                    "Environment": tags.get("Environment", "na").lower(),
                    "Timestamp": datetime.datetime.utcnow().isoformat() + "Z",
                    "ConfidenceScore": tags.get("ConfidenceScore", "na"),
                    "Status": "available",
                    "Entity": tags.get("Entity", "na").lower(),
                    "RootId": tenant_id,
                    "Email": user_email,
                    "Current_Free_IP_Percent": free_percent,
                    "VNet": vnet.name,
                    "ResourceGroup": resource_group_name
                }
                underutilized_resources.append(formatted_resource)
                print(f"  ⚠️  {subnet.name} (VNet: {vnet.name}) - {free_percent:.2f}% free IPs (flagged)")

    # --- Orphaned resource detection ---
    # Orphaned Disks
    for disk in compute_client.disks.list():
        if not disk.managed_by:
            tags = disk.tags if hasattr(disk, "tags") and disk.tags else {}
            formatted_resource = {
                "_id": str(disk.id),
                "CloudProvider": tags.get("CloudProvider", "Azure"),
                "ManagementUnitId": subscription_id,
                "ApplicationCode": tags.get("ApplicationCode", "na").lower(),
                "CostCenter": tags.get("CostCenter", "na").lower(),
                "CIO": tags.get("CIO", "na").lower(),
                "Platform": tags.get("Platform", "na").lower(),
                "Lab": tags.get("Lab", "na").lower(),
                "Feature": tags.get("Feature", "na").lower(),
                "Owner": tags.get("Owner", "na").lower(),
                "TicketId": tags.get("Ticket", "na").lower(),
                "ResourceType": "Storage",
                "SubResourceType": "Disk",
                "ResourceName": disk.name,
                "Region": disk.location,
                "TotalCost": 0,
                "Currency": "USD",
                "Finding": "OrphandResource",
                "Recommendation": "Delete",
                "Environment": tags.get("Environment", "na").lower(),
                "Timestamp": datetime.datetime.utcnow().isoformat() + "Z",
                "ConfidenceScore": tags.get("ConfidenceScore", "na"),
                "Status": "available",
                "Entity": tags.get("Entity", "na").lower(),
                "RootId": tenant_id,
                "Email": user_email,
                "Size": f"{disk.disk_size_gb}GB"
            }
            underutilized_resources.append(formatted_resource)

    # Orphaned NICs
    for nic in network_client.network_interfaces.list_all():
        if not nic.virtual_machine:
            tags = nic.tags if hasattr(nic, "tags") and nic.tags else {}
            formatted_resource = {
                "_id": str(nic.id),
                "CloudProvider": tags.get("CloudProvider", "Azure"),
                "ManagementUnitId": subscription_id,
                "ApplicationCode": tags.get("ApplicationCode", "na").lower(),
                "CostCenter": tags.get("CostCenter", "na").lower(),
                "CIO": tags.get("CIO", "na").lower(),
                "Platform": tags.get("Platform", "na").lower(),
                "Lab": tags.get("Lab", "na").lower(),
                "Feature": tags.get("Feature", "na").lower(),
                "Owner": tags.get("Owner", "na").lower(),
                "TicketId": tags.get("Ticket", "na").lower(),
                "ResourceType": "Network",
                "SubResourceType": "NIC",
                "ResourceName": nic.name,
                "Region": nic.location,
                "TotalCost": 0,
                "Currency": "USD",
                "Finding": "OrphandResource",
                "Recommendation": "Delete",
                "Environment": tags.get("Environment", "na").lower(),
                "Timestamp": datetime.datetime.utcnow().isoformat() + "Z",
                "ConfidenceScore": tags.get("ConfidenceScore", "na"),
                "Status": "available",
                "Entity": tags.get("Entity", "na").lower(),
                "RootId": tenant_id,
                "Email": user_email,
                "Size": ""
            }
            underutilized_resources.append(formatted_resource)

    # Orphaned Public IPs
    for pip in network_client.public_ip_addresses.list_all():
        if not pip.ip_configuration:
            tags = pip.tags if hasattr(pip, "tags") and pip.tags else {}
            formatted_resource = {
                "_id": str(pip.id),
                "CloudProvider": tags.get("CloudProvider", "Azure"),
                "ManagementUnitId": subscription_id,
                "ApplicationCode": tags.get("ApplicationCode", "na").lower(),
                "CostCenter": tags.get("CostCenter", "na").lower(),
                "CIO": tags.get("CIO", "na").lower(),
                "Platform": tags.get("Platform", "na").lower(),
                "Lab": tags.get("Lab", "na").lower(),
                "Feature": tags.get("Feature", "na").lower(),
                "Owner": tags.get("Owner", "na").lower(),
                "TicketId": tags.get("Ticket", "na").lower(),
                "ResourceType": "Network",
                "SubResourceType": "PublicIps",
                "ResourceName": pip.name,
                "Region": pip.location,
                "TotalCost": 0,
                "Currency": "USD",
                "Finding": "OrphandResource",
                "Recommendation": "Delete",
                "Environment": tags.get("Environment", "na").lower(),
                "Timestamp": datetime.datetime.utcnow().isoformat() + "Z",
                "ConfidenceScore": tags.get("ConfidenceScore", "na"),
                "Status": "available",
                "Entity": tags.get("Entity", "na").lower(),
                "RootId": tenant_id,
                "Email": user_email,
                "Size": ""
            }
            underutilized_resources.append(formatted_resource)

     # Prepare lists for orphaned NSG analysis
    all_nsgs = list(network_client.network_security_groups.list_all())
    all_nics = list(network_client.network_interfaces.list_all())
    all_vnets = list(network_client.virtual_networks.list_all())
    all_subnets = []
    for vnet in all_vnets:
        rg_name = vnet.id.split("/")[4]
        all_subnets.extend(list(network_client.subnets.list(rg_name, vnet.name)))

    # Orphaned NSGs
    for nsg in all_nsgs:
        nsg_id = nsg.id
        nsg_nics = [nic for nic in all_nics if nic.network_security_group and nic.network_security_group.id == nsg_id]
        nsg_subnets = [subnet for subnet in all_subnets if subnet.network_security_group and subnet.network_security_group.id == nsg_id]
        security_rules = getattr(nsg, "security_rules", [])
        if len(nsg_nics) == 0 and len(nsg_subnets) == 0 and len(security_rules) == 0:
            tags = nsg.tags if hasattr(nsg, "tags") and nsg.tags else {}
            formatted_resource = {
                "_id": str(nsg.id),
                "CloudProvider": tags.get("CloudProvider", "Azure"),
                "ManagementUnitId": subscription_id,
                "ApplicationCode": tags.get("ApplicationCode", "na").lower(),
                "CostCenter": tags.get("CostCenter", "na").lower(),
                "CIO": tags.get("CIO", "na").lower(),
                "Platform": tags.get("Platform", "na").lower(),
                "Lab": tags.get("Lab", "na").lower(),
                "Feature": tags.get("Feature", "na").lower(),
                "Owner": tags.get("Owner", "na").lower(),
                "TicketId": tags.get("Ticket", "na").lower(),
                "ResourceType": "Network",
                "SubResourceType": "NSG",
                "ResourceName": nsg.name,
                "Region": nsg.location,
                "TotalCost": 0,
                "Currency": "USD",
                "Finding": "OrphandResource",
                "Recommendation": "Delete",
                "Environment": tags.get("Environment", "na").lower(),
                "Timestamp": datetime.datetime.utcnow().isoformat() + "Z",
                "ConfidenceScore": tags.get("ConfidenceScore", "na"),
                "Status": "available",
                "Entity": tags.get("Entity", "na").lower(),
                "RootId": tenant_id,
                "Email": user_email,
                "Size": ""
            }
            underutilized_resources.append(formatted_resource)

    # --- Orphaned NSG Flow Logs ---
    try:
        locations = [loc.name for loc in network_client.locations.list()]
        all_nsgs = {nsg.id: nsg for nsg in network_client.network_security_groups.list_all()}
        all_vnets = {vnet.id: vnet for vnet in network_client.virtual_networks.list_all()}
        for location in locations:
            network_watchers = list(network_client.network_watchers.list(location))
            for nw in network_watchers:
                try:
                    flow_logs = list(network_client.flow_logs.list(nw.resource_group_name, nw.name))
                except Exception as e:
                    print(f"[WARNING] Could not fetch flow logs for Network Watcher {nw.name}: {e}")
                    continue
                for flow_log in flow_logs:
                    # Orphaned NSG Flow Log
                    if hasattr(flow_log, "network_security_group") and flow_log.network_security_group:
                        nsg_id = flow_log.network_security_group.id
                        if nsg_id not in all_nsgs:
                            tags = flow_log.tags if hasattr(flow_log, "tags") and flow_log.tags else {}
                            formatted_resource = {
                                "_id": str(flow_log.id),
                                "CloudProvider": tags.get("CloudProvider", "Azure"),
                                "ManagementUnitId": subscription_id,
                                "ApplicationCode": tags.get("ApplicationCode", "na").lower(),
                                "CostCenter": tags.get("CostCenter", "na").lower(),
                                "CIO": tags.get("CIO", "na").lower(),
                                "Platform": tags.get("Platform", "na").lower(),
                                "Lab": tags.get("Lab", "na").lower(),
                                "Feature": tags.get("Feature", "na").lower(),
                                "Owner": tags.get("Owner", "na").lower(),
                                "TicketId": tags.get("Ticket", "na").lower(),
                                "ResourceType": "Network",
                                "SubResourceType": "NSGFlowLog",
                                "ResourceName": flow_log.name,
                                "Region": flow_log.location,
                                "TotalCost": 0,
                                "Currency": "USD",
                                "Finding": "OrphandResource",
                                "Recommendation": "Delete",
                                "Environment": tags.get("Environment", "na").lower(),
                                "Timestamp": datetime.datetime.utcnow().isoformat() + "Z",
                                "ConfidenceScore": tags.get("ConfidenceScore", "na"),
                                "Status": "available",
                                "Entity": tags.get("Entity", "na").lower(),
                                "RootId": tenant_id,
                                "Email": user_email,
                                "Size": ""
                            }
                            underutilized_resources.append(formatted_resource)
                    # Orphaned VNET Flow Log
                    elif hasattr(flow_log, "virtual_network") and flow_log.virtual_network:
                        vnet_id = flow_log.virtual_network.id
                        if vnet_id not in all_vnets:
                            tags = flow_log.tags if hasattr(flow_log, "tags") and flow_log.tags else {}
                            formatted_resource = {
                                "_id": str(flow_log.id),
                                "CloudProvider": tags.get("CloudProvider", "Azure"),
                                "ManagementUnitId": subscription_id,
                                "ApplicationCode": tags.get("ApplicationCode", "na").lower(),
                                "CostCenter": tags.get("CostCenter", "na").lower(),
                                "CIO": tags.get("CIO", "na").lower(),
                                "Platform": tags.get("Platform", "na").lower(),
                                "Lab": tags.get("Lab", "na").lower(),
                                "Feature": tags.get("Feature", "na").lower(),
                                "Owner": tags.get("Owner", "na").lower(),
                                "TicketId": tags.get("Ticket", "na").lower(),
                                "ResourceType": "Network",
                                "SubResourceType": "VNETFlowLog",
                                "ResourceName": flow_log.name,
                                "Region": flow_log.location,
                                "TotalCost": 0,
                                "Currency": "USD",
                                "Finding": "OrphandResource",
                                "Recommendation": "Delete",
                                "Environment": tags.get("Environment", "na").lower(),
                                "Timestamp": datetime.datetime.utcnow().isoformat() + "Z",
                                "ConfidenceScore": tags.get("ConfidenceScore", "na"),
                                "Status": "available",
                                "Entity": tags.get("Entity", "na").lower(),
                                "RootId": tenant_id,
                                "Email": user_email,
                                "Size": ""
                            }
                            underutilized_resources.append(formatted_resource)
    except Exception as e:
        print(f"[WARNING] Error during orphaned flow log detection: {e}")

    # --- Merge findings and recommendations by _id ---
    merged_resources = {}
    for res in underutilized_resources:
        key = res["_id"]
        if key in merged_resources:
            # Merge Finding
            existing_finding = merged_resources[key].get("Finding", "")
            new_finding = res.get("Finding", "")
            findings_set = set(existing_finding.split(";")) | set(new_finding.split(";"))
            merged_resources[key]["Finding"] = ";".join([f for f in findings_set if f])

            # Merge Recommendation
            existing_recommendation = merged_resources[key].get("Recommendation", "")
            new_recommendation = res.get("Recommendation", "")
            recommendations_set = set(existing_recommendation.split(";")) | set(new_recommendation.split(";"))
            merged_resources[key]["Recommendation"] = ";".join([r for r in recommendations_set if r])

            # Merge MissingTags if present
            if "MissingTags" in res:
                existing_tags = merged_resources[key].get("MissingTags", "")
                new_tags = res.get("MissingTags", "")
                tags_set = set(existing_tags.split(";")) | set(new_tags.split(";"))
                merged_resources[key]["MissingTags"] = ";".join([t for t in tags_set if t])
        else:
            merged_resources[key] = res

    underutilized_resources = list(merged_resources.values())

    # --- Save to JSON ---
    filename = "azure_underutilised.json"
    if underutilized_resources:
        try:
            with open(filename, 'w') as f:
                json.dump(underutilized_resources, f, indent=2, default=str)
            print(f"[INFO] Saved {len(underutilized_resources)} underutilized resources to {filename}")
        except Exception as e:
            print(f"[ERROR] Failed to save underutilized resources to JSON: {e}")
    else:
        print(f"[INFO] No underutilized resources found. JSON file not created.")

    # --- Insert into MongoDB ---
    try:
        json_test = json.dumps(underutilized_resources, default=str)
        print("[INFO] JSON validation passed - data is valid for MongoDB insertion")
        filter_query = {
            "CloudProvider": "Azure",
            "ManagementUnitId": subscription_id,
            "Email": user_email
        }
        existing_count = cost_insights_collection.count_documents(filter_query)
        if existing_count > 0:
            cost_insights_collection.delete_many(filter_query)
            print(f"[INFO] Cleared {existing_count} existing records from Cost_Insights collection")
        else:
            print("[INFO] Collection is empty, no records to clear")
        if underutilized_resources:
            cost_insights_collection.insert_many(underutilized_resources)
            print(f"[INFO] Inserted {len(underutilized_resources)} underutilized resources into database")
        else:
            print("[INFO] No underutilized resources found to insert")
    except json.JSONEncodeError as e:
        print(f"[ERROR] JSON validation failed: {e}")
        print("[ERROR] Skipping MongoDB insertion due to invalid JSON data")
    except Exception as e:
        print(f"[ERROR] Failed to insert data into database: {e}")

    # --- Close MongoDB connection ---
    try:
        client.close()
        print("[INFO] MongoDB connection closed successfully")
    except Exception as e:
        print(f"[WARNING] Error closing MongoDB connection: {e}")

    # --- Final summary ---
    print(f"[INFO] Total resources processed: {matched_count + unmatched_count}")
    print(f"[INFO] Matched resources with cost data: {matched_count}")
    print(f"[INFO] Unmatched resources (no cost data): {unmatched_count}")
    print(f"[INFO] Underutilized/Orphaned resources: {len(underutilized_resources)}")
    print("[INFO] Azure resource optimization analysis completed.")

if __name__ == "__main__":
    analyze_azure_resources()
