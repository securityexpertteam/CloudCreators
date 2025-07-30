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

# --- MongoDB connection details ---
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
DB_NAME = os.getenv("DB_NAME", "myDB")
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
triggers_collection = db["triggers"]
cost_insights_collection = db["Cost_Insights"]
environment_onboarding_collection = db["environmentOnboarding"]
standard_config_collection = db["standardConfigsDb"]

# Get storage_size value from standardConfigsDb collection
storage_config = standard_config_collection.find_one({}, {"storage_size": 1, "_id": 0})
sc_stor_size_in_gb = storage_config.get("storage_size") if storage_config else 1  # Default to 1 if not found
VM_UNDERUTILIZED_CPU_THRESHOLD = 15
VM_UNDERUTILIZED_MEMORY_THRESHOLD = 30
VM_UNDERUTILIZED_NETWORK_THRESHOLD = 40
VM_UNDERUTILIZED_TOTAL_AVG_THRESHOLD = 30
SUBNET_FREE_IP_THRESHOLD = 90  # percent
DISK_QUOTA_GB = int(os.getenv("DISK_QUOTA_GB", 100))  # Default to 100GB if not set

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
    """Analyze Azure resources and identify underutilized storage accounts."""
    print("[INFO] Starting Azure resource optimization analysis...")
    
    # Get the latest user from users collection for Azure credentials
    try:
        latest_env = environment_onboarding_collection.find({"cloudName": "Azure"}).sort("_id", -1).limit(1)
        env = next(latest_env, None)
        if not env:
            print("[ERROR] No Azure environment found in environmentOnboarding collection")
            return
        
        # Extract Azure credentials from user record
        client_id = env.get("srvaccntName")           # client_id
        client_secret = env.get("srvacctPass") 
       
        tenant_id = env.get("rootId")                 # tenant_id
        subscription_id = env.get("managementUnitId") # subscription_id
        
        if not all([client_id, client_secret, tenant_id, subscription_id]):
            print(f"[ERROR] Missing Azure credentials in environmentOnboarding record: {env.get('_id')}")
            return
            
        #print(f"[INFO] Using credentials from environmentOnboarding: {client_id}")
        
    except Exception as e:
        print(f"[ERROR] Failed to retrieve user credentials: {e}")
        return

    # === Azure scan logic ===
    credential = ClientSecretCredential(tenant_id, client_id, client_secret)
    resource_client = ResourceManagementClient(credential, subscription_id)
    cost_client = CostManagementClient(credential)
    storage_client = StorageManagementClient(credential, subscription_id)
    monitor_client = MonitorManagementClient(credential, subscription_id)
    compute_client = ComputeManagementClient(credential, subscription_id)
    network_client = NetworkManagementClient(credential, subscription_id)

    end_date = datetime.datetime.utcnow()
    start_date = end_date - datetime.timedelta(days=30)

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
    underutilized_storage_accounts = []

    # Iterate and format output
    for resource in resource_client.resources.list():
        tags = resource.tags or {}
        type_parts = resource.type.split("/") if resource.type else ["Unknown", "Unknown"]
        resource_type = type_parts[0].replace("Microsoft.", "").capitalize() if len(type_parts) > 0 else "Unknown"
        
        # Set SubResourceType to "bucket" for storage accounts, otherwise use original logic
        if resource.type and "Microsoft.Storage/storageAccounts" in resource.type:
            sub_resource_type = "bucket"
        else:
            sub_resource_type = type_parts[1][0].upper() + type_parts[1][1:] if len(type_parts) > 1 else "Unknown"

        normalized_id = normalize_resource_id(resource.id)
        total_cost = resource_cost_map.get(normalized_id, "Unknown")

        if total_cost == "Unknown":
            unmatched_count += 1
        else:
            matched_count += 1

        # Set specific values for storage accounts vs other resources
        if resource.type and "Microsoft.Storage/storageAccounts" in resource.type:
            finding_value = "Bucket underutilised"
            recommendation_value = "Try Merging"
            resource_type_value = "Storage"
        else:
            finding_value = tags.get("Finding", "auto-generated from cost explorer").lower()
            recommendation_value = tags.get("Recommendation", "review usage").lower()
            resource_type_value = resource_type.lower()

        formatted_resource = {
            "_id": str(resource.id),
            "CloudProvider": tags.get("CloudProvider", "Azure"),
            "ManagementUnitId": subscription_id,
            "ApplicationCode": tags.get("ApplicationCode", "na").lower(),
            "CostCenter": tags.get("CostCenter", "na").lower(),
            "CIO":tags.get("CIO", "na").lower(),
            "Platform":tags.get("Platform", "na").lower(),
            "Lab":tags.get("Lab", "na").lower(),
            "Feature":tags.get("Feature", "na").lower(),
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
            "Email": env.get("email", "")      
        }

        # Check if this is a storage account and handle filtering
        if resource.type and "Microsoft.Storage/storageAccounts" in resource.type:
            resource_group_name = resource.id.split('/')[4] if len(resource.id.split('/')) > 4 else None
            if resource_group_name:
                storage_size_gb = get_storage_account_size(storage_client, resource_group_name, resource.name)
                if storage_size_gb is not None and storage_size_gb < sc_stor_size_in_gb:
                    formatted_resource["Current_Size"] = storage_size_gb
                    print(f"[UNDERUTILIZED] Storage Account: {resource.name} - Size: {storage_size_gb}GB")
                    underutilized_storage_accounts.append(formatted_resource)
            continue

        # --- Virtual Machine logic ---
        if resource.type and "Microsoft.Compute/virtualMachines" in resource.type:
            avg_cpu = get_vm_average_cpu(
                monitor_client,
                resource.id,
                start_date.isoformat() + "Z",
                end_date.isoformat() + "Z"
            )
            avg_memory = get_vm_average_memory(
                monitor_client,
                resource.id,
                start_date.isoformat() + "Z",
                end_date.isoformat() + "Z"
            )
            avg_network = get_vm_average_network(
                monitor_client,
                resource.id,
                start_date.isoformat() + "Z",
                end_date.isoformat() + "Z"
            )

            metrics = [m for m in [avg_cpu, avg_memory, avg_network] if m is not None]
            if metrics:
                total_avg = sum(metrics) / len(metrics)
                formatted_resource["Current_Avg_VM"] = total_avg

                if total_avg > VM_UNDERUTILIZED_TOTAL_AVG_THRESHOLD:
                    formatted_resource["Finding"] = "VM underutilised"
                    formatted_resource["Recommendation"] = "Scale Down"
                    underutilized_storage_accounts.append(formatted_resource)
                    print(f"[UNDERUTILIZED] VM: {resource.name} - Total Avg: {total_avg:.2f}")
            continue

        # --- Managed Disk logic ---
        if resource.type and "Microsoft.Compute/disks" in resource.type:
            # Get disk details
            disk = compute_client.disks.get(resource_group_name=resource.id.split('/')[4], disk_name=resource.name)
            disk_size_gb = disk.disk_size_gb
            disk_status = getattr(disk, "disk_state", None) or getattr(disk, "provisioning_state", None)
            attached = bool(disk.managed_by)
            
            findings = []
            recommendations = []
            underutilized = False

            if disk_size_gb is not None and disk_size_gb < DISK_QUOTA_GB:
                findings.append("disk small")
                recommendations.append("scale down")
                underutilized = True

            if not attached:
                findings.append("disk unattached")
                recommendations.append("delete or attach")
                underutilized = True

            if disk_status and disk_status.lower() != "succeeded":
                findings.append(f"disk status {disk_status}")
                recommendations.append("investigate")
                underutilized = True

            if underutilized:
                formatted_resource["Current_Disk_Size_GB"] = disk_size_gb
                formatted_resource["Disk_Status"] = disk_status
                formatted_resource["Disk_Attached"] = attached
                formatted_resource["Finding"] = ", ".join(findings)
                formatted_resource["Recommendation"] = ", ".join(recommendations)
                underutilized_storage_accounts.append(formatted_resource)
                print(f"[UNDERUTILIZED] Disk: {resource.name} - Size: {disk_size_gb}GB, Status: {disk_status}, Attached: {attached}")
            continue

        # Don't insert any resources into database during resource loop - only JSON data will be inserted

    # --- Subnet analysis (after main resource loop) ---
    for vnet in network_client.virtual_networks.list_all():
        vnet_id_parts = vnet.id.split("/")
        resource_group_name = vnet_id_parts[4]
        for subnet in network_client.subnets.list(resource_group_name, vnet.name):
            # Exclude default subnets
            if "default" in subnet.name.lower():
                print(f"  • {subnet.name} (Default VPC) - Skipped")
                continue

            prefix = subnet.address_prefix
            if not prefix:
                print(f"  • {subnet.name} (VNet: {vnet.name}) - Skipped (no address prefix)")
                continue

            total_ips = ipaddress.ip_network(prefix).num_addresses - 5  # Azure reserves 5 IPs per subnet
            used_ips = subnet.ip_configurations and len(subnet.ip_configurations) or 0
            free_ips = total_ips - used_ips
            free_percent = (free_ips / total_ips) * 100 if total_ips > 0 else 0

            print(f"  • {subnet.name} (VNet: {vnet.name}) - {free_percent:.2f}% free IPs")
            if free_percent > SUBNET_FREE_IP_THRESHOLD:
                # Build formatted_resource for subnet using the same structure as storage accounts
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
                    "TotalCost": "na",
                    "Currency": tags.get("Currency", "usd").upper(),
                    "Finding": "subnet underutilised",
                    "Recommendation": "scale down",
                    "Environment": tags.get("Environment", "na").lower(),
                    "Timestamp": datetime.datetime.utcnow().isoformat() + "Z",
                    "ConfidenceScore": tags.get("ConfidenceScore", "na"),
                    "Status": "available",
                    "Entity": tags.get("Entity", "na").lower(),
                    "RootId": tenant_id,
                    "Email": env.get("email", ""),
                    "Current_Free_IP_Percent": free_percent,
                    "VNet": vnet.name,
                    "ResourceGroup": resource_group_name
                }
                underutilized_storage_accounts.append(formatted_resource)
                print(f"  ⚠️  {subnet.name} (VNet: {vnet.name}) - {free_percent:.2f}% free IPs (flagged)")

    # Create and save underutilized storage accounts to fixed JSON file (replace every time)
    filename = "azure_underutilised.json"
    
    if underutilized_storage_accounts:
        try:
            with open(filename, 'w') as f:
                json.dump(underutilized_storage_accounts, f, indent=2, default=str)
            print(f"[INFO] Saved {len(underutilized_storage_accounts)} underutilized storage accounts to {filename}")
        except Exception as e:
            print(f"[ERROR] Failed to save underutilized storage accounts to JSON: {e}")
    else:
        # Create empty JSON file even when no underutilized storage accounts found
        try:
            with open(filename, 'w') as f:
                json.dump([], f, indent=2)
            print(f"[INFO] Created empty JSON file {filename} - no underutilized storage accounts found")
        except Exception as e:
            print(f"[ERROR] Failed to create empty JSON file: {e}")

    # Insert ONLY underutilized storage accounts into database based on JSON file content
    try:
        # Validate JSON before insertion
        json_test = json.dumps(underutilized_storage_accounts, default=str)
        print("[INFO] JSON validation passed - data is valid for MongoDB insertion")
        

          # Clear existing records from the collection before inserting new data
        filter_query = {
            "CloudProvider": "Azure",
            "ManagementUnitId": subscription_id,
            "Email": env.get("email", "") 
        }
       
        # Clear existing records from the collection before inserting new data
        existing_count = cost_insights_collection.count_documents(filter_query)
        if existing_count > 0:
            cost_insights_collection.delete_many({})
            print(f"[INFO] Cleared {existing_count} existing records from Cost_Insights collection")
        else:
            print("[INFO] Collection is empty, no records to clear")
            
        # Insert underutilized storage accounts into database
        if underutilized_storage_accounts:
            cost_insights_collection.insert_many(underutilized_storage_accounts)
            print(f"[INFO] Inserted {len(underutilized_storage_accounts)} underutilized storage accounts into database")
        else:
            print("[INFO] No underutilized storage accounts found to insert")
            
    except json.JSONEncodeError as e:
        print(f"[ERROR] JSON validation failed: {e}")
        print("[ERROR] Skipping MongoDB insertion due to invalid JSON data")
    except Exception as e:
        print(f"[ERROR] Failed to insert data into database: {e}")

    # Close MongoDB connection
    try:
        client.close()
        print("[INFO] MongoDB connection closed successfully")
    except Exception as e:
        print(f"[WARNING] Error closing MongoDB connection: {e}")

    # Final summary
    print(f"[INFO] Total resources processed: {matched_count + unmatched_count}")
    print(f"[INFO] Matched resources with cost data: {matched_count}")
    print(f"[INFO] Unmatched resources (no cost data): {unmatched_count}")
    print(f"[INFO] Underutilized storage accounts (<1GB): {len(underutilized_storage_accounts)}")
    print("[INFO] Azure resource optimization analysis completed.")

if __name__ == "__main__":
    analyze_azure_resources()
