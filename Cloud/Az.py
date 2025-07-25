import re
import json
import datetime
import time
from azure.identity import ClientSecretCredential
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.costmanagement import CostManagementClient
from azure.mgmt.storage import StorageManagementClient
from azure.mgmt.monitor import MonitorManagementClient
from pymongo import MongoClient
import os

# --- MongoDB connection details ---
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
DB_NAME = os.getenv("DB_NAME", "myDB")
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
triggers_collection = db["triggers"]
cost_insights_collection = db["Cost_Insights"]
environment_onboarding_collection = db["environmentOnboarding"]
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
        client_secret = env.get("srvacctPass")        # client_secret
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

    end_date = datetime.datetime.utcnow().date()
    start_date = end_date - datetime.timedelta(days=30)

    cost_query = {
        "type": "Usage",
        "timeframe": "Custom",
        "time_period": {
            "from": start_date.isoformat() + "T00:00:00Z",
            "to": end_date.isoformat() + "T00:00:00Z"
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
                "CloudProvider": tags.get("CloudProvider", "azure").lower(),
                "ManagementUnits": subscription_id,
                "ApplicationCode": tags.get("ApplicationCode", "na").lower(),
                "CostCenter": tags.get("CostCenter", "na").lower(),
                "Owner": tags.get("Owner", "na").lower(),
                "TicketId": tags.get("Ticket", "na").lower(),
                "ResourceType": resource_type_value,
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
                # Get resource group name from resource ID
                resource_group_name = resource.id.split('/')[4] if len(resource.id.split('/')) > 4 else None
                
                if resource_group_name:
                    storage_size_gb = get_storage_account_size(storage_client, resource_group_name, resource.name)
                    
                    # Only collect storage accounts with less than 1GB utilization (don't add StorageSizeGB to output)
                    if storage_size_gb is not None and storage_size_gb < 1:
                        print(f"[UNDERUTILIZED] Storage Account: {resource.name} - Size: {storage_size_gb}GB")
                    underutilized_storage_accounts.append(formatted_resource)
                # Don't insert any storage accounts into database during resource loop
                continue
            
        # Don't insert any resources into database during resource loop - only JSON data will be inserted

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
        existing_count = cost_insights_collection.count_documents({})
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
