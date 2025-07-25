import re
import json
import datetime
from azure.identity import ClientSecretCredential
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.costmanagement import CostManagementClient

def normalize_resource_id(resource_id):
    clean_id = re.sub(r'[\u200b\xa0\s]+', '', resource_id)
    return clean_id.strip().rstrip('/').lower()

# Azure credentials
subscription_id = "45694c68-c5ea-452f-8bc1-7dcb3a143256"

# Authenticate
credential = ClientSecretCredential(tenant_id, client_id, client_secret)
resource_client = ResourceManagementClient(credential, subscription_id)
cost_client = CostManagementClient(credential)

# Cost query for last 30 days
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

print(f"Total resources with cost data: {len(resource_cost_map)}")
print("-" * 60)

matched_count = 0
unmatched_count = 0

# Iterate and format output
for resource in resource_client.resources.list():
    tags = resource.tags or {}
    type_parts = resource.type.split("/") if resource.type else ["Unknown", "Unknown"]
    resource_type = type_parts[0].replace("Microsoft.", "").capitalize() if len(type_parts) > 0 else "Unknown"
    sub_resource_type = type_parts[1][0].upper() + type_parts[1][1:] if len(type_parts) > 1 else "Unknown"

    normalized_id = normalize_resource_id(resource.id)
    total_cost = resource_cost_map.get(normalized_id, "Unknown")

    if total_cost == "Unknown":
        unmatched_count += 1
    else:
        matched_count += 1

    formatted_resource = {
        "_id": str(resource.id),
        "CloudProvider": tags.get("CloudProvider", "Azure"),
        "ManagementUnits": subscription_id,
        "ApplicationCode": tags.get("ApplicationCode", "Unknown"),
        "CostCenter": tags.get("CostCenter", "Unknown"),
        "Owner": tags.get("Owner", "Unknown"),
        "TicketId": tags.get("Ticket", "Unknown"),
        "ResourceType": resource_type,
        "SubResourceType": sub_resource_type,
        "ResourceName": resource.name,
        "Region": resource.location,
        "TotalCost": total_cost,
        "Currency": tags.get("Currency", "USD"),
        "Finding": tags.get("Finding", "Auto-generated from Cost Explorer"),
        "Recommendation": tags.get("Recommendation", "Review usage"),
        "Environment": tags.get("Environment", "Unknown"),
        "Timestamp": datetime.datetime.utcnow().isoformat() + "Z",
        "Tags": json.dumps(tags, indent=2),
        "ConfidenceScore": tags.get("ConfidenceScore", 0.0),
        "Status": tags.get("Status", "Unknown"),
        "Entity": tags.get("Entity", "Unknown")
    }

    for k, v in formatted_resource.items():
        print(f"{k}: {v}")
    print("-" * 60)

# Final summary
print(f"Total resources processed: {matched_count + unmatched_count}")
print(f"Matched resources with cost data: {matched_count}")
print(f"Unmatched resources (no cost data): {unmatched_count}")
