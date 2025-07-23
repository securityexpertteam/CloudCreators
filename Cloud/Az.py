from azure.identity import ClientSecretCredential
from azure.mgmt.resource import ResourceManagementClient
import datetime

# Azure credentials
subscription_id = "45694c68-c5ea-452f-8bc1-7dcb3a143256"

# Authenticate using ClientSecretCredential
credential = ClientSecretCredential(tenant_id, client_id, client_secret)

# Create a ResourceManagementClient
resource_client = ResourceManagementClient(credential, subscription_id)

# List and print all resources in the subscription in the requested format
for resource in resource_client.resources.list():
    tags = resource.tags if resource.tags else {}
    type_parts = resource.type.split("/") if resource.type else ["Unknown", "Unknown"]
    resource_type = (
        type_parts[0].replace("Microsoft.", "").capitalize() if len(type_parts) > 0 else "Unknown"
    )
    sub_resource_type = (
        type_parts[1][0].upper() + type_parts[1][1:] if len(type_parts) > 1 else "Unknown"
    )
    formatted_resource = {
        "_id": str(resource.id),
        "CloudProvider": "Azure",
        "ManagementUnits": subscription_id,
        "ApplicationCode": tags.get("ApplicationCode", "Unknown"),
        "CostCenter": tags.get("CostCenter", "Unknown"),
        "Owner": tags.get("Owner", "Unknown"),
        "TicketId": tags.get("TicketId", "Unknown"),
        "ResourceType": resource_type,
        "SubResourceType": sub_resource_type,
        "ResourceName": resource.name,
        "Region": resource.location,
        "TotalCost": tags.get("TotalCost", "Unknown"),
        "Currency": tags.get("Currency", "USD"),
        "Finding": tags.get("Finding", "Auto-generated from Cost Explorer"),
        "Recommendation": tags.get("Recommendation", "Review usage"),
        "Environment": tags.get("Environment", "Unknown"),
        "Timestamp": datetime.datetime.utcnow().isoformat() + "Z",
        "Tags": tags,
        "ConfidenceScore": tags.get("ConfidenceScore", 0.0),
        "Status": tags.get("Status", "Unknown"),
        "Entity": tags.get("Entity",  "Unknown")
    }
    for key, value in formatted_resource.items():
        print(f"{key}: {value}")
    print("-" * 40)