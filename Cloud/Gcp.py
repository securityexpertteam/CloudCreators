from google.cloud import asset_v1, monitoring_v3
from google.oauth2 import service_account
from datetime import datetime, timedelta
import time

def get_average_utilization(project_id, resource_type, resource_name, credentials):
    """
    Get average utilization for a resource over the last 7 days based on resource type.
    
    Args:
        project_id (str): GCP project ID
        resource_type (str): Asset type (e.g., compute.googleapis.com/Instance)
        resource_name (str): Resource name
        credentials: Service account credentials
    
    Returns:
        float or None: Average utilization (0-100%) or None if not applicable
    """
    client = monitoring_v3.MetricServiceClient(credentials=credentials)
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(days=7)
    
    # Convert times to seconds since epoch
    interval = monitoring_v3.TimeInterval(
        {
            "end_time": {"seconds": int(end_time.timestamp())},
            "start_time": {"seconds": int(start_time.timestamp())}
        }
    )
    
    # Define metric based on resource type
    if resource_type == "compute.googleapis.com/Instance":
        metric_type = "compute.googleapis.com/instance/cpu/utilization"
        # Format: projects/_/zones/<zone>/instances/<instance>
        parts = resource_name.split("/")
        if len(parts) >= 6:
            zone, instance = parts[-3], parts[-1]
            filter_str = f'metric.type="{metric_type}" AND resource.labels.instance_id="{instance}" AND resource.labels.zone="{zone}"'
        else:
            return None
    elif resource_type in [
        "compute.googleapis.com/Network",
        "compute.googleapis.com/Subnetwork",
        "compute.googleapis.com/Firewall",
        "compute.googleapis.com/Router"
    ]:
        # Use sent bytes for network resources (instance-level)
        metric_type = "compute.googleapis.com/instance/network/sent_bytes_count"
        parts = resource_name.split("/")
        if "instances" in resource_name and len(parts) >= 6:
            zone, instance = parts[-3], parts[-1]
            filter_str = f'metric.type="{metric_type}" AND resource.labels.instance_id="{instance}" AND resource.labels.zone="{zone}"'
        else:
            return None  # Global network resources lack direct metrics
    elif resource_type == "compute.googleapis.com/Disk":
        metric_type = "compute.googleapis.com/instance/disk/read_bytes_count"
        parts = resource_name.split("/")
        if len(parts) >= 6:
            zone, disk = parts[-3], parts[-1]
            filter_str = f'metric.type="{metric_type}" AND resource.labels.device_name="{disk}" AND resource.labels.zone="{zone}"'
        else:
            return None
    elif resource_type == "storage.googleapis.com/Bucket":
        metric_type = "storage.googleapis.com/api/request_count"
        bucket_name = resource_name.split("/")[-1]
        filter_str = f'metric.type="{metric_type}" AND resource.labels.bucket_name="{bucket_name}"'
    else:
        return None  # No utilization metric available
    
    try:
        results = client.list_time_series(
            request={
                "name": f"projects/{project_id}",
                "filter": filter_str,
                "interval": interval,
                "view": monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL
            }
        )
        
        # Calculate average utilization
        total_value = 0.0
        count = 0
        for result in results:
            for point in result.points:
                total_value += point.value.double_value
                count += 1
        
        if count == 0:
            return 0.0  # No data points
        if resource_type == "compute.googleapis.com/Instance":
            # CPU utilization is already in percentage
            return (total_value / count) * 100
        elif resource_type in [
            "compute.googleapis.com/Network",
            "compute.googleapis.com/Subnetwork",
            "compute.googleapis.com/Firewall",
            "compute.googleapis.com/Router"
        ]:
            # Normalize network bytes (1GB/s as max)
            return min((total_value / count) / 1_000_000_000, 100.0)  # Scale to 0-100
        elif resource_type == "compute.googleapis.com/Disk":
            # Normalize disk read bytes (1MB/s as max)
            return min((total_value / count) / 1_000_000, 100.0)  # Scale to 0-100
        elif resource_type == "storage.googleapis.com/Bucket":
            # Normalize request count (100 requests/s as max)
            return min((total_value / count) / 100, 100.0)  # Scale to 0-100
        return None
    except Exception as e:
        print(f"Error fetching utilization for {resource_name}: {e}")
        return None

def categorize_gcp_resources(project_id, credentials_path):
    """
    List all resources in a GCP project, check type-specific utilization, and categorize low usage (<20%).
    
    Args:
        project_id (str): GCP project ID
        credentials_path (str): Path to service account JSON key file
    """
    # Define categories for resource types
    network_resources = [
        'compute.googleapis.com/Network',
        'compute.googleapis.com/Subnetwork',
        'compute.googleapis.com/Firewall',
        'compute.googleapis.com/Router',
        'compute.googleapis.com/Address',
        'compute.googleapis.com/GlobalAddress',
        'compute.googleapis.com/Route',
        'compute.googleapis.com/VpnTunnel',
        'compute.googleapis.com/Interconnect',
        'compute.googleapis.com/LoadBalancer'
    ]
    
    storage_resources = [
        'storage.googleapis.com/Bucket',
        'compute.googleapis.com/Disk',
        'file.googleapis.com/Instance',
        'bigquery.googleapis.com/Dataset',
        'bigquery.googleapis.com/Table',
        'spanner.googleapis.com/Instance',
        'spanner.googleapis.com/Database'
    ]
    
    security_resources = [
        'iam.googleapis.com/ServiceAccount',
        'iam.googleapis.com/Role',
        'cloudkms.googleapis.com/CryptoKey',
        'cloudkms.googleapis.com/KeyRing',
        'securitycenter.googleapis.com/Finding',
        'accesscontextmanager.googleapis.com/AccessPolicy',
        'accesscontextmanager.googleapis.com/AccessLevel'
    ]
    
    # Create credentials object from service account file
    credentials = service_account.Credentials.from_service_account_file(credentials_path)
    
    # Initialize Asset Inventory client
    client = asset_v1.AssetServiceClient(credentials=credentials)
    
    # Define the scope (project)
    scope = f"projects/{project_id}"
    
    # Initialize categorized lists
    network = []
    storage = []
    security = []
    other = []
    low_util_instances = []
    low_util_network = []
    low_util_storage = []
    low_util_security = []
    low_util_other = []
    
    try:
        # Query all resources in the project
        response = client.search_all_resources(
            request={
                "scope": scope,
                "asset_types": [],  # Empty list means all asset types
                "page_size": 500
            }
        )
        
        # Categorize resources and check utilization
        for resource in response:
            asset_type = resource.asset_type
            resource_name = resource.name
            
            # Get utilization
            utilization = get_average_utilization(project_id, asset_type, resource_name, credentials)
            
            # Categorize resource and check low utilization
            if asset_type == "compute.googleapis.com/Instance":
                other.append((asset_type, resource_name, utilization))
                if utilization is not None and utilization < 20.0:
                    low_util_instances.append((asset_type, resource_name, utilization))
            elif asset_type in network_resources:
                network.append((asset_type, resource_name, utilization))
                if utilization is not None and utilization < 20.0:
                    low_util_network.append((asset_type, resource_name, utilization))
            elif asset_type in storage_resources:
                storage.append((asset_type, resource_name, utilization))
                if utilization is not None and utilization < 20.0:
                    low_util_storage.append((asset_type, resource_name, utilization))
            elif asset_type in security_resources:
                security.append((asset_type, resource_name, utilization))
                if utilization is not None and utilization < 20.0:
                    low_util_security.append((asset_type, resource_name, utilization))
            else:
                other.append((asset_type, resource_name, utilization))
                if utilization is not None and utilization < 20.0:
                    low_util_other.append((asset_type, resource_name, utilization))
        
        # Print categorized resources
        print(f"\nResources in project {project_id}:\n")
        
        print("Network-Related Resources:")
        for asset_type, name, util in network:
            util_str = f"{util:.2f}%" if util is not None else "N/A"
            print(f"- {asset_type}: {name} (Utilization: {util_str})")
        if not network:
            print("- None")
        
        print("\nStorage-Related Resources:")
        for asset_type, name, util in storage:
            util_str = f"{util:.2f}%" if util is not None else "N/A"
            print(f"- {asset_type}: {name} (Utilization: {util_str})")
        if not storage:
            print("- None")
        
        print("\nSecurity-Related Resources:")
        for asset_type, name, util in security:
            util_str = f"{util:.2f}%" if util is not None else "N/A"
            print(f"- {asset_type}: {name} (Utilization: {util_str})")
        if not security:
            print("- None")
        
        print("\nOther Resources:")
        for asset_type, name, util in other:
            util_str = f"{util:.2f}%" if util is not None else "N/A"
            print(f"- {asset_type}: {name} (Utilization: {util_str})")
        if not other:
            print("- None")
        
        # Print low-utilization resources by category
        print("\nLow Utilization Instances (<20% CPU):")
        if low_util_instances:
            for asset_type, name, util in low_util_instances:
                print(f"- {asset_type}: {name} (CPU Utilization: {util:.2f}%)")
        else:
            print("- None")
        
        print("\nLow Utilization Network Resources (<20% Network Throughput):")
        if low_util_network:
            for asset_type, name, util in low_util_network:
                print(f"- {asset_type}: {name} (Network Utilization: {util:.2f}%)")
        else:
            print("- None")
        
        print("\nLow Utilization Storage Resources (<20%):")
        if low_util_storage:
            for asset_type, name, util in low_util_storage:
                print(f"- {asset_type}: {name} (Utilization: {util:.2f}%)")
        else:
            print("- None")
        
        print("\nLow Utilization Security Resources (<20%):")
        if low_util_security:
            for asset_type, name, util in low_util_security:
                print(f"- {asset_type}: {name} (Utilization: {util:.2f}%)")
        else:
            print("- None")
        
        print("\nLow Utilization Other Resources (<20%):")
        if low_util_other:
            for asset_type, name, util in low_util_other:
                print(f"- {asset_type}: {name} (Utilization: {util:.2f}%)")
        else:
            print("- None")
    
    except Exception as e:
        print(f"Error retrieving resources: {e}")

if __name__ == "__main__":
    PROJECT_ID = "complete-stock-419311"
    CREDENTIALS_PATH = r"C:\Users\dasar\OneDrive\Documents\cloud_optimisation\complete-stock-419311-2a1494e5cfba.json"
    
    categorize_gcp_resources(PROJECT_ID, CREDENTIALS_PATH)
