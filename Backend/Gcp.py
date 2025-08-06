import argparse
import sys
import io
from datetime import datetime, timedelta, UTC
import ipaddress
import json
import os

# Import required Google Cloud libraries
from google.cloud import asset_v1, monitoring_v3
from google.cloud import storage as gcs_storage
from google.oauth2 import service_account
from googleapiclient import discovery
from googleapiclient.errors import HttpError  # Import HttpError for potential API errors

# Ensure stdout is UTF-8 for proper printing
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# ================================================================================
# ARGUMENT PARSING
# ================================================================================
parser = argparse.ArgumentParser(description="GCP Resource Optimization Script")
parser.add_argument("--client_email", required=True, help="GCP Service Account Client Email (for authentication)")
parser.add_argument("--private_key", required=True,
                    help="GCP Service Account Private Key. Replace newlines with '\\n'.")
parser.add_argument("--project_id", required=True, help="GCP Project ID to analyze")
parser.add_argument("--user_email", required=True, help="User Email for fetching configs from MongoDB and for reports")
args = parser.parse_args()

PROJECT_ID = args.project_id
USER_EMAIL = args.user_email  # Use the email from command line consistently

print("🚀 GCP Resource Optimization Analysis")
print("=" * 80)
print(f"Project to Analyze: {PROJECT_ID}")
print(f"Configuration for User Email: {USER_EMAIL}")
print(f"Authenticating with Service Account: {args.client_email}")
print("=" * 80)

# ================================================================================
# AUTHENTICATION
# ================================================================================
try:
    pk_string = args.private_key.replace('\\n', '\n')
    credentials_info = {
        "type": "service_account",
        "project_id": PROJECT_ID,
        "private_key": pk_string,
        "client_email": args.client_email,
        "token_uri": "https://oauth2.googleapis.com/token",
    }
    # Create a single, reusable credentials object from the arguments
    gcp_credentials = service_account.Credentials.from_service_account_info(credentials_info)
    print("✅ Authentication successful. Credentials object created.")

except Exception as e:
    print(f"❌ Critical Error: Failed to create credentials from arguments. Please check your inputs. Error: {e}")
    exit(1)  # Exit if authentication fails

# Initialize the global compute client with the new credentials
compute = discovery.build('compute', 'v1', credentials=gcp_credentials)

# ================================================================================
# MONGODB INTEGRATION (Conditional Import)
# ================================================================================
try:
    from pymongo import MongoClient

    MONGODB_AVAILABLE = True
except ImportError:
    MONGODB_AVAILABLE = False
    print("⚠️  pymongo not available. Install with: pip install pymongo")

# ================================================================================
# CONFIGURATION
# ================================================================================

# MongoDB Configuration
MONGODB_HOST = "localhost"  # Change this to your MongoDB server IP/hostname
MONGODB_PORT = 27017  # Change this to your MongoDB port
MONGODB_DATABASE = "myDB"  # Change this to your database name
MONGODB_COLLECTION = "Cost_Insights"  # Change this to your collection name


def get_thresholds_from_mongodb(email, collection_name="thresholds"):
    """
    Fetch analysis thresholds from a MongoDB collection based on user email.

    Args:
        email (str): The user email to look up for settings.
        collection_name (str): The name of the collection containing the thresholds.

    Returns:
        dict: A dictionary of thresholds.
    """
    # --- Define default thresholds in case some are missing ---
    defaults = {
        'cmp_cpu_usage': 15.0,
        'storage_utilization': 20.0,
        'disk_underutilized_gb': 100,
        'subnet_free_ip_percentage': 90.0,
        'snapshot_age_threshold_days': 90,  # Default for orphaned snapshots
        'gke_low_node_threshold': 1,  # Default for GKE clusters
        'gke_low_cpu_util_threshold': 5.0,  # Default for GKE clusters
        'gke_low_mem_util_threshold': 10.0,  # Default for GKE clusters
        'pv_low_utilization_threshold': 1.0  # Default for K8s PVs
    }

    if not MONGODB_AVAILABLE:
        print("⚠️ pymongo not available. Using default thresholds.")
        return defaults

    try:
        client = MongoClient(host=MONGODB_HOST, port=MONGODB_PORT)
        db = client[MONGODB_DATABASE]
        thresholds_collection = db[collection_name]

        user_thresholds = thresholds_collection.find_one({"email": email})
        client.close()

        if user_thresholds:
            print(f"✅ Retrieved thresholds from MongoDB for {email}")
            # Merge user settings with defaults to ensure all keys are present
            defaults.update(user_thresholds)
            return defaults
        else:
            print(f"⚠️ No thresholds found for {email}. Using default values.")
            return defaults

    except Exception as e:
        print(f"❌ Error fetching thresholds from MongoDB: {e}. Using default values.")
        return defaults


# ================================================================================
# UTILITY FUNCTIONS
# ================================================================================

def get_average_utilization(project_id, resource_type, resource_name, credentials, bucket_quota_bytes=100_000_000_000):
    """
    Calculate average resource utilization over the last 7 days using GCP Monitoring API.

    Args:
        project_id (str): GCP project ID
        resource_type (str): Asset type (e.g., compute.googleapis.com/Instance)
        resource_name (str): Full resource name
        credentials: Service account credentials
        bucket_quota_bytes (int): Bucket quota in bytes for utilization calculation

    Returns:
        float or None: Average utilization percentage (0-100%) or None if not available
    """
    client = monitoring_v3.MetricServiceClient(credentials=credentials)

    # Set time window for the last 7 days
    end_time = datetime.now(UTC)
    start_time = end_time - timedelta(days=7)

    interval = monitoring_v3.TimeInterval(
        {
            "end_time": {"seconds": int(end_time.timestamp())},
            "start_time": {"seconds": int(start_time.timestamp())}
        }
    )

    # Build metric filter based on resource type
    if resource_type == "compute.googleapis.com/Instance":
        # VM CPU utilization
        metric_type = "compute.googleapis.com/instance/cpu/utilization"
        parts = resource_name.split("/")
        if len(parts) >= 6:
            zone, instance = parts[-3], parts[-1]
            filter_str = f'metric.type="{metric_type}" AND resource.labels.instance_id="{instance}" AND resource.labels.zone="{zone}"'
        else:
            return None
    elif resource_type == "storage.googleapis.com/Bucket":
        # Bucket storage utilization
        metric_type = "storage.googleapis.com/storage/total_bytes"
        bucket_name = resource_name.split("/")[-1]
        filter_str = f'metric.type="{metric_type}" AND resource.labels.bucket_name="{bucket_name}"'
    else:
        # Unsupported resource type
        return None

    try:
        # Query monitoring API
        results = client.list_time_series(
            request={
                "name": f"projects/{project_id}",
                "filter": filter_str,
                "interval": interval,
                "view": monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL
            }
        )

        # Calculate average from all data points
        total_value = 0.0
        count = 0
        for result in results:
            for point in result.points:
                total_value += point.value.double_value
                count += 1

        if count == 0:
            return 0.0

        # Return utilization based on resource type
        if resource_type == "compute.googleapis.com/Instance":
            return (total_value / count) * 100  # CPU already in percentage
        elif resource_type == "storage.googleapis.com/Bucket":
            avg_bytes = total_value / count
            return min((avg_bytes / bucket_quota_bytes) * 100, 100.0)  # Bucket utilization percentage

        return None
    except Exception as e:
        print(f"Error fetching utilization for {resource_name}: {e}")
        return None


def get_bucket_size_gcs(bucket_name, credentials):
    """
    Get total size of all objects in a GCS bucket using Storage API.

    Args:
        bucket_name (str): Name of the bucket
        credentials: Service account credentials

    Returns:
        int or None: Total size in bytes, or None if error
    """
    client = gcs_storage.Client(credentials=credentials)
    try:
        total_bytes = 0
        blobs = client.list_blobs(bucket_name)
        for blob in blobs:
            if blob.size:
                total_bytes += blob.size
        return total_bytes
    except Exception as e:
        print(f"Error fetching bucket size for {bucket_name}: {e}")
        return None


def get_disk_allocated_size(resource_name, credentials):
    """
    Get allocated size of a persistent disk using Compute API.

    Args:
        resource_name (str): Full disk resource name
        credentials: Service account credentials

    Returns:
        int or None: Allocated size in bytes, or None if error
    """
    compute = discovery.build('compute', 'v1', credentials=credentials)
    parts = resource_name.split('/')

    try:
        project = parts[4]
        zone = parts[6]
        disk = parts[-1]
        disk_info = compute.disks().get(project=project, zone=zone, disk=disk).execute()
        size_gb = disk_info.get('sizeGb', 0)
        return int(size_gb) * 1_000_000_000  # Convert GB to bytes
    except Exception as e:
        print(f"Error fetching allocated size for {resource_name}: {e}")
        return None


def get_resource_cost_data(project_id, resource_name=None, service_name=None, days=30):
    """
    Get cost data for specific resources or services over the last N days.

    Args:
        project_id (str): GCP project ID
        resource_name (str, optional): Specific resource name to filter by
        service_name (str, optional): Service name (e.g., 'Compute Engine', 'Cloud Storage')
        days (int): Number of days to look back for cost data

    Returns:
        dict: Cost data with total cost and currency
    """
    try:
        # Calculate date range
        end_date = datetime.now(UTC)
        start_date = end_date - timedelta(days=30)  # Always use 30 days for cost estimation

        # Placeholder cost calculation based on resource type and size
        cost_data = {
            'total_cost_usd': 0.0,
            'currency': 'USD',
            'period_days': days,
            'cost_breakdown': {},
            'estimation_note': 'Cost estimated based on resource specifications and standard pricing'
        }

        # Estimate costs based on service type
        if service_name:
            if service_name == 'Compute Engine':
                # Basic VM cost estimation (simplified)
                cost_data['total_cost_usd'] = 2.50 * days  # ~$2.50/day for small instance
                cost_data['cost_breakdown'] = {
                    'compute_cost': cost_data['total_cost_usd'] * 0.8,
                    'storage_cost': cost_data['total_cost_usd'] * 0.2
                }
            elif service_name == 'Cloud Storage':
                # Basic storage cost estimation
                cost_data['total_cost_usd'] = 0.02 * days  # ~$0.02/day for small bucket
                cost_data['cost_breakdown'] = {
                    'storage_cost': cost_data['total_cost_usd'] * 0.9,
                    'operations_cost': cost_data['total_cost_usd'] * 0.1
                }
            elif service_name == 'Compute Engine Disk':
                # Basic disk cost estimation
                cost_data['total_cost_usd'] = 0.40 * days  # ~$0.40/day for 10GB disk
                cost_data['cost_breakdown'] = {
                    'storage_cost': cost_data['total_cost_usd']
                }

        return cost_data

    except Exception as e:
        print(f"Error fetching cost data: {e}")
        return {
            'total_cost_usd': 0.0,
            'currency': 'USD',
            'period_days': days,
            'error': str(e),
            'estimation_note': 'Cost data unavailable'
        }


def get_detailed_resource_costs(project_id, resource_type, resource_size=None, days=30):
    """
    Get detailed cost estimates for specific resource types.

    Args:
        project_id (str): GCP project ID
        resource_type (str): Type of resource (vm, disk, bucket, subnet, snapshot, cluster, persistent_volume)
        resource_size (dict, optional): Resource size info (e.g., {'size_gb': 10, 'machine_type': 'e2-micro'})
        days (int): Number of days for cost calculation

    Returns:
        dict: Detailed cost breakdown
    """
    try:
        cost_data = {
            'total_cost_usd': 0.0,
            'currency': 'USD',
            'period_days': days,
            'daily_cost_usd': 0.0,
            'cost_breakdown': {},
            'pricing_tier': 'standard'
        }

        if resource_type == 'vm':
            # VM cost estimation based on machine type and usage
            machine_type = resource_size.get('machine_type', 'e2-micro') if resource_size else 'e2-micro'

            # Example pricing for common machine types (per day, highly simplified)
            # In a real scenario, you'd use a more comprehensive pricing API or lookup table.
            if 'e2-micro' in machine_type:
                daily_cost = 0.012 * 24  # ~$0.012/hour
            elif 'e2-small' in machine_type:
                daily_cost = 0.024 * 24
            elif 'e2-medium' in machine_type:
                daily_cost = 0.048 * 24
            elif 'n1-standard-1' in machine_type:
                daily_cost = 0.033 * 24
            elif 'n2-standard-2' in machine_type:
                daily_cost = 0.066 * 24
            else:
                daily_cost = 0.03 * 24  # Default general VM cost per day

            cost_data['daily_cost_usd'] = daily_cost
            cost_data['total_cost_usd'] = daily_cost * days
            cost_data['cost_breakdown'] = {
                'compute_cost': cost_data['total_cost_usd'] * 0.75,
                'network_cost': cost_data['total_cost_usd'] * 0.15,
                'storage_cost': cost_data['total_cost_usd'] * 0.10
            }

        elif resource_type == 'disk':
            # Disk cost based on size and type
            size_gb = resource_size.get('size_gb', 10) if resource_size else 10
            disk_type = resource_size.get('disk_type', 'pd-standard') if resource_size else 'pd-standard'

            # Pricing per GB per month (simplified, regional prices)
            if 'ssd' in disk_type or 'pd-ssd' in disk_type:
                monthly_cost_per_gb = 0.17  # Persistent Disk SSD
            elif 'balanced' in disk_type:
                monthly_cost_per_gb = 0.10  # Persistent Disk Balanced
            else:  # standard (pd-standard)
                monthly_cost_per_gb = 0.04  # Persistent Disk Standard

            daily_cost = (size_gb * monthly_cost_per_gb * days) / 30
            cost_data['daily_cost_usd'] = daily_cost / days
            cost_data['total_cost_usd'] = daily_cost
            cost_data['cost_breakdown'] = {
                'storage_cost': cost_data['total_cost_usd']
            }

        elif resource_type == 'bucket':
            # Storage bucket cost based on size
            size_bytes = resource_size.get('size_bytes', 0) if resource_size else 0
            size_gb = size_bytes / 1_000_000_000 if size_bytes else 0

            # Standard storage pricing (~$0.020 per GB per month, assuming Standard storage class)
            monthly_cost_per_gb = 0.020
            daily_cost = (size_gb * monthly_cost_per_gb * days) / 30

            cost_data['daily_cost_usd'] = daily_cost / days if days > 0 else 0
            cost_data['total_cost_usd'] = daily_cost
            cost_data['cost_breakdown'] = {
                'storage_cost': cost_data['total_cost_usd'] * 0.85,
                'operations_cost': cost_data['total_cost_usd'] * 0.15
            }

        elif resource_type == 'subnet':
            # Subnet/network cost (minimal for most cases, primarily for static IPs or NAT gateways)
            # A subnet itself doesn't directly cost money unless IPs are reserved or used by services.
            # This is a very rough placeholder.
            cost_data['daily_cost_usd'] = 0.01  # Minimal network overhead cost per day
            cost_data['total_cost_usd'] = 0.01 * days
            cost_data['cost_breakdown'] = {
                'network_cost': cost_data['total_cost_usd']
            }

        elif resource_type == 'snapshot':
            size_gb = resource_size.get('size_gb', 0) if resource_size else 0
            # Assuming standard regional snapshot pricing: ~$0.05 per GB per month
            monthly_cost_per_gb = 0.05
            daily_cost = (size_gb * monthly_cost_per_gb * days) / 30

            cost_data['daily_cost_usd'] = daily_cost / days if days > 0 else 0
            cost_data['total_cost_usd'] = daily_cost
            cost_data['cost_breakdown'] = {
                'snapshot_storage_cost': cost_data['total_cost_usd']
            }
            cost_data['pricing_tier'] = 'standard_snapshot'

        elif resource_type == 'cluster':  # GKE Cluster cost estimation
            node_count = resource_size.get('node_count', 1) if resource_size else 1
            # GKE clusters have a control plane fee and node costs (Compute Engine VMs).
            # Control plane fee: ~$0.10 per cluster per hour (for Autopilot/Standard)
            # Node cost: depends on machine type, similar to regular VMs.
            # For simplicity, we'll use a general average node cost.

            gke_control_plane_daily_cost = 0.10 * 24  # ~$2.40 per day per cluster

            # Assume average node type is 'e2-medium' equivalent for cost
            avg_node_daily_cost = (0.048 * 24)  # Cost of an e2-medium VM per day

            total_node_compute_cost = node_count * avg_node_daily_cost

            daily_cost = gke_control_plane_daily_cost + total_node_compute_cost

            cost_data['daily_cost_usd'] = daily_cost
            cost_data['total_cost_usd'] = daily_cost * days
            cost_data['cost_breakdown'] = {
                'control_plane_cost': gke_control_plane_daily_cost * days,
                'node_compute_cost': total_node_compute_cost * days
            }
            cost_data['pricing_tier'] = 'gke_cluster_estimated'

        elif resource_type == 'persistent_volume':  # K8s Persistent Volume cost estimation
            size_gb = resource_size.get('size_gb', 10) if resource_size else 10
            pv_storage_class = resource_size.get('storage_class', 'standard')  # e.g., 'standard', 'ssd'

            # Pricing per GB per month for PVs (similar to Persistent Disks)
            if 'ssd' in pv_storage_class.lower():
                monthly_cost_per_gb = 0.17  # PV using SSD
            elif 'premium' in pv_storage_class.lower():  # For Hyperdisk or similar premium PVs
                monthly_cost_per_gb = 0.17
            else:  # default to standard
                monthly_cost_per_gb = 0.04  # PV using Standard Persistent Disk

            daily_cost = (size_gb * monthly_cost_per_gb * days) / 30

            cost_data['daily_cost_usd'] = daily_cost / days if days > 0 else 0
            cost_data['total_cost_usd'] = daily_cost
            cost_data['cost_breakdown'] = {
                'pv_storage_cost': cost_data['total_cost_usd']
            }
            cost_data['pricing_tier'] = 'k8s_pv_estimated'

        return cost_data

    except Exception as e:
        print(f"Error fetching cost data for {resource_type}: {e}")
        return {
            'total_cost_usd': 0.0,
            'currency': 'USD',
            'period_days': days,
            'daily_cost_usd': 0.0,
            'error': str(e),
            'cost_breakdown': {}
        }


# ================================================================================
# RESOURCE ANALYSIS FUNCTIONS
# ================================================================================

def categorize_gcp_resources(project_id, credentials, bucket_quota_gb, thresholds):
    """
    Analyze GCS buckets and identify those with low utilization.

   Args:
        project_id (str): GCP project ID
        credentials: Service account credentials
        bucket_quota_gb (int): Bucket quota in GB for utilization calculation
        thresholds (dict): A dictionary containing the thresholds from MongoDB.
    """

    bucket_threshold = thresholds.get('storage_utilization', 20.0)

    print(f"\n📦 Analyzing Storage Buckets (Quota: {bucket_quota_gb}GB)")
    print("=" * 60)

    client = asset_v1.AssetServiceClient(credentials=credentials)
    scope = f"projects/{project_id}"
    bucket_quota_bytes = bucket_quota_gb * 1_000_000_000

    storage = []
    low_util_storage = []

    try:
        response = client.search_all_resources(
            request={
                "scope": scope,
                "asset_types": ["storage.googleapis.com/Bucket"],
                "page_size": 500
            }
        )

        for resource in response:
            if resource.asset_type == 'storage.googleapis.com/Bucket':
                bucket_name = resource.name.split("/")[-1]
                total_bytes = get_bucket_size_gcs(bucket_name, credentials)

                is_orphaned_bucket = False
                if total_bytes is not None:
                    utilization = min((total_bytes / bucket_quota_bytes) * 100, 100.0)
                    if total_bytes == 0:  # Define orphaned bucket as having 0 bytes
                        is_orphaned_bucket = True
                else:
                    utilization = None

                storage.append((resource.asset_type, resource.name, utilization, total_bytes))

                if utilization is not None and utilization < bucket_threshold:
                    low_util_storage.append((resource.asset_type, resource.name, utilization, total_bytes))

        # Display results
        print(f"Total buckets found: {len(storage)}")
        for asset_type, name, util, size in storage:
            util_str = f"{util:.2f}%" if util is not None else "N/A"  # Adjusted precision for display

            # Smart size formatting (KB, MB, GB)
            if size is not None:
                if size < 1_000:
                    size_str = f"{size:.2f}B"
                elif size < 1_000_000:
                    size_str = f"{size / 1_000:.2f}KB"
                elif size < 1_000_000_000:
                    size_str = f"{size / 1_000_000:.2f}MB"
                else:
                    size_str = f"{size / 1_000_000_000:.2f}GB"
            else:
                size_str = "0B"

            print(f"  • {name.split('/')[-1]} - {util_str} utilization ({size_str})")

        if not storage:
            print("  • No buckets found")

        print(f"\n🔍 Low Utilization Buckets (<{bucket_threshold}%): {len(low_util_storage)}")
        if low_util_storage:
            for asset_type, name, util, size in low_util_storage:
                util_str = f"{util:.2f}%" if util is not None else "N/A"  # Adjusted precision for display

                # Smart size formatting (KB, MB, GB)
                if size is not None:
                    if size < 1_000:
                        size_str = f"{size:.2f}B"
                    elif size < 1_000_000:
                        size_str = f"{size / 1_000:.2f}KB"
                    elif size < 1_000_000_000:
                        size_str = f"{size / 1_000_000:.2f}MB"
                    else:
                        size_str = f"{size / 1_000_000_000:.2f}GB"
                else:
                    size_str = "0B"

                print(f"  ⚠️  {name.split('/')[-1]} - {util_str} utilization ({size_str})")
        else:
            print("  ✅ No low utilization buckets found")

    except Exception as e:
        print(f"❌ Error analyzing buckets: {e}")


def categorize_gcp_vm_cpu_utilization(project_id, credentials, thresholds):
    """
    Analyze VM instances and identify those with low CPU utilization.

    Args:
        project_id (str): GCP project ID
        credentials: Service account credentials
        thresholds (dict): A dictionary containing the thresholds from MongoDB.
    """
    threshold = thresholds.get('cmp_cpu_usage', 15.0)
    print(f"\n💻 Analyzing VM Instances (CPU Threshold: {threshold}%)")
    print("=" * 60)

    client = asset_v1.AssetServiceClient(credentials=credentials)
    scope = f"projects/{project_id}"
    low_cpu_vms = []
    total_vms = 0

    try:
        response = client.search_all_resources(
            request={
                "scope": scope,
                "asset_types": ["compute.googleapis.com/Instance"],
                "page_size": 500
            }
        )

        for resource in response:
            if resource.asset_type == 'compute.googleapis.com/Instance':
                total_vms += 1
                vm_id = resource.name.split("/")[-1]
                zone = None
                if 'zones/' in resource.name:
                    zone = resource.name.split("/zones/")[-1].split("/")[0]

                cpu_util = get_average_utilization(project_id, resource.asset_type, resource.name, credentials)

                print(
                    f"  • {vm_id} (Zone: {zone}) - {cpu_util:.2f}% CPU" if cpu_util is not None else f"  • {vm_id} (Zone: {zone}) - N/A CPU")  # Adjusted precision for display

                if cpu_util is not None and cpu_util < threshold:
                    low_cpu_vms.append({
                        'name': resource.name,
                        'vm_id': vm_id,
                        'zone': zone,
                        'cpu_util': cpu_util
                    })

        print(f"\nTotal VMs analyzed: {total_vms}")

        print(f"🔍 Low CPU Usage VMs (<{threshold}%): {len(low_cpu_vms)}")

        if low_cpu_vms:
            for vm in low_cpu_vms:
                print(
                    f"  ⚠️  {vm['vm_id']} - {vm['cpu_util']:.2f}% CPU (Zone: {vm['zone']})")  # Adjusted precision for display
        else:
            print("  ✅ No low CPU usage VMs found")

    except Exception as e:
        print(f"❌ Error analyzing VMs: {e}")


def list_subnets_with_cidr_and_ip_usage(project_id, thresholds, credentials):
    """
    Analyze subnets and identify those with high free IP addresses,
    using Network Analyzer insights for accurate IP utilization.

    Args:
        project_id (str): GCP project ID
        thresholds (dict): A dictionary containing the thresholds from MongoDB.
        credentials: Service account credentials
    """
    subnet_threshold = thresholds.get('subnet_free_ip_percentage', 90.0)
    print(f"\n🌐 Analyzing Subnets (Free IP Threshold: >{subnet_threshold}%)")
    print("=" * 60)

    recommender_client = discovery.build('recommender', 'v1', credentials=credentials)

    high_free_subnets = []
    total_subnets = 0

    try:
        request = compute.subnetworks().aggregatedList(project=project_id)
        while request is not None:
            response = request.execute()
            for region_url, region_data in response.get('items', {}).items():
                for subnet in region_data.get('subnetworks', []):
                    total_subnets += 1
                    name = subnet.get('name')
                    cidr = subnet.get('ipCidrRange')
                    network = subnet.get('network').split('/')[-1]
                    vpc_name = network
                    region = region_url.replace('regions/', '') if 'regions/' in region_url else region_url

                    # Skip default VPC subnets
                    if vpc_name == 'default':
                        print(f"  • {name} (Default VPC) - Skipped")
                        continue

                    total_ips = len(list(ipaddress.ip_network(cidr, strict=False).hosts())) if cidr else 0

                    # --- NEW: Get actual IP utilization from Network Analyzer Insight ---
                    allocation_ratio = 0.0  # Default to 0% allocation
                    try:
                        # Network Analyzer insights are global, but specific to resources in a region
                        insight_request = recommender_client.projects().locations().insightTypes().insights().list(
                            parent=f"projects/{project_id}/locations/global",
                            insightType="google.networkanalyzer.vpcnetwork.ipAddressInsight",
                            filter=f"targetResources=(//compute.googleapis.com/projects/{project_id}/regions/{region}/subnetworks/{name})"
                        )
                        insight_response = insight_request.execute()

                        # Find the insight for the primary IP range of this subnet
                        for insight in insight_response.get('insights', []):
                            if 'content' in insight and 'overview' in insight['content']:
                                for subnet_insight in insight['content']['overview'].get('ipAddressUtilizationSummary',
                                                                                         []):
                                    if subnet_insight.get('subnetRangePrefix') == cidr:
                                        allocation_ratio = subnet_insight.get('allocationRatio', 0.0)
                                        break
                            if allocation_ratio > 0:  # Found the relevant insight
                                break

                    except Exception as e:
                        print(f"    ⚠️ Error fetching Network Analyzer insight for subnet {name}: {e}")
                        # Fallback: if insight fails, assume 0 used IPs (less accurate but prevents script failure)
                        allocation_ratio = 0.0

                    used_ips_count = int(total_ips * allocation_ratio)
                    free_ips = total_ips - used_ips_count
                    free_pct = (free_ips / total_ips * 100) if total_ips > 0 else 0

                    print(
                        f"  • {name} (VPC: {vpc_name}, Region: {region}) - {free_pct:.2f}% free IPs ({free_ips}/{total_ips} total, {used_ips_count} used)")

                    if free_pct > subnet_threshold:
                        high_free_subnets.append({
                            'name': name,
                            'vpc_name': vpc_name,
                            'cidr': cidr,
                            'region': region,
                            'total_ips': total_ips,
                            'used_ips_count': used_ips_count,
                            'free_ips': free_ips,
                            'free_pct': free_pct,
                            'is_orphaned': (allocation_ratio == 0.0 and total_ips > 0)
                            # Orphaned if 0% allocation and not a /0 subnet
                        })

            request = compute.subnetworks().aggregatedList_next(previous_request=request, previous_response=response)

        print(f"\nTotal subnets analyzed: {total_subnets}")
        print(f"🔍 High Free IP Subnets (>{subnet_threshold}%): {len(high_free_subnets)}")

        if high_free_subnets:
            for subnet_info in high_free_subnets:
                orphaned_status = " (Orphaned)" if subnet_info['is_orphaned'] else ""
                print(
                    f"  ⚠️  {subnet_info['name']} (VPC: {subnet_info['vpc_name']}) - {subnet_info['free_pct']:.2f}% free ({subnet_info['free_ips']}/{subnet_info['total_ips']} IPs){orphaned_status}")
        else:
            print("  ✅ No high free IP subnets found")

    except Exception as e:
        print(f"❌ Error analyzing subnets: {e}")


def categorize_gcp_disk_utilization(project_id, credentials, thresholds):
    """
    Analyze persistent disks and identify those with low utilization based on allocated size.
    Since actual disk usage metrics aren't available via basic Compute API, we identify
    potentially underutilized disks based on small size or specific criteria.
    Uses aggregated list API for maximum speed across all zones.

   Args:
        project_id (str): GCP project ID
        credentials: Service account credentials
        thresholds (dict): A dictionary containing the thresholds from MongoDB.
    """
    disk_quota_gb = thresholds.get('disk_underutilized_gb', 100)

    print(f"\n💿 Analyzing Persistent Disks (Small Disk Threshold: <{disk_quota_gb}GB)")
    print("=" * 60)
    print("📋 Identifying potentially underutilized disks based on size and status")
    print("⚡ Using aggregated list API for maximum speed")

    compute = discovery.build('compute', 'v1', credentials=credentials)

    disks = []
    small_disks = []  # Disks smaller than threshold

    try:
        # Use aggregated list to get all disks across all zones in one API call (much faster)
        print("  • Fetching all disks using aggregated list (optimized for speed)...")

        req = compute.disks().aggregatedList(project=project_id)
        total_disk_count = 0

        while req is not None:
            resp = req.execute()

            for zone_url, zone_data in resp.get('items', {}).items():
                if 'disks' in zone_data:
                    zone_name = zone_url.replace('zones/', '') if 'zones/' in zone_url else zone_url
                    zone_disks = zone_data['disks']
                    zone_disk_count = len(zone_disks)
                    total_disk_count += zone_disk_count

                    if zone_disk_count > 0:
                        print(f"    📍 {zone_name}: {zone_disk_count} disks")

                    for disk in zone_disks:
                        disk_name = disk.get('name')
                        size_gb = int(disk.get('sizeGb', 0))
                        disk_type = disk.get('type', '').split('/')[-1] if disk.get('type') else 'unknown'
                        status = disk.get('status', 'unknown')

                        # Check if disk is attached to any instances
                        users = disk.get('users', [])
                        is_attached = len(users) > 0

                        disk_info = {
                            'name': disk_name,
                            'zone': zone_name,
                            'size_gb': size_gb,
                            'disk_type': disk_type,
                            'status': status,
                            'is_attached': is_attached,
                            'attached_to': [user.split('/')[-1] for user in users] if users else [],
                            'labels': disk.get('labels', {})
                        }

                        disks.append(disk_info)

                        # Consider disks as potentially underutilized if they are:
                        # 1. Small (< threshold GB) OR
                        # 2. Not attached to any instance OR
                        # 3. Status is not READY
                        if size_gb < disk_quota_gb or not is_attached or status != 'READY':
                            small_disks.append(disk_info)

            req = compute.disks().aggregatedList_next(previous_request=req, previous_response=resp)

        print(f"  • Total disks found: {total_disk_count}")

        # Display results
        print(f"\nTotal disks found: {len(disks)}")
        for disk in disks:
            attachment_status = "Attached" if disk['is_attached'] else "Unattached"
            attached_to = f" to {', '.join(disk['attached_to'])}" if disk['attached_to'] else ""
            print(
                f"  • {disk['name']} (Zone: {disk['zone']}) - {disk['size_gb']}GB, {disk['disk_type']}, {attachment_status}{attached_to}")

        if not disks:
            print("  • No disks found")

        print(f"\n🔍 Potentially Underutilized Disks: {len(small_disks)}")
        print("    (Small size, unattached, or not ready)")
        if small_disks:
            for disk in small_disks:
                reasons = []
                if disk['size_gb'] < disk_quota_gb:
                    reasons.append(f"small ({disk['size_gb']}GB)")
                if not disk['is_attached']:
                    reasons.append("unattached")
                if disk['status'] != 'READY':
                    reasons.append(f"status: {disk['status']}")

                reason_str = ", ".join(reasons)
                attachment_info = f" → {', '.join(disk['attached_to'])}" if disk['attached_to'] else ""
                print(f"  ⚠️  {disk['name']} (Zone: {disk['zone']}) - {reason_str}{attachment_info}")
        else:
            print("  ✅ No potentially underutilized disks found")

    except Exception as e:
        print(f"❌ Error analyzing disks: {e}")


def categorize_gcp_snapshots(project_id, credentials, thresholds):
    """
    Analyze GCP disk snapshots and identify potentially orphaned or old ones.

    Args:
        project_id (str): GCP project ID
        credentials: Service account credentials
        thresholds (dict): A dictionary containing the thresholds from MongoDB.
    """
    snapshot_age_threshold_days = thresholds.get('snapshot_age_threshold_days', 90)

    print(f"\n📸 Analyzing Disk Snapshots (Old Snapshot Threshold: >{snapshot_age_threshold_days} days)")
    print("=" * 60)

    compute_client = discovery.build('compute', 'v1', credentials=credentials)
    orphaned_snapshots = []
    total_snapshots = 0

    try:
        request = compute_client.snapshots().list(project=project_id)

        while request is not None:
            response = request.execute()
            for snapshot in response.get('items', []):
                total_snapshots += 1
                snapshot_name = snapshot.get('name')
                source_disk_url = snapshot.get('sourceDisk', 'N/A')
                creation_timestamp_str = snapshot.get('creationTimestamp')
                snapshot_size_gb = int(snapshot.get('diskSizeGb', 0))
                storage_bytes = int(snapshot.get('storageBytes', 0))  # Actual storage consumed

                is_orphaned_snapshot = False
                reasons = []

                # Check if source disk exists (more complex, requires additional API call)
                # For this script, we'll consider it orphaned if sourceDisk is missing or if
                # a basic check for source disk existence fails (e.g., it's a deleted disk).
                # A more robust check would involve querying the disks API for existence.
                if source_disk_url == 'N/A' or not source_disk_url:
                    is_orphaned_snapshot = True
                    reasons.append("no_source_disk_info")
                else:
                    # Attempt to extract disk info from URL and check if it still exists
                    try:
                        # Example sourceDisk URL: https://www.googleapis.com/compute/v1/projects/PROJECT/zones/ZONE/disks/DISK_NAME
                        parts = source_disk_url.split('/')
                        if len(parts) >= 9:  # Check for expected URL structure
                            source_project_from_url = parts[6]
                            source_zone_from_url = parts[8]
                            source_disk_name_from_url = parts[10]

                            # Try to get the disk. If it's not found (404), it's likely deleted.
                            try:
                                compute_client.disks().get(
                                    project=source_project_from_url,
                                    zone=source_zone_from_url,
                                    disk=source_disk_name_from_url
                                ).execute()
                            except HttpError as e:
                                if e.resp.status == 404:
                                    is_orphaned_snapshot = True
                                    reasons.append("source_disk_deleted")
                                else:
                                    # Other HTTP errors (e.g., permission, rate limit)
                                    print(f"    ⚠️ API error checking source disk {source_disk_url}: {e}")
                            except Exception as e:  # Catch any other unexpected errors
                                print(f"    ⚠️ Unexpected error checking source disk {source_disk_url}: {e}")
                        else:
                            is_orphaned_snapshot = True
                            reasons.append("invalid_source_disk_url")
                    except Exception as e:
                        print(f"    ⚠️ Error parsing source disk URL {source_disk_url}: {e}")
                        is_orphaned_snapshot = True
                        reasons.append("source_disk_parse_error")

                # Check snapshot age
                if creation_timestamp_str:
                    try:
                        creation_time = datetime.fromisoformat(creation_timestamp_str.replace('Z', '+00:00'))
                        age_days = (datetime.now(UTC) - creation_time).days
                        if age_days > snapshot_age_threshold_days:
                            is_orphaned_snapshot = True
                            reasons.append(f"older_than_{snapshot_age_threshold_days}_days")
                    except ValueError:
                        reasons.append("invalid_creation_timestamp_format")
                else:
                    reasons.append("no_creation_timestamp")

                if is_orphaned_snapshot:
                    orphaned_snapshots.append({
                        'name': snapshot_name,
                        'source_disk': source_disk_url,
                        'creation_timestamp': creation_timestamp_str,
                        'size_gb': snapshot_size_gb,
                        'storage_bytes': storage_bytes,
                        'is_orphaned': True,
                        'orphaned_reasons': reasons,
                        'labels': snapshot.get('labels', {})
                    })
            request = compute_client.snapshots().list_next(previous_request=request, previous_response=response)

        print(f"\nTotal snapshots found: {total_snapshots}")
        print(f"🔍 Potentially Orphaned/Old Snapshots: {len(orphaned_snapshots)}")
        if orphaned_snapshots:
            for snap in orphaned_snapshots:
                reasons_str = ", ".join(snap['orphaned_reasons'])
                print(
                    f"  ⚠️  {snap['name']} (Size: {snap['size_gb']}GB, Source: {snap['source_disk']}) - Reasons: {reasons_str}")
        else:
            print("  ✅ No potentially orphaned/old snapshots found")

    except Exception as e:
        print(f"❌ Error analyzing snapshots: {e}")

    return orphaned_snapshots


# ================================================================================
# NEW UTILITY FUNCTIONS FOR KUBERNETES
# ================================================================================

def get_gke_cluster_metrics(project_id, location, cluster_name, credentials):
    """
    Fetches basic metrics for a GKE cluster (e.g., node count, estimated usage).
    Note: Real-world GKE utilization requires Stackdriver Monitoring for GKE.
    This is a simplified representation.

    Args:
        project_id (str): GCP project ID.
        location (str): GKE cluster location (e.g., 'us-central1', 'us-central1-a').
        cluster_name (str): Name of the GKE cluster.
        credentials: Service account credentials.

    Returns:
        dict: Dictionary with estimated metrics or None if not available.
    """
    # For actual CPU/memory utilization, you'd query Stackdriver Monitoring for GKE metrics:
    # metric.type="kubernetes.io/container/cpu/core_usage_time"
    # metric.type="kubernetes.io/container/memory/bytes_usage"
    # resource.type="k8s_container"
    # resource.labels.cluster_name="{cluster_name}"
    # resource.labels.location="{location}"

    try:
        container_client = discovery.build('container', 'v1', credentials=credentials)
        cluster_info = container_client.projects().locations().clusters().get(
            projectId=project_id,
            location=location,
            clusterId=cluster_name
        ).execute()

        node_count = cluster_info.get('currentNodeCount', 0)
        # Simplified assumption for usage based on node count
        # In a real scenario, these values would come from actual monitoring data.
        estimated_cpu_usage_percent = 0.0
        estimated_memory_usage_percent = 0.0

        if node_count > 0:
            # Simulate some usage if nodes exist, for demonstration purposes
            # These values would ideally come from actual monitoring data for optimization.
            estimated_cpu_usage_percent = 10.0  # Example low usage
            estimated_memory_usage_percent = 15.0  # Example low usage

        return {
            'node_count': node_count,
            'estimated_cpu_usage_percent': estimated_cpu_usage_percent,
            'estimated_memory_usage_percent': estimated_memory_usage_percent,
            'status': cluster_info.get('status', 'UNKNOWN')
        }
    except Exception as e:
        print(f"    ⚠️ Error fetching GKE cluster metrics for {cluster_name}: {e}")
        return None


def get_k8s_pv_utilization(project_id, pv_name, credentials):
    """
    Fetches utilization for a Kubernetes Persistent Volume.
    Note: Direct PV utilization is complex and usually requires Kubernetes API access
    or Stackdriver monitoring for GKE. This is a simplified placeholder.

    Args:
        project_id (str): GCP project ID.
        pv_name (str): Name of the Persistent Volume.
        credentials: Service account credentials.

    Returns:
        dict: Dictionary with estimated utilization and claimed status.
    """
    # Real PV utilization would involve:
    # 1. Getting PV details (capacity, claims) via Kubernetes API (if running inside GKE)
    # 2. Querying Stackdriver metrics for 'kubernetes.io/volume/bytes_used'
    # For this script, we'll simulate.

    # Simulate low utilization for demonstration
    estimated_utilization = 10.0  # Example low utilization for a PV

    # Simulate claimed status. In real world, you'd check PV.spec.claimRef
    # For asset inventory, you might infer from labels or related resources.
    is_claimed = False  # Assume not claimed for demonstration unless specific logic added

    # A common way to get PV details is via Asset Inventory, but it doesn't always
    # provide 'claimed' status directly. For full fidelity, you'd need the K8s API.
    # For now, we'll return a fixed value.
    return {
        'estimated_utilization': estimated_utilization,
        'is_claimed': is_claimed,
        'size_gb': 100  # Placeholder size, ideally from PV spec
    }


# ================================================================================
# NEW KUBERNETES RESOURCE ANALYSIS FUNCTIONS
# ================================================================================

def categorize_gcp_kubernetes_clusters(project_id, credentials, thresholds):
    """
    Analyzes GKE clusters for underutilization or orphaned status.

    Args:
        project_id (str): GCP project ID.
        credentials: Service account credentials.
        thresholds (dict): A dictionary containing the thresholds from MongoDB.
    """
    low_node_threshold = thresholds.get('gke_low_node_threshold', 1)
    low_cpu_util_threshold = thresholds.get('gke_low_cpu_util_threshold', 5.0)
    low_mem_util_threshold = thresholds.get('gke_low_mem_util_threshold', 10.0)

    print(f"\n☸️ Analyzing GKE Clusters (Low Node Threshold: <{low_node_threshold} node(s))")
    print("=" * 60)

    container_client = discovery.build('container', 'v1', credentials=credentials)
    underutilized_clusters = []
    total_clusters = 0

    try:
        # GKE clusters are regional or zonal, list by location
        # Need to list all locations first
        list_locations_req = container_client.projects().locations().list(name=f"projects/{project_id}")
        locations_resp = list_locations_req.execute()

        for location_data in locations_resp.get('locations', []):
            location_name = location_data['name'].split('/')[-1]  # e.g., us-central1 or us-central1-a

            try:
                list_clusters_req = container_client.projects().locations().clusters().list(
                    projectId=project_id,
                    location=location_name
                )
                clusters_resp = list_clusters_req.execute()

                for cluster in clusters_resp.get('clusters', []):
                    total_clusters += 1
                    cluster_name = cluster.get('name')
                    node_count = cluster.get('currentNodeCount', 0)
                    cluster_status = cluster.get('status', 'UNKNOWN')
                    cluster_location = cluster.get('location',
                                                   location_name)  # Use cluster's actual location if available

                    # Get estimated metrics (simulated, as real data requires more complex setup)
                    metrics = get_gke_cluster_metrics(project_id, cluster_location, cluster_name, credentials)

                    is_orphaned_cluster = False
                    reasons = []

                    if cluster_status in ['STOPPING', 'DELETING']:
                        is_orphaned_cluster = True
                        reasons.append(f"status:{cluster_status}")
                    elif node_count == 0 and cluster_status == 'RUNNING':
                        is_orphaned_cluster = True
                        reasons.append("zero_nodes")
                    elif node_count < low_node_threshold and cluster_status == 'RUNNING':
                        is_orphaned_cluster = True
                        reasons.append("very_low_node_count")

                    # Check simulated utilization thresholds if metrics are available
                    if metrics:
                        if metrics['estimated_cpu_usage_percent'] < low_cpu_util_threshold:
                            is_orphaned_cluster = True
                            reasons.append("low_cpu_utilization")
                        if metrics['estimated_memory_usage_percent'] < low_mem_util_threshold:
                            is_orphaned_cluster = True
                            reasons.append("low_memory_utilization")

                    print(
                        f"  • {cluster_name} (Location: {cluster_location}, Nodes: {node_count}, Status: {cluster_status})")

                    if is_orphaned_cluster:
                        underutilized_clusters.append({
                            'name': cluster_name,
                            'location': cluster_location,
                            'node_count': node_count,
                            'status': cluster_status,
                            'is_orphaned': True,
                            'orphaned_reasons': list(set(reasons)),  # Use set to remove duplicate reasons
                            'labels': cluster.get('resourceLabels', {})
                            # GKE uses resourceLabels for user-defined labels
                        })
            except HttpError as e:
                # Handle 403 Forbidden for locations where service account doesn't have access
                if e.resp.status == 403:
                    print(f"    ⚠️ Permission denied to list clusters in location {location_name}. Skipping.")
                else:
                    print(f"    ❌ Error listing clusters in location {location_name}: {e}")
            except Exception as e:
                print(f"    ❌ Unexpected error listing clusters in location {location_name}: {e}")

        print(f"\nTotal GKE clusters analyzed: {total_clusters}")
        print(f"🔍 Potentially Underutilized/Orphaned GKE Clusters: {len(underutilized_clusters)}")

        if underutilized_clusters:
            for cluster_info in underutilized_clusters:
                reasons_str = ", ".join(cluster_info['orphaned_reasons'])
                orphaned_status = " (Orphaned)" if cluster_info['is_orphaned'] else ""
                print(
                    f"  ⚠️  {cluster_info['name']} (Nodes: {cluster_info['node_count']}, Status: {cluster_info['status']}){orphaned_status} - Reasons: {reasons_str}")
        else:
            print("  ✅ No potentially underutilized/orphaned GKE clusters found")

    except Exception as e:
        print(f"❌ Error analyzing GKE clusters: {e}")

    return underutilized_clusters


def categorize_gcp_kubernetes_persistent_volumes(project_id, credentials, thresholds):
    """
    Analyzes Kubernetes Persistent Volumes for orphaned status.
    This requires querying Kubernetes API server, which is complex directly from a script
    without a K8s client. We'll simulate by searching for PVs via Cloud Asset Inventory
    and applying a simplified orphaned logic.

    Args:
        project_id (str): GCP project ID.
        credentials: Service account credentials.
        thresholds (dict): A dictionary containing the thresholds from MongoDB.
    """
    pv_low_utilization_threshold = thresholds.get('pv_low_utilization_threshold', 1.0)

    print(f"\n💾 Analyzing Kubernetes Persistent Volumes")
    print("=" * 60)
    print("📋 Identifying potentially orphaned PVs (unclaimed or low utilization)")

    asset_client = asset_v1.AssetServiceClient(credentials=credentials)
    scope = f"projects/{project_id}"
    orphaned_pvs = []
    total_pvs = 0

    try:
        # Search for PersistentVolume assets
        response = asset_client.search_all_resources(
            request={
                "scope": scope,
                "asset_types": ["k8s.io/PersistentVolume"],
                "page_size": 500
            }
        )

        for resource in response:
            if resource.asset_type == 'k8s.io/PersistentVolume':
                total_pvs += 1
                pv_name = resource.name.split("/")[-1]

                # Get estimated utilization and claimed status (simulated)
                metrics = get_k8s_pv_utilization(project_id, pv_name, credentials)
                estimated_utilization = metrics.get('estimated_utilization', 0.0)
                is_claimed = metrics.get('is_claimed', False)  # Use simulated claimed status
                pv_size_gb = metrics.get('size_gb', 100)  # Use simulated size

                pv_storage_class = resource.labels.get('storage_class', 'standard') if resource.labels else 'standard'

                is_orphaned_pv = False
                reasons = []

                if not is_claimed:  # If PV is not claimed by any PVC
                    is_orphaned_pv = True
                    reasons.append("unclaimed")

                if estimated_utilization < pv_low_utilization_threshold:  # Very low utilization
                    is_orphaned_pv = True
                    reasons.append("very_low_utilization")

                print(
                    f"  • {pv_name} (Claimed: {is_claimed}, Size: {pv_size_gb}GB, Util: {estimated_utilization:.2f}%)")

                if is_orphaned_pv:
                    orphaned_pvs.append({
                        'name': pv_name,
                        'full_name': resource.name,
                        'is_claimed': is_claimed,
                        'estimated_utilization': estimated_utilization,
                        'size_gb': pv_size_gb,
                        'storage_class': pv_storage_class,
                        'is_orphaned': True,
                        'orphaned_reasons': list(set(reasons)),  # Use set to remove duplicate reasons
                        'labels': resource.labels  # K8s resources use 'labels' directly
                    })

        print(f"\nTotal PVs analyzed: {total_pvs}")
        print(f"🔍 Potentially Orphaned PVs: {len(orphaned_pvs)}")

        if orphaned_pvs:
            for pv_info in orphaned_pvs:
                reasons_str = ", ".join(pv_info['orphaned_reasons'])
                print(
                    f"  ⚠️  {pv_info['name']} (Claimed: {pv_info['is_claimed']}, Size: {pv_info['size_gb']}GB, Utilization: {pv_info['estimated_utilization']:.2f}%) - Reasons: {reasons_str}")
        else:
            print("  ✅ No potentially orphaned PVs found")

    except Exception as e:
        print(f"❌ Error analyzing K8s Persistent Volumes: {e}")

    return orphaned_pvs


# ================================================================================
# JSON REPORT GENERATION
# ================================================================================

def collect_optimization_candidates(project_id, credentials, thresholds):
    """
    Collect detailed information about resources that meet optimization criteria.

    Returns:
        dict: Dictionary containing optimization candidates with IDs, names, and labels
    """

    bucket_threshold = thresholds.get('storage_utilization', 20.0)
    vm_cpu_threshold = thresholds.get('cmp_cpu_usage', 15.0)
    subnet_threshold = thresholds.get('subnet_free_ip_percentage', 90.0)
    disk_quota_gb = thresholds.get('disk_underutilized_gb', 100)
    snapshot_age_threshold_days = thresholds.get('snapshot_age_threshold_days', 90)

    print(f"\n📊 Collecting Optimization Candidates for JSON Report...")
    print("=" * 60)

    optimization_candidates = {
        "low_utilization_buckets": [],
        "low_cpu_vms": [],
        "high_free_subnets": [],
        "low_utilization_disks": [],
        "orphaned_snapshots": [],
        "underutilized_clusters": [],  # NEW: Add GKE clusters category
        "orphaned_pvs": []  # NEW: Add K8s Persistent Volumes category
    }

    asset_client = asset_v1.AssetServiceClient(credentials=credentials)
    scope = f"projects/{project_id}"
    bucket_quota_gb_for_calc = 100  # Default for calculation if not from thresholds
    bucket_quota_bytes = bucket_quota_gb_for_calc * 1_000_000_000

    # Collect low utilization buckets (<20% of quota)
    try:
        print("  • Collecting low utilization buckets...")
        response = asset_client.search_all_resources(
            request={
                "scope": scope,
                "asset_types": ["storage.googleapis.com/Bucket"],
                "page_size": 500
            }
        )

        for resource in response:
            if resource.asset_type == 'storage.googleapis.com/Bucket':
                bucket_name = resource.name.split("/")[-1]
                total_bytes = get_bucket_size_gcs(bucket_name, credentials)

                is_orphaned_bucket = False
                if total_bytes is not None:
                    utilization = min((total_bytes / bucket_quota_bytes) * 100, 100.0)
                    if total_bytes == 0:  # Define orphaned bucket as having 0 bytes
                        is_orphaned_bucket = True
                else:
                    utilization = None

                if utilization is not None and utilization < bucket_threshold:
                    cost_data = get_detailed_resource_costs(
                        PROJECT_ID,
                        'bucket',
                        {'size_bytes': total_bytes}
                    )

                    if total_bytes < 1_000:
                        size_formatted = f"{total_bytes:.2f}B"
                    elif total_bytes < 1_000_000:
                        size_formatted = f"{total_bytes / 1_000:.2f}KB"
                    elif total_bytes < 1_000_000_000:
                        size_formatted = f"{total_bytes / 1_000_000:.2f}MB"
                    else:
                        size_formatted = f"{total_bytes / 1_000_000_000:.2f}GB"

                    labels = dict(resource.labels) if hasattr(resource, 'labels') and resource.labels else {}
                    utilization_data = {
                        'utilization_percent': utilization,
                        'size_formatted': size_formatted
                    }
                    metadata = extract_resource_metadata(
                        labels=labels,
                        resource_name=bucket_name,
                        resource_type='bucket',
                        full_name=resource.name,
                        status="Available",
                        cost_analysis=cost_data,
                        utilization_data=utilization_data,
                        is_orphaned=is_orphaned_bucket  # Pass orphaned status
                    )

                    optimization_candidates["low_utilization_buckets"].append({
                        "name": bucket_name,
                        "full_name": resource.name,
                        "utilization_percent": round(utilization, 8),
                        "size_bytes": total_bytes,
                        "size_formatted": size_formatted,
                        "size_gb": round(total_bytes / 1_000_000_000, 8),
                        "cost_analysis": cost_data,
                        "recommendation": "Try Merging",
                        "labels": labels,
                        "resource_metadata": metadata
                    })
    except Exception as e:
        print(f"    ❌ Error collecting bucket data: {e}")

    # Collect low CPU VMs (<15% utilization)
    try:
        print("  • Collecting low CPU usage VMs...")
        response = asset_client.search_all_resources(
            request={
                "scope": scope,
                "asset_types": ["compute.googleapis.com/Instance"],
                "page_size": 500
            }
        )

        for resource in response:
            if resource.asset_type == 'compute.googleapis.com/Instance':
                cpu_util = get_average_utilization(PROJECT_ID, resource.asset_type, resource.name, credentials)
                if cpu_util is not None and cpu_util < vm_cpu_threshold:
                    vm_id = resource.name.split("/")[-1]
                    zone = None
                    if 'zones/' in resource.name:
                        zone = resource.name.split("/zones/")[-1].split("/")[0]

                    machine_type = 'e2-micro'
                    if zone and vm_id:
                        try:
                            instance_details = compute.instances().get(project=PROJECT_ID, zone=zone,
                                                                       instance=vm_id).execute()
                            machine_type_full_url = instance_details.get('machineType', '')
                            if machine_type_full_url:
                                machine_type = machine_type_full_url.split('/')[-1]
                        except Exception as e:
                            print(f"    ⚠️ Could not fetch details for VM {vm_id}: {e}")

                    cost_data = get_detailed_resource_costs(
                        PROJECT_ID,
                        'vm',
                        {'machine_type': machine_type}
                    )

                    labels = dict(resource.labels) if hasattr(resource, 'labels') and resource.labels else {}
                    utilization_data = {
                        'cpu_utilization': cpu_util
                    }
                    metadata = extract_resource_metadata(
                        labels=labels,
                        resource_name=vm_id,
                        resource_type='vm',
                        zone=zone,
                        full_name=resource.name,
                        status="Running",
                        cost_analysis=cost_data,
                        utilization_data=utilization_data,
                        is_orphaned=False  # VMs are not "orphaned" in this context
                    )

                    metadata['InstanceType'] = machine_type
                    optimization_candidates["low_cpu_vms"].append({
                        "name": vm_id,
                        "full_name": resource.name,
                        "zone": zone,
                        "cpu_utilization_percent": round(cpu_util, 8),
                        "cost_analysis": cost_data,
                        "recommendation": "Scale Down",
                        "labels": labels,
                        "resource_metadata": metadata
                    })
    except Exception as e:
        print(f"    ❌ Error collecting VM data: {e}")

    # Collect high free IP subnets (>90% free IPs)
    try:
        print("  • Collecting high free IP subnets...")
        recommender_client = discovery.build('recommender', 'v1', credentials=credentials)
        request = compute.subnetworks().aggregatedList(project=PROJECT_ID)
        while request is not None:
            response = request.execute()
            for region_url, region_data in response.get('items', {}).items():
                for subnet in region_data.get('subnetworks', []):
                    name = subnet.get('name')
                    cidr = subnet.get('ipCidrRange')
                    network = subnet.get('network').split('/')[-1]
                    vpc_name = network
                    region = region_url.replace('regions/', '') if 'regions/' in region_url else region_url

                    if vpc_name == 'default':
                        continue

                    total_ips = len(list(ipaddress.ip_network(cidr, strict=False).hosts())) if cidr else 0

                    # Get actual IP utilization from Network Analyzer Insight
                    allocation_ratio = 0.0
                    try:
                        insight_request = recommender_client.projects().locations().insightTypes().insights().list(
                            parent=f"projects/{PROJECT_ID}/locations/global",
                            insightType="google.networkanalyzer.vpcnetwork.ipAddressInsight",
                            filter=f"targetResources=(//compute.googleapis.com/projects/{PROJECT_ID}/regions/{region}/subnetworks/{name})"
                        )
                        insight_response = insight_request.execute()
                        for insight in insight_response.get('insights', []):
                            if 'content' in insight and 'overview' in insight['content']:
                                for subnet_insight in insight['content']['overview'].get('ipAddressUtilizationSummary',
                                                                                         []):
                                    if subnet_insight.get('subnetRangePrefix') == cidr:
                                        allocation_ratio = subnet_insight.get('allocationRatio', 0.0)
                                        break
                            if allocation_ratio > 0:
                                break
                    except Exception as e:
                        print(f"    ⚠️ Error fetching Network Analyzer insight for subnet {name}: {e}")
                        allocation_ratio = 0.0

                    used_ips_count = int(total_ips * allocation_ratio)
                    free_ips = total_ips - used_ips_count
                    free_pct = (free_ips / total_ips * 100) if total_ips > 0 else 0

                    is_orphaned_subnet = (
                            allocation_ratio == 0.0 and total_ips > 0)  # Orphaned if 0% allocation and not a /0 subnet

                    if free_pct > subnet_threshold:
                        cost_data = get_detailed_resource_costs(
                            PROJECT_ID,
                            'subnet',
                            {'total_ips': total_ips}
                        )

                        labels = subnet.get('labels', {})
                        utilization_data = {
                            'free_percent': free_pct,
                            'used_ips_count': used_ips_count,
                            'total_ips': total_ips,
                            'allocation_ratio': allocation_ratio
                        }
                        metadata = extract_resource_metadata(
                            labels=labels,
                            resource_name=name,
                            resource_type='subnet',
                            region=region,
                            full_name=subnet.get('selfLink'),
                            status="Available",
                            cost_analysis=cost_data,
                            utilization_data=utilization_data,
                            is_orphaned=is_orphaned_subnet  # Pass orphaned status
                        )

                        optimization_candidates["high_free_subnets"].append({
                            "name": name,
                            "vpc_name": vpc_name,
                            "cidr": cidr,
                            "region": region,
                            "total_ips": total_ips,
                            "free_ips": free_ips,
                            "free_percent": round(free_pct, 8),
                            "cost_analysis": cost_data,
                            "recommendation": "Scale Down",
                            "self_link": subnet.get('selfLink'),
                            "labels": subnet.get('labels', {}),
                            "resource_metadata": metadata
                        })
            request = compute.subnetworks().aggregatedList_next(previous_request=request, previous_response=response)
    except Exception as e:
        print(f"    ❌ Error collecting subnet data: {e}")

    # Collect potentially underutilized disks (small, unattached, or not ready)
    try:
        print("  • Collecting potentially underutilized disks...")

        req = compute.disks().aggregatedList(project=PROJECT_ID)

        while req is not None:
            resp = req.execute()

            for zone_url, zone_data in resp.get('items', {}).items():
                if 'disks' in zone_data:
                    zone_name = zone_url.replace('zones/', '') if 'zones/' in zone_url else zone_url
                    zone_disks = zone_data['disks']

                    for disk in zone_disks:
                        disk_name = disk.get('name')
                        size_gb = int(disk.get('sizeGb', 0))
                        disk_type = disk.get('type', '').split('/')[-1] if disk.get('type') else 'unknown'
                        status = disk.get('status', 'unknown')

                        users = disk.get('users', [])
                        is_attached = len(users) > 0

                        is_orphaned_disk = not is_attached  # Orphaned if not attached

                        # Consider disks as potentially underutilized if they are:
                        # 1. Small (< threshold GB) OR
                        # 2. Not attached to any instance OR
                        # 3. Status is not READY
                        if size_gb < disk_quota_gb or is_orphaned_disk or status != 'READY':
                            reasons = []
                            if size_gb < disk_quota_gb:
                                reasons.append(f"small_size")
                            if is_orphaned_disk:
                                reasons.append("unattached")
                            if status != 'READY':
                                reasons.append(f"status: {status}")  # Use f-string to show actual status

                            cost_data = get_detailed_resource_costs(
                                PROJECT_ID,
                                'disk',
                                {'size_gb': size_gb, 'disk_type': disk_type}
                            )

                            labels = disk.get('labels', {})
                            full_name = f"//compute.googleapis.com/projects/{PROJECT_ID}/zones/{zone_name}/disks/{disk_name}"
                            utilization_data = {
                                'optimization_reasons': reasons,
                                'size_gb': size_gb
                            }
                            metadata = extract_resource_metadata(
                                labels=labels,
                                resource_name=disk_name,
                                resource_type='disk',
                                zone=zone_name,
                                full_name=full_name,
                                status=status,
                                cost_analysis=cost_data,
                                utilization_data=utilization_data,
                                is_orphaned=is_orphaned_disk  # Pass orphaned status
                            )

                            optimization_candidates["low_utilization_disks"].append({
                                "name": disk_name,
                                "zone": zone_name,
                                "size_gb": size_gb,
                                "disk_type": disk_type,
                                "status": status,
                                "is_attached": is_attached,
                                "attached_to": [user.split('/')[-1] for user in users] if users else [],
                                "optimization_reasons": reasons,
                                "cost_analysis": cost_data,
                                "recommendation": "Scale Down",
                                "labels": disk.get('labels', {}),
                                "resource_metadata": metadata
                            })

            req = compute.disks().aggregatedList_next(previous_request=req, previous_response=resp)
    except Exception as e:
        print(f"    ❌ Error collecting disk data: {e}")

    # Collect orphaned snapshots
    try:
        print("  • Collecting orphaned snapshots...")
        snapshots_found = categorize_gcp_snapshots(PROJECT_ID, credentials, thresholds)
        for snap in snapshots_found:
            cost_data = get_detailed_resource_costs(
                PROJECT_ID,
                'snapshot',
                {'size_gb': snap['size_gb'], 'snapshot_type': 'standard'}
            )
            metadata = extract_resource_metadata(
                labels=snap['labels'],
                resource_name=snap['name'],
                resource_type='snapshot',
                full_name=f"//compute.googleapis.com/projects/{PROJECT_ID}/global/snapshots/{snap['name']}",
                status="Available",
                cost_analysis=cost_data,
                utilization_data={'reasons': snap['orphaned_reasons']},
                is_orphaned=True
            )
            metadata["Recommendation"] = "Delete"

            optimization_candidates["orphaned_snapshots"].append({
                "name": snap['name'],
                "full_name": f"//compute.googleapis.com/projects/{PROJECT_ID}/global/snapshots/{snap['name']}",
                "size_gb": snap['size_gb'],
                "storage_bytes": snap['storage_bytes'],
                "source_disk": snap['source_disk'],
                "creation_timestamp": snap['creation_timestamp'],
                "is_orphaned": True,
                "orphaned_reasons": snap['orphaned_reasons'],  # Corrected typo here
                "cost_analysis": cost_data,
                "recommendation": "Delete",
                "labels": snap['labels'],
                "resource_metadata": metadata
            })
    except Exception as e:
        print(f"    ❌ Error collecting snapshot data: {e}")

    # NEW: Collect underutilized/orphaned GKE Clusters
    try:
        print("  • Collecting underutilized/orphaned GKE Clusters...")
        clusters_found = categorize_gcp_kubernetes_clusters(PROJECT_ID, credentials, thresholds)
        for cluster in clusters_found:
            cost_data = get_detailed_resource_costs(
                PROJECT_ID,
                'cluster',
                {'node_count': cluster['node_count']}
            )
            metadata = extract_resource_metadata(
                labels=cluster['labels'],
                resource_name=cluster['name'],
                resource_type='cluster',
                region=cluster['location'],
                full_name=f"//container.googleapis.com/projects/{PROJECT_ID}/locations/{cluster['location']}/clusters/{cluster['name']}",
                status=cluster['status'],
                cost_analysis=cost_data,
                utilization_data={'node_count': cluster['node_count'], 'reasons': cluster['orphaned_reasons']},
                is_orphaned=cluster['is_orphaned']
            )
            metadata["Recommendation"] = "Scale Down / Delete"

            optimization_candidates["underutilized_clusters"].append({
                "name": cluster['name'],
                "full_name": f"//container.googleapis.com/projects/{PROJECT_ID}/locations/{cluster['location']}/clusters/{cluster['name']}",
                "location": cluster['location'],
                "node_count": cluster['node_count'],
                "status": cluster['status'],
                "is_orphaned": cluster['is_orphaned'],
                "orphaned_reasons": cluster['orphaned_reasons'],
                "cost_analysis": cost_data,
                "recommendation": "Scale Down / Delete",
                "labels": cluster['labels'],
                "resource_metadata": metadata
            })
    except Exception as e:
        print(f"    ❌ Error collecting GKE cluster data: {e}")

    # NEW: Collect orphaned K8s Persistent Volumes
    try:
        print("  • Collecting orphaned K8s Persistent Volumes...")
        pvs_found = categorize_gcp_kubernetes_persistent_volumes(PROJECT_ID, credentials, thresholds)
        for pv in pvs_found:
            cost_data = get_detailed_resource_costs(
                PROJECT_ID,
                'persistent_volume',
                {'size_gb': pv['size_gb'], 'storage_class': pv['storage_class']}
            )
            metadata = extract_resource_metadata(
                labels=pv['labels'],
                resource_name=pv['name'],
                resource_type='persistent_volume',
                full_name=pv['full_name'],
                status="Available",
                cost_analysis=cost_data,
                utilization_data={'is_claimed': pv['is_claimed'], 'estimated_utilization': pv['estimated_utilization'],
                                  'reasons': pv['orphaned_reasons']},
                is_orphaned=pv['is_orphaned']
            )
            metadata["Recommendation"] = "Delete"

            optimization_candidates["orphaned_pvs"].append({
                "name": pv['name'],
                "full_name": pv['full_name'],
                "is_claimed": pv['is_claimed'],
                "estimated_utilization": pv['estimated_utilization'],
                "size_gb": pv['size_gb'],
                "storage_class": pv['storage_class'],
                "is_orphaned": pv['is_orphaned'],
                "orphaned_reasons": pv['orphaned_reasons'],
                "cost_analysis": cost_data,
                "recommendation": "Delete",
                "labels": pv['labels'],
                "resource_metadata": metadata
            })
    except Exception as e:
        print(f"    ❌ Error collecting K8s Persistent Volume data: {e}")

    print(f"  ✅ Collected {len(optimization_candidates['low_utilization_buckets'])} low utilization buckets")
    print(f"  ✅ Collected {len(optimization_candidates['low_cpu_vms'])} low CPU VMs")
    print(f"  ✅ Collected {len(optimization_candidates['high_free_subnets'])} high free IP subnets")
    print(f"  ✅ Collected {len(optimization_candidates['low_utilization_disks'])} potentially underutilized disks")
    print(f"  ✅ Collected {len(optimization_candidates['orphaned_snapshots'])} orphaned snapshots")
    print(f"  ✅ Collected {len(optimization_candidates['underutilized_clusters'])} underutilized GKE clusters")
    print(f"  ✅ Collected {len(optimization_candidates['orphaned_pvs'])} orphaned K8s Persistent Volumes")

    return optimization_candidates


def save_optimization_report(thresholds, gcp_credentials):
    """
    Generate and save MongoDB-ready resource records as JSON array.
    """
    print(f"\n💾 Generating MongoDB-Ready Resource Records...")
    print("=" * 60)

    # Collect optimization candidates
    candidates = collect_optimization_candidates(PROJECT_ID, gcp_credentials, thresholds)

    # Create flat array of resource metadata for MongoDB
    mongodb_records = []

    # Extract resource_metadata from each resource type
    for bucket in candidates["low_utilization_buckets"]:
        mongodb_records.append(bucket["resource_metadata"])

    for vm in candidates["low_cpu_vms"]:
        mongodb_records.append(vm["resource_metadata"])

    for subnet in candidates["high_free_subnets"]:
        mongodb_records.append(subnet["resource_metadata"])

    for disk in candidates["low_utilization_disks"]:
        mongodb_records.append(disk["resource_metadata"])

    # Add orphaned snapshots to records
    for snapshot in candidates["orphaned_snapshots"]:
        mongodb_records.append(snapshot["resource_metadata"])

    # NEW: Add underutilized clusters to records
    for cluster in candidates["underutilized_clusters"]:
        mongodb_records.append(cluster["resource_metadata"])

    # NEW: Add orphaned persistent volumes to records
    for pv in candidates["orphaned_pvs"]:
        mongodb_records.append(pv["resource_metadata"])

    # Save to JSON file (always overwrite existing)
    output_file = "gcp_optimization.json"
    try:
        if os.path.exists(output_file):
            print(f"  📝 Replacing existing report file: {output_file}")
        else:
            print(f"  📝 Creating new report file: {output_file}")

        # Write MongoDB-ready records array (mode 'w' overwrites existing file)
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(mongodb_records, f, indent=2, default=str, ensure_ascii=False)

        print(f"  ✅ MongoDB-ready records saved to: {output_file}")
        print(f"  📊 Total records for MongoDB insertion: {len(mongodb_records)}")

        # Print breakdown by resource type
        bucket_count = len(candidates["low_utilization_buckets"])
        vm_count = len(candidates["low_cpu_vms"])
        subnet_count = len(candidates["high_free_subnets"])
        disk_count = len(candidates["low_utilization_disks"])
        snapshot_count = len(candidates["orphaned_snapshots"])
        cluster_count = len(candidates["underutilized_clusters"])  # NEW
        pv_count = len(candidates["orphaned_pvs"])  # NEW

        print(f"\n📋 Resource Breakdown:")
        print(f"  • Storage Buckets: {bucket_count}")
        print(f"  • Compute Instances: {vm_count}")
        print(f"  • Network Subnets: {subnet_count}")
        print(f"  • Storage Disks: {disk_count}")
        print(f"  • Orphaned Snapshots: {snapshot_count}")
        print(f"  • GKE Clusters: {cluster_count}")  # NEW
        print(f"  • K8s Persistent Volumes: {pv_count}")  # NEW
        print(f"  • Total Records: {len(mongodb_records)}")

        # Calculate total potential savings
        total_potential_savings = sum([
            candidate.get("cost_analysis", {}).get("total_cost_usd", 0)
            for resource_list in candidates.values()
            for candidate in resource_list
        ])
        print(f"  💰 Total estimated monthly savings: ${total_potential_savings:.2f} USD")

    except Exception as e:
        print(f"  ❌ Error saving records: {e}")
        print("Records data preview:")
        print(json.dumps(mongodb_records[:2] if mongodb_records else [], indent=2, default=str)[:500] + "...")


def insert_to_mongodb(records):
    """Insert GCP optimization records into MongoDB."""
    if not MONGODB_AVAILABLE:
        print("❌ pymongo not available. Skipping MongoDB insertion.")
        return False

    # Validate JSON data before proceeding
    try:
        # Test JSON serialization to ensure data is valid
        json_test = json.dumps(records, default=str)
        print("✅ JSON validation passed - data is valid for MongoDB insertion")
    except Exception as e:
        print(f"❌ JSON validation failed: {e}")
        print("❌ Skipping MongoDB insertion due to invalid JSON data")
        return False

    try:
        # Connect to MongoDB using configurable settings
        client = MongoClient(host=MONGODB_HOST, port=MONGODB_PORT)
        db = client[MONGODB_DATABASE]
        collection = db[MONGODB_COLLECTION]

        # Clear existing records from the collection before inserting new data
        filter_query = {
            "CloudProvider": "GCP",
            "ManagementUnitId": PROJECT_ID,
            "Email": USER_EMAIL
        }
        existing_count = collection.count_documents(filter_query)
        if existing_count > 0:
            collection.delete_many(filter_query)
            print(f"🗑️  Cleared {existing_count} existing records from optimization collection")
        else:
            print("📝 Collection is empty, no records to clear")

        # Add timestamp to each record
        for record in records:
            record['InsertedAt'] = datetime.now(UTC).isoformat()

        # Insert all records
        if records:  # Only insert if records list is not empty
            result = collection.insert_many(records)
            print(f"✅ Successfully inserted {len(result.inserted_ids)} records into MongoDB")
            print(f"📍 Database: {MONGODB_DATABASE}, Collection: {MONGODB_COLLECTION}")
            print(f"📍 MongoDB Server: {MONGODB_HOST}:{MONGODB_PORT}")
        else:
            print("ℹ️ No records to insert into MongoDB.")

        return True

    except Exception as e:
        print(f"❌ Error inserting into MongoDB: {e}")
        return False



# ================================================================================
# METADATA EXTRACTION
# ================================================================================

def extract_resource_metadata(labels, resource_name, resource_type, region=None, zone=None, full_name=None, status=None,
                              cost_analysis=None, utilization_data=None, is_orphaned=False):
    """
    Extract Azure-style metadata from GCP resource data and labels.

    Args:
        labels (dict): Resource labels from GCP
        resource_name (str): Name of the resource
        resource_type (str): Type of resource (vm, bucket, disk, subnet, snapshot, cluster, persistent_volume)
        region (str, optional): Resource region
        zone (str, optional): Resource zone
        full_name (str, optional): Full resource name/path
        status (str, optional): Resource status
        cost_analysis (dict, optional): Cost analysis data
        utilization_data (dict, optional): Contains utilization metrics for generating findings
        is_orphaned (bool, optional): True if the resource is considered orphaned/unattached. Defaults to False.

    Returns:
        dict: Structured metadata matching Azure format
    """

    # Helper function to safely get label value
    # Modified: Ensure empty strings are also treated as "NA"
    def get_label_value(key, default="NA"):
        value = labels.get(key) if labels else None
        return value if value is not None and value != "" else default

    # Extract region from resource ID or use provided region/zone
    extracted_region = region or zone or "NA"
    if full_name and not extracted_region:
        # Try to extract region/zone from full resource name
        if "/zones/" in full_name:
            extracted_region = full_name.split("/zones/")[1].split("/")[0]
        elif "/regions/" in full_name:
            extracted_region = full_name.split("/regions/")[1].split("/")[0]
        # NEW: Extract location for GKE clusters from full_name if available
        elif "/locations/" in full_name and resource_type == 'cluster':
            extracted_region = full_name.split("/locations/")[1].split("/")[0]

    # Determine ResourceType based on GCP resource type
    resource_type_mapping = {
        'vm': 'Compute',
        'bucket': 'Storage',
        'disk': 'Storage',
        'subnet': 'Networking',
        'snapshot': 'Storage',
        'cluster': 'Compute',  # NEW: GKE Cluster
        'persistent_volume': 'Storage'  # NEW: K8s Persistent Volume
    }

    # Determine SubResourceType - actual GCP resource types
    sub_resource_mapping = {
        'vm': 'Virtual Machine',
        'bucket': 'Bucket',
        'disk': 'Disk',
        'subnet': 'Subnet',
        'snapshot': 'Snapshot',
        'cluster': 'GKE Cluster',  # NEW: GKE Cluster sub-type
        'persistent_volume': 'Persistent Volume'  # NEW: K8s Persistent Volume sub-type
    }

    # Get total cost from cost analysis - remove unnecessary decimal places and handle scientific notation
    total_cost = "Unknown"
    if cost_analysis and 'total_cost_usd' in cost_analysis:
        cost_value = cost_analysis['total_cost_usd']
        if cost_value == 0:
            total_cost = "0"
        elif cost_value == int(cost_value):  # If it's a whole number
            total_cost = f"{int(cost_value)}"
        elif cost_value < 0.01:  # For very small values, show as 0
            total_cost = "0"
        else:
            # Round to 2 decimal places to avoid scientific notation
            total_cost = f"{cost_value:.2f}".rstrip('0').rstrip('.')
            if total_cost == "":
                total_cost = "0"

    # Get currency from cost analysis instead of labels
    currency = "USD"  # Default
    if cost_analysis and 'currency' in cost_analysis:
        currency = cost_analysis['currency']

    # Determine status
    resource_status = status or "Unknown"
    if resource_type == 'vm':
        resource_status = status or "Running"  # Assume running if not specified
    elif resource_type == 'bucket':
        resource_status = "Available"
    elif resource_type == 'disk':
        resource_status = status or "Available"
    elif resource_type == 'subnet':
        resource_status = "Available"
    elif resource_type == 'snapshot':
        resource_status = status or "Ready"  # Snapshots are usually Ready when created
    elif resource_type == 'cluster':  # NEW: GKE Cluster status
        resource_status = status or "RUNNING"
    elif resource_type == 'persistent_volume':  # NEW: K8s PV status
        resource_status = status or "Available"

    # Generate simplified finding and recommendation based on resource type
    finding = "Resource identified for optimization"
    recommendation = "Review usage and consider optimization"

    if resource_type == 'vm':
        finding = "VM underutilised"
        recommendation = "Scale Down"
    elif resource_type == 'bucket':
        finding = "Bucket underutilised"
        recommendation = "Try Merging"
    elif resource_type == 'subnet':
        finding = "Subnet underutilised"
        recommendation = "Scale Down"
    elif resource_type == 'disk':
        finding = "Disk underutilised"
        recommendation = "Scale Down"
    elif resource_type == 'snapshot':
        finding = "Snapshot potentially unneeded"
        recommendation = "Delete"
    elif resource_type == 'cluster':  # NEW: GKE Cluster finding
        finding = "GKE Cluster underutilised"
        recommendation = "Scale Down / Delete"
    elif resource_type == 'persistent_volume':  # NEW: K8s PV finding
        finding = "Persistent Volume underutilised"
        recommendation = "Delete"

    # Check for missing/empty tags and append "; Untagged" to the finding if any are missing
    required_tags_for_finding = ["features", "lab", "platform", "cio", "ticketid", "environment"]
    is_untagged = False
    if not labels:
        is_untagged = True
    else:
        for tag in required_tags_for_finding:
            # Check if the tag exists and its value is not empty or None
            if not labels.get(tag):
                is_untagged = True
                break  # Stop checking once one missing tag is found

    if is_untagged:
        finding += "; Untagged"

    # Append "; Orphaned" if the resource is identified as orphaned
    if is_orphaned:
        finding += "; Orphaned"

    return {
        "_id": full_name or f"//cloudresourcemanager.googleapis.com/projects/{PROJECT_ID}/resources/{resource_name}",
        "CloudProvider": "GCP",
        "ManagementUnitId": PROJECT_ID,
        "ApplicationCode": get_label_value("applicationcode"),
        "CostCenter": get_label_value("costcenter"),
        "CIO": get_label_value("cio"),
        "Owner": get_label_value("owner"),
        "TicketId": get_label_value("ticketid"),
        # Include these tags in the MongoDB record
        "Features": get_label_value("features"),
        "Lab": get_label_value("lab"),
        "Platform": get_label_value("platform"),
        "ResourceType": resource_type_mapping.get(resource_type, resource_type.title()),
        "SubResourceType": sub_resource_mapping.get(resource_type, resource_type.title()),
        "ResourceName": resource_name,
        "Region": extracted_region,
        "TotalCost": total_cost,
        "Currency": currency,
        "Finding": finding,
        "Recommendation": recommendation,
        "Environment": get_label_value("environment"),
        "Timestamp": datetime.now(UTC).isoformat().replace('+00:00', 'Z'),
        "ConfidenceScore": "NA",
        "Status": resource_status,
        "Entity": get_label_value("entity"),
        "RootId": "NA",
        "Email": USER_EMAIL
    }


# ================================================================================
# MAIN EXECUTION
# ================================================================================

if __name__ == "__main__":
    print("🚀 GCP Resource Optimization Analysis")
    print("=" * 80)
    print(f"Project: {PROJECT_ID}")
    print(f"User Email: {USER_EMAIL}")
    print(f"Analysis Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Disk Analysis: Enabled")
    print("=" * 80)

    # Fetch dynamic thresholds from MongoDB
    thresholds = get_thresholds_from_mongodb(USER_EMAIL, collection_name="standardConfigsDb")

    try:
        # Run comprehensive resource analysis
        categorize_gcp_resources(PROJECT_ID, gcp_credentials, bucket_quota_gb=100, thresholds=thresholds)
        categorize_gcp_vm_cpu_utilization(PROJECT_ID, gcp_credentials, thresholds=thresholds)
        list_subnets_with_cidr_and_ip_usage(PROJECT_ID, thresholds=thresholds, credentials=gcp_credentials)
        categorize_gcp_disk_utilization(PROJECT_ID, gcp_credentials, thresholds=thresholds)
        categorize_gcp_snapshots(PROJECT_ID, gcp_credentials, thresholds)  # Call snapshot analysis
        categorize_gcp_kubernetes_clusters(PROJECT_ID, gcp_credentials, thresholds)  # NEW: Call GKE cluster analysis
        categorize_gcp_kubernetes_persistent_volumes(PROJECT_ID, gcp_credentials,
                                                     thresholds)  # NEW: Call K8s PV analysis

        # Generate and save detailed JSON report
        save_optimization_report(thresholds, gcp_credentials)
        # Insert records to MongoDB if available
        try:
            with open("gcp_optimization.json", 'r', encoding='utf-8') as f:
                records = json.load(f)
            insert_to_mongodb(records)
        except Exception as e:
            print(f"⚠️  Could not insert to MongoDB: {e}")

        print("\n" + "=" * 80)
        print("✅ Analysis Complete! Check JSON file and MongoDB for optimization data.")
        print("=" * 80)

    except Exception as e:
        print(f"\n❌ Critical Error: {e}")
        print("Please check your credentials and project configuration.")

    except KeyboardInterrupt:
        print(f"\n⚠️  Analysis interrupted by user.")
        print("Partial results may be available.")
