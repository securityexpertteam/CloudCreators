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

SKU_CACHE = {}
BILLING_SERVICE_NAME = ""


# ================================================================================
# ARGUMENT PARSING
# ================================================================================
parser = argparse.ArgumentParser(description="GCP Resource Optimization Script")
# Use '--client_email' consistently for the service account
parser.add_argument("--client_email", required=True, help="GCP Service Account Client Email (for authentication)")
parser.add_argument("--private_key", required=True, help="GCP Service Account Private Key. Replace newlines with '\\n'.")
parser.add_argument("--project_id", required=True, help="GCP Project ID to analyze")
# Use '--user_email' consistently for the user's email
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
        "scopes": ["https://www.googleapis.com/auth/cloud-platform"]
    }
    gcp_credentials = service_account.Credentials.from_service_account_info(credentials_info)
    print("✅ Authentication successful.")

    # Initialize all necessary clients with credentials
    compute = discovery.build('compute', 'v1', credentials=gcp_credentials)
    run_admin_client = discovery.build('run', 'v1', credentials=gcp_credentials)
    monitoring_client = monitoring_v3.MetricServiceClient(credentials=gcp_credentials)
    asset_client = asset_v1.AssetServiceClient(credentials=gcp_credentials)
    billing_client = discovery.build('cloudbilling', 'v1', credentials=gcp_credentials)
    print("✅ All GCP clients initialized.")

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


def initialize_billing_info():
    """Fetches the service name for billing lookups, targeting Compute Engine."""
    global BILLING_SERVICE_NAME
    if BILLING_SERVICE_NAME:
        return

    try:
        print("🔍 Initializing billing information...")
        request = billing_client.services().list()
        response = request.execute()

        # A more reliable method is to find the service for Compute Engine,
        # as most costs are derived from it.
        for service in response.get('services', []):
            if service.get('displayName') == 'Compute Engine':
                BILLING_SERVICE_NAME = service['name']
                break

        if not BILLING_SERVICE_NAME:
            # Fallback if Compute Engine isn't found for some reason
            if response.get('services'):
                BILLING_SERVICE_NAME = response['services'][0]['name']
            else:
                raise Exception(
                    "Could not find any billing services. Ensure the Cloud Billing API is enabled and permissions are correct.")

        print(f"✅ Billing Service Name for lookups set to: {BILLING_SERVICE_NAME}")

    except HttpError as e:

        error_details = json.loads(e.content.decode())
        if "SERVICE_DISABLED" in str(error_details):
            print("\n❌ CRITICAL ERROR: The Cloud Billing API is not enabled for this project.")
            print(
                "Please enable it by visiting: https://console.cloud.google.com/apis/library/cloudbilling.googleapis.com")
        else:
            print(f"❌ HTTP Error initializing billing: {e}")
        exit(1)
    except Exception as e:
        print(f"❌ Error initializing billing: {e}")
        exit(1)

def get_sku_price(service_name, sku_description_filter, region="global"):
    """
    Fetches the price for a given SKU description and caches it.
    Returns price per unit and the unit (e.g., "per hour", "per gibibyte month").
    """
    cache_key = (service_name, sku_description_filter, region)
    if cache_key in SKU_CACHE:
        return SKU_CACHE[cache_key]

    if not BILLING_SERVICE_NAME:
        initialize_billing_info()

    try:
        # Construct the parent service name for the SKU lookup
        service_lookup_name = f"services/{service_name}"

        # Fetch all SKUs for the service (e.g., Compute Engine)
        all_skus = []
        page_token = None
        while True:
            request = billing_client.services().skus().list(parent=service_lookup_name, pageToken=page_token)
            response = request.execute()
            all_skus.extend(response.get('skus', []))
            page_token = response.get('nextPageToken')
            if not page_token:
                break

        for sku in all_skus:
            # Match based on description and region
            if sku_description_filter.lower() in sku.get('description', '').lower() and \
                    (region in sku.get('serviceRegions', []) or region == "global"):
                pricing_info = sku.get('pricingInfo', [{}])[0]
                pricing_expression = pricing_info.get('pricingExpression', {})

                # Find the first price tier
                price_nanos = pricing_expression.get('tieredRates', [{}])[0].get('unitPrice', {}).get('nanos', 0)
                price_usd = price_nanos / 1_000_000_000

                usage_unit = pricing_expression.get('usageUnitDescription', 'unit')

                # Cache the result
                print(f"  DEBUG: Found price: ${price_usd} per {usage_unit}")
                SKU_CACHE[cache_key] = (price_usd, usage_unit)
                return price_usd, usage_unit

    except Exception as e:
        print(f"    ⚠️ Could not fetch price for SKU '{sku_description_filter}': {e}")

    # Cache failure case
    SKU_CACHE[cache_key] = (0.0, "unknown")
    return 0.0, "unknown"


def analyze_gke_container_images(project_id, credentials):
    """
    Analyzes container images in GKE pods to find those using standard, non-minimal base images.
    """
    print(f"\n🖼️  Analyzing GKE Container Base Images")
    print("=" * 60)
    print("📋 Identifying containers that could use more efficient base images (e.g., alpine, slim)")

    asset_client = asset_v1.AssetServiceClient(credentials=credentials)
    flagged_containers = []
    total_pods_analyzed = 0

    # Map of standard base images to their recommended minimal alternatives
    MINIMAL_IMAGE_MAP = {
        'ubuntu': 'ubuntu:minimal',
        'debian': 'debian:slim',
        'python': 'python:slim or python:alpine',
        'node': 'node:alpine',
        'golang': 'golang:alpine',
        'nginx': 'nginx:alpine',
        'httpd': 'httpd:alpine',
        'openjdk': 'openjdk:alpine',
        'mysql': 'mysql:8.0-slim',
        'redis': 'redis:alpine'
    }

    try:
        # Use Cloud Asset Inventory to find all Pod resources in the project
        response = asset_client.search_all_resources(
            request={
                "scope": f"projects/{project_id}",
                "asset_types": ["k8s.io/Pod"],
                "page_size": 500
            }
        )

        for resource in response:
            total_pods_analyzed += 1
            pod_data_str = resource.additional_attributes.get('resource')
            if not pod_data_str:
                continue

            pod_data = json.loads(pod_data_str)
            pod_name = pod_data.get('metadata', {}).get('name', 'unknown-pod')
            namespace = pod_data.get('metadata', {}).get('namespace', 'default')

            # Extract cluster name and location from the full resource name
            # e.g., //container.googleapis.com/projects/p/locations/l/clusters/c/k8s/pods/ns/pod
            try:
                parts = resource.name.split('/clusters/')
                cluster_name = parts[1].split('/')[0]
                location = resource.location
            except IndexError:
                cluster_name = "unknown-cluster"
                location = "unknown-location"

            # Check all containers within the pod spec
            containers = pod_data.get('spec', {}).get('containers', [])
            for container in containers:
                image_used = container.get('image')
                if not image_used:
                    continue

                # Get the base name of the image (e.g., 'ubuntu' from 'ubuntu:20.04')
                base_image = image_used.split(':')[0].split('/')[-1]

                if base_image in MINIMAL_IMAGE_MAP:
                    recommended_image = MINIMAL_IMAGE_MAP[base_image]
                    container_name = container.get('name', 'unknown-container')

                    print(
                        f"  ⚠️  Found inefficient image in Pod '{pod_name}' (Cluster: {cluster_name}): '{image_used}'")

                    # Create a standard metadata record for the finding
                    metadata = extract_resource_metadata(
                        labels=pod_data.get('metadata', {}).get('labels', {}),
                        resource_name=f"{pod_name}/{container_name}",
                        resource_type='container',  # Use a new type for this
                        region=location,
                        full_name=f"{resource.name}/containers/{container_name}",
                        status="Running",
                        cost_analysis={'total_cost_usd': 0.0},  # This is an efficiency gain, not a direct cost
                        utilization_data={'finding': f'Inefficient base image: {image_used}'},
                        is_orphaned=False
                    )
                    metadata['Recommendation'] = f"Consider using a minimal base image like '{recommended_image}'"

                    flagged_containers.append({
                        'name': f"{pod_name}/{container_name}",
                        'pod_name': pod_name,
                        'namespace': namespace,
                        'cluster': cluster_name,
                        'location': location,
                        'image_used': image_used,
                        'recommendation': recommended_image,
                        'resource_metadata': metadata
                    })

        print(f"\nTotal pods analyzed: {total_pods_analyzed}")
        if not flagged_containers:
            print("  ✅ All container base images appear to be efficient or are not in the standard library.")
        else:
            print(f"🔍 Found {len(flagged_containers)} containers using inefficient base images.")


    except Exception as e:
        print(f"❌ Error analyzing GKE container images: {e}")

    return flagged_containers

def analyze_cloud_run_optimization_opportunities(project_id, credentials):
    """Analyzes Cloud Run services for right-sizing, concurrency, and min-instance costs."""
    print("\n🏃 Analyzing Cloud Run Services for Advanced Optimization...")
    print("=" * 60)

    optimization_candidates = []

    try:
        # The parent location '-' indicates a global search for all services in the project.
        parent = f"projects/{project_id}/locations/-"
        request = run_admin_client.projects().locations().services().list(parent=parent)
        response = request.execute()
        services = response.get('items', [])

        if not services:
            print("  ✅ No Cloud Run services found.")
            return []

        print(f"  Found {len(services)} Cloud Run services to analyze.")

        for service in services:
            service_name = service['metadata']['name']
            location = service['metadata']['labels']['cloud.googleapis.com/location']
            service_link = service['metadata'].get('selfLink',
                                                   f"projects/{project_id}/locations/{location}/services/{service_name}")
            all_findings_for_service = []

            template = service['spec']['template']
            annotations = template['metadata'].get('annotations', {})

            # --- 1. Min Instances Analysis (Cost of Idle) ---
            min_instances = int(annotations.get('autoscaling.knative.dev/minScale', 0))

            if min_instances > 0:
                container = template['spec']['containers'][0]
                cpu_limit_str = container['resources']['limits'].get('cpu', '1000m')
                cpu_limit = float(cpu_limit_str.replace('m', '')) / 1000 if 'm' in cpu_limit_str else float(
                    cpu_limit_str)

                mem_limit_str = container['resources']['limits'].get('memory', '512Mi')
                mem_value = int(''.join(filter(str.isdigit, mem_limit_str)))
                mem_limit_gb = mem_value / 1024 if 'Mi' in mem_limit_str else mem_value

                idle_cost_config = {
                    'name': service_name,
                    'cpu': cpu_limit,
                    'memory_gb': mem_limit_gb,
                    'region': location
                }
                idle_cost = get_resource_cost('cloud_run_idle', idle_cost_config) * min_instances

                metadata = extract_resource_metadata(
                    labels=service['metadata'].get('labels', {}),
                    resource_name=service_name,
                    resource_type='cloud_run',
                    region=location,
                    full_name=f"//run.googleapis.com/{service_link}",
                    status="ACTIVE",
                    cost_analysis={'total_cost_usd': idle_cost},
                    utilization_data={'finding': f'Idle cost for {min_instances} min-instance(s)'},
                    is_orphaned=False
                )
                metadata['Recommendation'] = "Set min-instances to 0"

                all_findings_for_service.append({
                    "type": "Idle Cost",
                    "description": f"Service has min-instances set to {min_instances}, incurring an estimated monthly idle cost of ${idle_cost:.2f}.",
                    "recommendation": "Set min-instances to 0 if cold starts are acceptable.",
                    "monthly_savings": idle_cost,
                    "resource_metadata": metadata
                })

            # --- 2. Concurrency Analysis ---
            concurrency = template['spec'].get('containerConcurrency', 80)
            if concurrency > 0 and concurrency < 10:
                metadata = extract_resource_metadata(
                    labels=service['metadata'].get('labels', {}),
                    resource_name=service_name,
                    resource_type='cloud_run',
                    region=location,
                    full_name=f"//run.googleapis.com/{service_link}",
                    status="ACTIVE",
                    cost_analysis={'total_cost_usd': 0.0},  # No direct cost saving
                    utilization_data={'finding': f'Low concurrency set to {concurrency}'},
                    is_orphaned=False
                )
                metadata['Recommendation'] = "Increase concurrency if I/O bound"
                all_findings_for_service.append({
                    "type": "Low Concurrency",
                    "description": f"Concurrency is set to a low value of {concurrency}. This may cause more instances to spin up than necessary.",
                    "recommendation": "Review if this application is truly CPU-bound. If I/O bound, consider increasing concurrency.",
                    "monthly_savings": 0.0,
                    "resource_metadata": metadata
                })

            # --- 3. Right-Sizing Analysis (CPU/Memory) ---
            # This part remains a placeholder as it requires more complex metric analysis,
            # but it's structured to be added in the future.

            if all_findings_for_service:
                print(f"  ⚠️  Found {len(all_findings_for_service)} optimization opportunities for '{service_name}'")
                optimization_candidates.extend(all_findings_for_service)
            else:
                print(f"  ✅ '{service_name}' appears well-configured.")

    except HttpError as e:
        if "run.googleapis.com has not been used" in str(e):
            print("  ✅ Cloud Run API is not used in this project, skipping.")
        else:
            print(f"❌ Error analyzing Cloud Run services: {e}")
    except Exception as e:
        print(f"❌ An unexpected error occurred during Cloud Run analysis: {e}")

    return optimization_candidates

def get_thresholds_from_mongodb(email, collection_name="standardConfigsDb"):
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

def get_resource_cost(resource_type, config):
    """
    Calculates the monthly cost for a resource using dynamic pricing from the Billing API.
    Handles VMs, disks, Cloud Run idle instances, snapshots, and storage buckets.
    """
    # Service IDs for Billing API.
    compute_service_id = "6F81-5844-456A"
    run_service_id = "9662-B5DA-4595"
    storage_service_id = "95FF-2D51-2352" # Specific service ID for Cloud Storage

    cost_per_month = 0.0

    try:
        if resource_type == 'vm':
            instance_family = config['machine_type'].split('-')[0].upper()
            cpu_sku_filter = f"{instance_family} Instance Core running in"
            cpu_price_per_hour, _ = get_sku_price(compute_service_id, cpu_sku_filter, config['region'])

            ram_sku_filter = f"{instance_family} Instance Ram running in"
            ram_price_per_hour_gb, _ = get_sku_price(compute_service_id, ram_sku_filter, config['region'])

            cost_per_month = (config['cpu_cores'] * cpu_price_per_hour + config['memory_gb'] * ram_price_per_hour_gb) * 730

        elif resource_type == 'disk':
            disk_type_map = {'pd-standard': 'Standard', 'pd-balanced': 'Balanced', 'pd-ssd': 'SSD'}
            disk_type_name = disk_type_map.get(config['disk_type'], 'Standard')
            disk_sku_filter = f"{disk_type_name} backed PD Capacity"
            disk_price_per_gb_month, _ = get_sku_price(compute_service_id, disk_sku_filter, config['region']) # Note: Disks are under Compute service
            cost_per_month = config['size_gb'] * disk_price_per_gb_month

        elif resource_type == 'snapshot':
            # Snapshots are priced based on their storage size.
            snapshot_sku_filter = "Snapshot Storage"
            snapshot_price_per_gb_month, _ = get_sku_price(compute_service_id, snapshot_sku_filter, config['region']) # Snapshots are also under Compute
            cost_per_month = config['size_gb'] * snapshot_price_per_gb_month

        elif resource_type == 'bucket':
            # Using a common SKU for standard storage. Assumes 'Standard' storage class.
            bucket_sku_filter = "Standard Storage US" # More specific filter
            bucket_price_per_gb_month, _ = get_sku_price(storage_service_id, bucket_sku_filter, config['region'])
            cost_per_month = config['size_gb'] * bucket_price_per_gb_month

        elif resource_type == 'cloud_run_idle':
            cpu_sku_filter = "Cloud Run CPU Allocation"
            cpu_price_per_sec, _ = get_sku_price(run_service_id, cpu_sku_filter, config['region'])

            ram_sku_filter = "Cloud Run Memory Allocation"
            ram_price_per_sec_gb, _ = get_sku_price(run_service_id, ram_sku_filter, config['region'])

            cost_per_second = (config['cpu'] * cpu_price_per_sec) + (config['memory_gb'] * ram_price_per_sec_gb)
            cost_per_month = cost_per_second * 60 * 60 * 730 * config.get('min_instances', 1)

    except Exception as e:
        print(f"    ⚠️ Cost calculation failed for {resource_type} {config.get('name', '')}: {e}")
        return 0.0

    return cost_per_month

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
    """
    try:
        container_client = discovery.build('container', 'v1', credentials=credentials)

        # CORRECTED: Use a single 'name' parameter for the API call
        cluster_full_name = f"projects/{project_id}/locations/{location}/clusters/{cluster_name}"

        cluster_info = container_client.projects().locations().clusters().get(
            name=cluster_full_name
        ).execute()

        node_count = cluster_info.get('currentNodeCount', 0)
        estimated_cpu_usage_percent = 0.0
        estimated_memory_usage_percent = 0.0

        if node_count > 0:
            estimated_cpu_usage_percent = 10.0
            estimated_memory_usage_percent = 15.0

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

def categorize_gcp_kubernetes_clusters(project_id, credentials, thresholds):
    """
    Analyzes GKE clusters for underutilization or orphaned status.
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
        # Use a wildcard '-' to list clusters from all locations in a single API call
        parent = f"projects/{project_id}/locations/-"
        request = container_client.projects().locations().clusters().list(parent=parent)
        response = request.execute()

        for cluster in response.get('clusters', []):
            total_clusters += 1
            cluster_name = cluster.get('name')
            node_count = cluster.get('currentNodeCount', 0)
            cluster_status = cluster.get('status', 'UNKNOWN')
            cluster_location = cluster.get('location')

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

            if metrics:
                if metrics.get('estimated_cpu_usage_percent', 100) < low_cpu_util_threshold:
                    is_orphaned_cluster = True
                    reasons.append("low_cpu_utilization")
                if metrics.get('estimated_memory_usage_percent', 100) < low_mem_util_threshold:
                    is_orphaned_cluster = True
                    reasons.append("low_memory_utilization")

            print(f"  • {cluster_name} (Location: {cluster_location}, Nodes: {node_count}, Status: {cluster_status})")

            if is_orphaned_cluster:
                underutilized_clusters.append({
                    'name': cluster_name,
                    'location': cluster_location,
                    'node_count': node_count,
                    'status': cluster_status,
                    'is_orphaned': True,
                    'orphaned_reasons': list(set(reasons)),
                    'labels': cluster.get('resourceLabels', {})
                })

        print(f"\nTotal GKE clusters analyzed: {total_clusters}")
        print(f"🔍 Potentially Underutilized/Orphaned GKE Clusters: {len(underutilized_clusters)}")

        if underutilized_clusters:
            for cluster_info in underutilized_clusters:
                reasons_str = ", ".join(cluster_info['orphaned_reasons'])
                orphaned_status = " (Orphaned)" if cluster_info['is_orphaned'] else ""
                print(f"  ⚠️  {cluster_info['name']} (Nodes: {cluster_info['node_count']}, Status: {cluster_info['status']}){orphaned_status} - Reasons: {reasons_str}")
        else:
            print("  ✅ No potentially underutilized/orphaned GKE clusters found")

    except Exception as e:
        print(f"❌ Error analyzing GKE clusters: {e}")

    return underutilized_clusters

def categorize_gcp_cloud_run(project_id, credentials, thresholds):
    """Analyzes Cloud Run services for inactivity."""
    inactivity_threshold_days = thresholds.get('cloud_run_inactivity_days', 30)
    print(f"\n🏃 Analyzing Cloud Run Services (Inactive if no requests in >{inactivity_threshold_days} days)")
    print("=" * 60)

    inactive_services = []
    total_services = 0
    monitoring_client = monitoring_v3.MetricServiceClient(credentials=credentials)
    asset_client = asset_v1.AssetServiceClient(credentials=credentials)
    scope = f"projects/{project_id}"

    try:
        response = asset_client.search_all_resources(
            request={"scope": scope, "asset_types": ["run.googleapis.com/Service"]}
        )

        for resource in response:
            total_services += 1
            service_name = resource.display_name
            location = resource.location
            service_link = resource.name

            end_time = datetime.now(UTC)
            start_time = end_time - timedelta(days=inactivity_threshold_days)
            interval = monitoring_v3.TimeInterval(end_time=end_time, start_time=start_time)

            filter_str = f'metric.type="run.googleapis.com/request_count" AND resource.labels.service_name="{service_name}" AND resource.labels.location="{location}"'
            time_series_request = monitoring_v3.ListTimeSeriesRequest(
                name=f"projects/{project_id}",
                filter=filter_str,
                interval=interval,
                view=monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.HEADERS,
            )

            results = monitoring_client.list_time_series(request=time_series_request)
            request_count = sum(1 for _ in results)

            if request_count == 0:
                print(f"  ⚠️  {service_name} (Location: {location}) - Inactive")

                # FIX: Create the full metadata record for MongoDB
                metadata = extract_resource_metadata(
                    labels=dict(resource.labels) if hasattr(resource, 'labels') and resource.labels else {},
                    resource_name=service_name,
                    resource_type='cloud_run',
                    region=location,
                    full_name=service_link,
                    status="INACTIVE",
                    cost_analysis={'total_cost_usd': 0.0},  # Inactivity itself has no cost, min-instances does
                    utilization_data={'finding': 'Inactive for over 30 days'},
                    is_orphaned=True  # Treat as orphaned due to inactivity
                )
                metadata['Recommendation'] = "Delete if no longer needed"

                inactive_services.append({
                    'name': service_name,
                    'location': location,
                    'full_name': service_link,
                    'labels': dict(resource.labels) if hasattr(resource, 'labels') and resource.labels else {},
                    'resource_metadata': metadata  # Attach the full record
                })
            else:
                print(f"  • {service_name} (Location: {location}) - Active")

        print(f"\nTotal Cloud Run services analyzed: {total_services}")
        if not inactive_services:
            print("  ✅ No inactive Cloud Run services found.")

    except Exception as e:
        print(f"❌ Error analyzing Cloud Run services: {e}")

    return inactive_services


def categorize_gcp_instance_groups(project_id, credentials, thresholds):
    """
    Analyzes Instance Group Managers with clarified, distinct logic for:
    1. Fixed-size IGMs (min instances == max instances).
    2. Underutilized IGMs (running instances below a threshold).
    3. Untagged instance templates.
    """
    print(f"\n👨‍👩‍👧‍👦 Analyzing Instance Groups with Enhanced Logic")
    print("=" * 60)

    flagged_groups = []
    total_groups = 0
    required_tags = ["features", "lab", "platform", "cio", "ticketid", "environment"]

    try:
        request = compute.instanceGroupManagers().aggregatedList(project=project_id)
        while request is not None:
            response = request.execute()
            for scope, data in response.get('items', {}).items():
                if 'instanceGroupManagers' in data:
                    for igm in data['instanceGroupManagers']:
                        total_groups += 1
                        igm_name = igm.get('name')
                        location = igm.get('zone', igm.get('region', 'unknown')).split('/')[-1]
                        target_instances = igm.get('targetSize', 0)

                        reasons_for_flagging = []
                        recommendation = "Review configuration"
                        labels = {}

                        # Check 1: Is the instance template untagged?
                        template_url = igm.get('instanceTemplate')
                        if template_url:
                            try:
                                template_name = template_url.split('/')[-1]
                                template_info = compute.instanceTemplates().get(project=project_id,
                                                                                instanceTemplate=template_name).execute()
                                labels = template_info.get('properties', {}).get('labels', {})
                                if not all(tag in labels for tag in required_tags):
                                    reasons_for_flagging.append("Instance template is untagged")
                            except Exception as e:
                                print(f"    ⚠️ Could not fetch labels for template {template_url}: {e}")

                        # Check 2: Is the IGM configured as a fixed-size group?
                        autoscaling_policy = igm.get('autoscalingPolicy', {})
                        min_replicas = autoscaling_policy.get('minNumReplicas')
                        max_replicas = autoscaling_policy.get('maxNumReplicas')

                        if min_replicas is not None and min_replicas == max_replicas:
                            reasons_for_flagging.append(f"Fixed size (min/max instances are both {min_replicas})")
                            recommendation = "Consider enabling autoscaling if workload varies"

                        # Check 3: Is the IGM underutilized (e.g., set to 0 instances)?
                        elif target_instances < thresholds.get('instance_group_min_instances', 1):
                            reasons_for_flagging.append(f"Underutilized (target size is {target_instances})")
                            recommendation = "Delete if no longer needed"

                        # If we have any reason to flag this IGM, create a record
                        if reasons_for_flagging:
                            finding_reason = "; ".join(reasons_for_flagging)

                            metadata = extract_resource_metadata(
                                labels=labels,
                                resource_name=igm_name,
                                resource_type='instance_group',
                                region=location,
                                full_name=igm.get('selfLink'),
                                status="ACTIVE",
                                cost_analysis={'total_cost_usd': 0.0},
                                utilization_data={'target_size': target_instances},
                                is_orphaned=(target_instances == 0)
                            )
                            metadata['Recommendation'] = recommendation
                            metadata['Finding'] = finding_reason

                            flagged_groups.append({
                                "name": igm_name,
                                "location": location,
                                "instance_count": target_instances,
                                "full_name": igm.get('selfLink'),
                                "labels": labels,
                                "resource_metadata": metadata
                            })

            request = compute.instanceGroupManagers().aggregatedList_next(previous_request=request,
                                                                          previous_response=response)

        print(f"\nTotal instance groups analyzed: {total_groups}")
        if not flagged_groups:
            print("  ✅ No instance groups flagged for review.")
        else:
            print(f"  ⚠️ Found {len(flagged_groups)} instance groups for review:")
            for group in flagged_groups:
                print(f"    - {group['name']}: {group['resource_metadata']['Finding']}")

    except Exception as e:
        print(f"❌ Error analyzing instance groups: {e}")

    return flagged_groups

def analyze_gcp_resource_quotas(project_id, credentials, thresholds):
    """
    Analyzes GCP project-level quotas to find those nearing their limits.
    """
    print(f"\n📊 Analyzing Resource Quotas (Flagged if > 80% used)")
    print("=" * 60)

    high_usage_quotas = []
    # The threshold for flagging a quota, e.g., 80%
    utilization_threshold = thresholds.get('quota_utilization_threshold', 80.0)

    try:
        # The project details contain the quota information for Compute Engine
        project_info = compute.projects().get(project=project_id).execute()
        quotas = project_info.get('quotas', [])

        print(f"  • Found {len(quotas)} Compute Engine quotas to analyze for project {project_id}.")

        for quota in quotas:
            limit = quota.get('limit', 0.0)
            usage = quota.get('usage', 0.0)
            metric = quota.get('metric', 'UNKNOWN_METRIC')

            # Skip quotas with no limit
            if limit == 0.0:
                continue

            utilization_percent = (usage / limit) * 100

            if utilization_percent > utilization_threshold:
                print(
                    f"  ⚠️  High Usage Quota: {metric} is at {utilization_percent:.2f}% ({int(usage)} / {int(limit)})")

                # Create a metadata record for the finding
                metadata = {
                    "_id": f"//compute.googleapis.com/projects/{project_id}/quotas/{metric}",
                    "CloudProvider": "GCP",
                    "ManagementUnitId": project_id,
                    "ResourceType": "Project",
                    "SubResourceType": "Quota",
                    "ResourceName": metric,
                    "Region": "global",
                    "Finding": f"Quota usage is high ({utilization_percent:.2f}%)",
                    "Recommendation": "Review quota limit or clean up unused resources.",
                    "Email": USER_EMAIL,
                    # Add other standard fields
                    "ApplicationCode": "NA", "CostCenter": "NA", "CIO": "NA", "Owner": "NA", "TicketId": "NA",
                    "Features": "NA", "Lab": "NA", "Platform": "NA", "TotalCost": "0", "Currency": "USD",
                    "Environment": "NA", "Timestamp": datetime.now(UTC).isoformat().replace('+00:00', 'Z'),
                    "ConfidenceScore": "NA", "Status": "High Usage", "Entity": "NA", "RootId": "NA",
                }

                high_usage_quotas.append({
                    "metric": metric,
                    "usage": usage,
                    "limit": limit,
                    "utilization_percent": utilization_percent,
                    "resource_metadata": metadata
                })

        if not high_usage_quotas:
            print("  ✅ All quota utilizations are within the threshold.")

    except Exception as e:
        print(f"❌ Error analyzing resource quotas: {e}")

    return high_usage_quotas

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

                    # === START of REPLACEMENT for VM cost calculation ===
                    # Get machine type details to calculate cost accurately
                    machine_type = 'unknown'
                    monthly_cost = 0.0
                    cost_data = {'total_cost_usd': 0.0}  # Default cost data

                    try:
                        instance_details = compute.instances().get(project=PROJECT_ID, zone=zone,
                                                                   instance=vm_id).execute()
                        machine_type_full_url = instance_details.get('machineType', '')
                        if machine_type_full_url:
                            machine_type = machine_type_full_url.split('/')[-1]

                            # Get vCPU and Memory details for the machine type
                            machine_type_details = compute.machineTypes().get(project=PROJECT_ID, zone=zone,
                                                                              machineType=machine_type).execute()
                            cpu_cores = machine_type_details.get('guestCpus')
                            memory_gb = machine_type_details.get('memoryMb', 0) / 1024

                            # Prepare config for the accurate cost function
                            cost_config = {
                                'machine_type': machine_type,
                                'region': zone.rsplit('-', 1)[0],  # Extract region from zone
                                'cpu_cores': cpu_cores,
                                'memory_gb': memory_gb
                            }
                            # Call the accurate, Billing API-based cost function
                            monthly_cost = get_resource_cost('vm', cost_config)
                            cost_data['total_cost_usd'] = monthly_cost

                    except Exception as e:
                        print(f"    ⚠️ Could not fetch details or cost for VM {vm_id}: {e}")
                    # === END of REPLACEMENT for VM cost calculation ===

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

                            # === START of REPLACEMENT for Disk cost calculation ===
                            cost_config = {
                                'disk_type': disk_type,
                                'size_gb': size_gb,
                                'region': zone_name.rsplit('-', 1)[0]  # Extract region from zone
                            }
                            # Call the accurate, Billing API-based cost function
                            monthly_cost = get_resource_cost('disk', cost_config)
                            cost_data = {'total_cost_usd': monthly_cost}
                            # === END of REPLACEMENT for Disk cost calculation ===

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

def save_optimization_report(candidates):
    """
    Generates and saves MongoDB-ready resource records from a pre-compiled dictionary of candidates.
    This function is responsible for report generation ONLY and does not perform any analysis.
    """
    print(f"\n💾 Generating MongoDB-Ready Resource Records...")
    print("=" * 60)

    mongodb_records = []
    category_counts = {}

    # --- Unified Processing Loop ---
    # Iterates through all categories of findings passed in the 'candidates' dictionary.
    for category, items in candidates.items():
        if not items:
            category_counts[category] = 0
            continue  # Skip empty categories

        category_counts[category] = len(items)

        if category == "advanced_cloud_run":
            # This category has a unique structure, so it needs special handling.
            for item in items:
                for finding in item.get("findings", []):
                    # Create a distinct metadata record for each individual finding.
                    metadata = extract_resource_metadata(
                        labels={},  # Labels are not readily available in this specific analysis
                        resource_name=item['name'],
                        resource_type='cloud_run',  # CORRECTED: Was 'cluster'
                        region=item['location'],
                        full_name=f"//run.googleapis.com/projects/{PROJECT_ID}/locations/{item['location']}/services/{item['name']}",
                        status="RUNNING",
                        cost_analysis={"total_cost_usd": finding.get("monthly_savings", 0.0)},
                        utilization_data={'finding_type': finding['type'], 'description': finding['description']},
                        is_orphaned=False
                    )
                    metadata['Finding'] = finding['type']
                    metadata['Recommendation'] = finding['recommendation']
                    mongodb_records.append(metadata)
        else:
            # This handles all other standard resource types.
            for item in items:
                if "resource_metadata" in item:
                    mongodb_records.append(item["resource_metadata"])

    # --- Save the consolidated records to the JSON file ---
    output_file = "gcp_optimization.json"
    try:
        if os.path.exists(output_file):
            print(f"  📝 Replacing existing report file: {output_file}")
        else:
            print(f"  📝 Creating new report file: {output_file}")

        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(mongodb_records, f, indent=2, default=str, ensure_ascii=False)

        print(f"  ✅ MongoDB-ready records saved to: {output_file}")
        print(f"  📊 Total actionable records generated: {len(mongodb_records)}")

        # --- Print a dynamic breakdown by resource type ---
        print(f"\n📋 Resource Breakdown:")
        for category, count in category_counts.items():
            # Make category names more readable for the report
            friendly_name = category.replace('_', ' ').title()
            print(f"  • {friendly_name}: {count}")
        print(f"  • Total Records: {len(mongodb_records)}")

        # --- Calculate total potential savings from the final records list ---
        total_potential_savings = sum(
            record.get("cost_analysis", {}).get("total_cost_usd", 0)
            for record in mongodb_records if record.get("cost_analysis")
        )
        print(f"  💰 Total estimated monthly savings from all findings: ${total_potential_savings:.2f} USD")

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
        'cluster': 'Compute',
        'persistent_volume': 'Storage',
        'cloud_run': 'Compute',
        'instance_group': 'Compute',
        'container': 'Compute'  # <-- ADD THIS LINE
    }

    # Determine SubResourceType - actual GCP resource types
    sub_resource_mapping = {
        'vm': 'Virtual Machine',
        'bucket': 'Bucket',
        'disk': 'Disk',
        'subnet': 'Subnet',
        'snapshot': 'Snapshot',
        'cluster': 'GKE Cluster',
        'persistent_volume': 'Persistent Volume',
        'cloud_run': 'Cloud Run Service',
        'instance_group': 'Instance Group',
        'container': 'Container Image'  # <-- ADD THIS LINE
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
    try:
        # Step 1: Initialize Billing API and fetch custom thresholds from MongoDB
        initialize_billing_info()
        thresholds = get_thresholds_from_mongodb(USER_EMAIL)

        print("\n🏁 Starting GCP resource analysis... This may take several minutes.")
        print("=" * 80)

        # Step 2: Run ALL analyses ONCE and collect all results.
        all_candidates = collect_optimization_candidates(PROJECT_ID, gcp_credentials, thresholds)

        # Call the other analysis functions
        all_candidates["inactive_cloud_run"] = categorize_gcp_cloud_run(PROJECT_ID, gcp_credentials, thresholds)
        # Use the NEW enhanced function for instance groups
        all_candidates["underutilized_instance_groups"] = categorize_gcp_instance_groups(PROJECT_ID, gcp_credentials,
                                                                                         thresholds)
        all_candidates["advanced_cloud_run"] = analyze_cloud_run_optimization_opportunities(PROJECT_ID, gcp_credentials)

        # NEW: Call the quota analysis function
        all_candidates["high_usage_quotas"] = analyze_gcp_resource_quotas(PROJECT_ID, gcp_credentials, thresholds)

        # NEW: Call the container image analysis function
        all_candidates["inefficient_base_images"] = analyze_gke_container_images(PROJECT_ID, gcp_credentials)

        # Step 3: Generate the final JSON report.
        save_optimization_report(all_candidates)

        # Step 4: (Optional) Insert the generated JSON report into MongoDB.
        print("\n💾 Inserting records into MongoDB...")
        with open("gcp_optimization.json", 'r', encoding='utf-8') as f:
            records_to_insert = json.load(f)
        insert_to_mongodb(records_to_insert)

        print("\n" + "=" * 80)
        print("✅ Analysis Complete! Check 'gcp_optimization.json' for the full report.")
        print("=" * 80)

    except Exception as e:
        print(f"\n❌ A critical error occurred during the main execution: {e}")
    except KeyboardInterrupt:
        print("\n⚠️ Analysis interrupted by user.")
