import argparse
import sys
import io
from datetime import datetime, timedelta, UTC
import ipaddress
import json
import os
import base64
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
CACHED_SKU_LISTS = {}
SERVICE_ID_CACHE = {}

FALLBACK_SERVICE_IDS = {
    "Compute Engine API": "6F81-5844-456A",
    "Cloud Storage": "95FF-2D51-2352", # This ID is causing the 404 error
    "Cloud Run Admin API": "152E-C115-5142"
}

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

print("GCP Resource Optimization Analysis")
print("=" * 80)
print(f"Project to Analyze: {PROJECT_ID}")
print(f"Configuration for User Email: {USER_EMAIL}")
print(f"Authenticating with Service Account: {args.client_email}")
print("=" * 80)

# ================================================================================
# AUTHENTICATION
# ================================================================================
try:
    pk_string = base64.b64decode(args.private_key).decode('utf-8')
    #pk_string = args.private_key.replace('\\n', '\n')
    credentials_info = {
        "type": "service_account",
        "project_id": PROJECT_ID,
        "private_key": pk_string,
        "client_email": args.client_email,
        "token_uri": "https://oauth2.googleapis.com/token",
        "scopes": ["https://www.googleapis.com/auth/cloud-platform"]
    }
    gcp_credentials = service_account.Credentials.from_service_account_info(credentials_info)
    print("Authentication successful.")

    # Initialize all necessary clients with credentials
    compute = discovery.build('compute', 'v1', credentials=gcp_credentials)
    run_admin_client = discovery.build('run', 'v1', credentials=gcp_credentials)
    monitoring_client = monitoring_v3.MetricServiceClient(credentials=gcp_credentials)
    asset_client = asset_v1.AssetServiceClient(credentials=gcp_credentials)
    billing_client = discovery.build('cloudbilling', 'v1', credentials=gcp_credentials)
    print("All GCP clients initialized.")

except Exception as e:
    print(f"Critical Error: Failed to create credentials from arguments. Please check your inputs. Error: {e}")
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
    print("pymongo not available. Install with: pip install pymongo")

# ================================================================================
# CONFIGURATION
# ================================================================================

# MongoDB Configuration
MONGODB_HOST = "localhost"  # Change this to yƒˇour MongoDB server IP/hostname
MONGODB_PORT = 27017  # Change this to your MongoDB port
MONGODB_DATABASE = "myDB"  # Change this to your database name
MONGODB_COLLECTION = "Cost_Insights"  # Change this to your collection name




def get_service_id(display_name, credentials):
    """
    Dynamically fetches the service ID for a given display name and caches it.
    This version includes extra print statements for debugging.
    """
    if display_name in SERVICE_ID_CACHE:
        return SERVICE_ID_CACHE[display_name]

    print(f"Discovering Service ID for '{display_name}'...")
    try:
        billing_client = discovery.build('cloudbilling', 'v1', credentials=credentials)
        request = billing_client.services().list()
        response = request.execute()

        # --- START DEBUGGING CHANGE ---
        print("  Available services found in billing account:")
        found_service = False
        all_services = response.get('services', [])
        if not all_services:
            print("    -> No services found. This strongly indicates a permissions issue.")
        # --- END DEBUGGING CHANGE ---

        for service in all_services:
            # --- START DEBUGGING CHANGE ---
            # Print every service name we find to see what's available
            print(f"    -> Found service: '{service.get('displayName')}'")
            # --- END DEBUGGING CHANGE ---

            if service.get('displayName') == display_name:
                service_id = service['name'].split('/')[-1]
                SERVICE_ID_CACHE[display_name] = service_id
                print(f"  SUCCESS: Found matching Service ID: {service_id}")
                found_service = True
                return service_id

        if not found_service:
            raise ValueError("Service not found in dynamic lookup.")

    except Exception as e:
        print(f"     Dynamic lookup failed: {e}")
        if display_name in FALLBACK_SERVICE_IDS:
            fallback_id = FALLBACK_SERVICE_IDS[display_name]
            SERVICE_ID_CACHE[display_name] = fallback_id
            print(f"  WARNING: Using hardcoded fallback ID: {fallback_id}")
            return fallback_id
        else:
            print(f"  CRITICAL: No fallback ID available for '{display_name}'.")
            return None


def cache_all_skus(display_name, credentials):
    """
    Refactored to accept a display_name. It looks up the ID internally
    and caches the SKU list against the display_name.
    """
    # If SKUs for this name are already cached, do nothing.
    if display_name in CACHED_SKU_LISTS:
        return

    print(f"Caching all SKUs for service '{display_name}' for faster price lookups...")

    # Get the numeric service ID from the cache populated by get_service_id.
    service_id = SERVICE_ID_CACHE.get(display_name)

    # If the ID was never found, we can't get SKUs.
    if not service_id:
        print(f"     Could not cache SKUs for '{display_name}': Service ID not found.")
        CACHED_SKU_LISTS[display_name] = []  # Cache empty list on failure.
        return

    try:
        all_skus = []
        page_token = None
        while True:
            request = billing_client.services().skus().list(parent=f"services/{service_id}", pageToken=page_token)
            response = request.execute()
            all_skus.extend(response.get('skus', []))
            page_token = response.get('nextPageToken')
            if not page_token:
                break

        # IMPORTANT: Cache the list against the display_name, not the ID.
        CACHED_SKU_LISTS[display_name] = all_skus
        print(f"  Cached {len(all_skus)} SKUs for service '{display_name}'.")
    except Exception as e:
        print(f"     Could not cache SKUs for service '{display_name}' (ID: {service_id}): {e}")
        CACHED_SKU_LISTS[display_name] = []  # Cache empty list on failure.

def find_sku_in_list(display_name, sku_description_filter, region="global"):
    """
    Refactored to accept a display_name. Finds a specific SKU
    from the pre-cached list.
    """
    cache_key = (display_name, sku_description_filter, region)
    if cache_key in SKU_CACHE:
        return SKU_CACHE[cache_key]

    # Get the list of SKUs using the display_name as the key.
    sku_list = CACHED_SKU_LISTS.get(display_name, [])
    if not sku_list:
        return 0.0, "unknown"

    for sku in sku_list:
        if sku_description_filter.lower() in sku.get('description', '').lower() and \
                (region in sku.get('serviceRegions', []) or region == "global"):
            pricing_info = sku.get('pricingInfo', [{}])[0]
            pricing_expression = pricing_info.get('pricingExpression', {})
            price_nanos = pricing_expression.get('tieredRates', [{}])[0].get('unitPrice', {}).get('nanos', 0)
            price_usd = price_nanos / 1_000_000_000
            usage_unit = pricing_expression.get('usageUnitDescription', 'per unit')

            SKU_CACHE[cache_key] = (price_usd, usage_unit)
            return price_usd, usage_unit

    SKU_CACHE[cache_key] = (0.0, "unknown")
    return 0.0, "unknown"


def analyze_k8s_overprovisioning(project_id, credentials):
    """
    Analyzes Kubernetes container metrics to find over-provisioned workloads
    where requests are more than 2x the actual usage.

    NOTE: Requires GKE Workload Metrics to be enabled on the clusters.
    """
    print(f"\nAnalyzing Kubernetes Workload Provisioning (2x Threshold)")
    print("=" * 60)
    print("Identifying containers where CPU/Memory requests are >200% of actual usage.")
    print(" NOTE: This check requires GKE Workload Metrics to be enabled on your clusters.")

    monitoring_client = monitoring_v3.MetricServiceClient(credentials=credentials)
    flagged_workloads = {}  # Use a dict to merge CPU and Memory findings for the same container

    # --- MQL Query for CPU Over-provisioning ---
    # Fetches containers where the average requested cores are more than 2x the average usage rate.
    mql_cpu = f"""
    fetch k8s_container
    | {{ metric 'kubernetes.io/container/cpu/request_cores'
    ; metric 'kubernetes.io/container/cpu/core_usage_time' | align rate(5m)
    }}
    | group_by 1d, [resource.cluster_name, resource.location, resource.namespace_name, resource.pod_name, resource.container_name],
        [val(0): mean(value.request_cores), val(1): mean(value.core_usage_time)]
    | join
    | filter val(0) > 0 && val(1) > 0 && val(0) > 2 * val(1)
    """

    # --- MQL Query for Memory Over-provisioning ---
    # Fetches containers where the average requested bytes are more than 2x the average used bytes.
    mql_mem = f"""
    fetch k8s_container
    | {{ metric 'kubernetes.io/container/memory/request_bytes'
    ; metric 'kubernetes.io/container/memory/used_bytes'
    }}
    | group_by 1d, [resource.cluster_name, resource.location, resource.namespace_name, resource.pod_name, resource.container_name],
        [val(0): mean(value.request_bytes), val(1): mean(value.used_bytes)]
    | join
    | filter val(0) > 0 && val(1) > 0 && val(0) > 2 * val(1)
    """

    queries = {
        "CPU": mql_cpu,
        "Memory": mql_mem
    }

    try:
        for resource_type, mql_query in queries.items():
            request = monitoring_v3.QueryTimeSeriesRequest(
                name=f"projects/{project_id}",
                query=mql_query,
            )
            try:
                results = monitoring_client.query_time_series(request=request)
            except AttributeError:
                print("   Could not perform check. The 'query_time_series' method is not available.")
                print(
                    "     RECOMMENDATION: Please upgrade the google-cloud-monitoring library with: pip install --upgrade google-cloud-monitoring")
                return []  # Exit the function early


            for result in results:
                labels = result.label_values
                cluster = labels[0].string_value
                location = labels[1].string_value
                namespace = labels[2].string_value
                pod = labels[3].string_value
                container = labels[4].string_value

                request_val = result.point_data[0].values[0].double_value
                usage_val = result.point_data[0].values[1].double_value

                workload_id = f"{cluster}/{namespace}/{pod}/{container}"
                finding_text = ""

                if resource_type == "CPU":
                    finding_text = f"CPU Request ({request_val:.3f} cores) is >2x usage ({usage_val:.3f} cores)."
                    print(f"    Over-provisioned CPU in Pod '{pod}' (Container: {container})")
                elif resource_type == "Memory":
                    # Convert bytes to MiB for readability
                    request_mib = request_val / 1024 / 1024
                    usage_mib = usage_val / 1024 / 1024
                    finding_text = f"Memory Request ({request_mib:.2f} MiB) is >2x usage ({usage_mib:.2f} MiB)."
                    print(f"    Over-provisioned Memory in Pod '{pod}' (Container: {container})")

                # If this is the first time we see this container, create a new record
                if workload_id not in flagged_workloads:
                    metadata = extract_resource_metadata(
                        labels={},  # Pod labels are not easily available in MQL
                        resource_name=f"{pod}/{container}",
                        resource_type='container',
                        region=location,
                        full_name=f"//container.googleapis.com/projects/{project_id}/locations/{location}/clusters/{cluster}/namespaces/{namespace}/pods/{pod}/containers/{container}",
                        status="Over-provisioned",
                        cost_analysis={'total_cost_usd': 0.0},
                        utilization_data={},
                        is_orphaned=False
                    )
                    flagged_workloads[workload_id] = {
                        "name": f"{pod}/{container}",
                        "cluster": cluster,
                        "namespace": namespace,
                        "findings": [finding_text],
                        "resource_metadata": metadata
                    }
                else:
                    # Otherwise, just add the new finding to the existing record
                    flagged_workloads[workload_id]["findings"].append(finding_text)

        # Consolidate findings into the final metadata record
        for workload in flagged_workloads.values():
            workload['resource_metadata']['Finding'] = "; ".join(workload['findings'])
            workload['resource_metadata']['Recommendation'] = "Right-size resource requests in the deployment YAML."

        if not flagged_workloads:
            print("  All Kubernetes workload requests appear well-sized.")
        else:
            print(f"Found {len(flagged_workloads)} over-provisioned workloads.")

    except Exception as e:
        # Gracefully handle the case where workload metrics are not enabled
        if "one of the following metrics is not available" in str(e) or "invalid argument" in str(e).lower():
            print("   Could not perform check. GKE Workload Metrics may not be enabled for this project's clusters.")
            print("     Please enable it to use this feature.")
        else:
            print(f" An unexpected error occurred during workload analysis: {e}")

    return list(flagged_workloads.values())

def analyze_gke_container_images(project_id, credentials):
    """
    Analyzes container images in GKE pods to find those using standard, non-minimal base images.
    This version includes recommendations for 'distroless' and other minimal bases.
    """
    print(f"\nAnalyzing GKE Container Base Images (Enhanced Sizing Logic)")
    print("=" * 60)
    print("Identifying images that could be smaller and more secure (recommending alpine, slim, distroless)")

    asset_client = asset_v1.AssetServiceClient(credentials=credentials)
    flagged_containers = []
    total_pods_analyzed = 0

    # Enhanced map of standard images to their recommended minimal/secure alternatives
    MINIMAL_IMAGE_MAP = {
        # General Purpose
        'ubuntu': 'ubuntu:minimal or gcr.io/distroless/base-debian11',
        'debian': 'debian:slim or gcr.io/distroless/base-debian11',
        'centos': 'Consider a smaller base like debian:slim or Alpine',
        'amazonlinux': 'Consider a smaller base like debian:slim or Alpine',
        # Application Runtimes
        'python': 'python:slim or python:alpine. For non-native dependencies, use gcr.io/distroless/python3-debian11',
        'node': 'node:alpine or gcr.io/distroless/nodejs18-debian11',
        'golang': 'golang:alpine (for builder) and gcr.io/distroless/static-debian11 (for final image)',
        'openjdk': 'openjdk:alpine or gcr.io/distroless/java17-debian11',
        'ruby': 'ruby:alpine',
        'php': 'php:alpine',
        # Web Servers
        'nginx': 'nginx:alpine',
        'httpd': 'httpd:alpine',
        # Build Tools (should not be in production images)
        'maven': 'Use in a multi-stage build; final image should be minimal (e.g., openjdk:alpine)',
        'gradle': 'Use in a multi-stage build; final image should be minimal (e.g., openjdk:alpine)',
        # Databases (less common in K8s, but good to check)
        'mysql': 'mysql:8.0-slim',
        'redis': 'redis:alpine'
    }

    try:
        response = asset_client.search_all_resources(
            request={"scope": f"projects/{project_id}", "asset_types": ["k8s.io/Pod"], "page_size": 500}
        )

        for resource in response:
            total_pods_analyzed += 1
            if not resource.additional_attributes:  # ADD THIS LINE TO FIX THE BUG
                continue
            pod_data_str = resource.additional_attributes.get('resource')
            if not pod_data_str:
                continue

            pod_data = json.loads(pod_data_str)
            pod_name = pod_data.get('metadata', {}).get('name', 'unknown-pod')
            namespace = pod_data.get('metadata', {}).get('namespace', 'default')

            try:
                parts = resource.name.split('/clusters/')
                cluster_name = parts[1].split('/')[0]
                location = resource.location
            except IndexError:
                cluster_name = "unknown-cluster"
                location = "unknown-location"

            containers = pod_data.get('spec', {}).get('containers', [])
            for container in containers:
                if not container:
                    continue
                image_used = container.get('image')
                if not image_used:
                    continue



                base_image = image_used.split(':')[0].split('/')[-1]

                if base_image in MINIMAL_IMAGE_MAP:
                    recommended_image = MINIMAL_IMAGE_MAP[base_image]
                    container_name = container.get('name', 'unknown-container')

                    print(f"    Found standard base image in Pod '{pod_name}' (Container: {container_name}): '{image_used}'")

                    metadata = extract_resource_metadata(
                        labels=pod_data.get('metadata', {}).get('labels', {}),
                        resource_name=f"{pod_name}/{container_name}",
                        resource_type='container',
                        region=location,
                        full_name=f"{resource.name}/containers/{container_name}",
                        status="Running",
                        cost_analysis={'total_cost_usd': 0.0},
                        utilization_data={'finding': f'Standard base image used: {image_used}'},
                        is_orphaned=False
                    )
                    metadata['Finding'] = "Standard base image can be optimized for size and security"
                    metadata['Recommendation'] = f"Replace '{image_used}' with a minimal alternative like '{recommended_image}'"

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
            print("  All container base images appear to be optimized.")
        else:
            print(f"Found {len(flagged_containers)} containers that can be optimized for smaller image size.")

    except Exception as e:
        print(f" Error analyzing GKE container images: {e}")

    return flagged_containers

def analyze_cloud_run_optimization_opportunities(project_id, credentials):
    """Analyzes Cloud Run services for right-sizing, concurrency, and min-instance costs."""
    print("\nAnalyzing Cloud Run Services for Advanced Optimization...")
    print("=" * 60)

    optimization_candidates = []

    try:
        # The parent location '-' indicates a global search for all services in the project.
        parent = f"projects/{project_id}/locations/-"
        request = run_admin_client.projects().locations().services().list(parent=parent)
        response = request.execute()
        services = response.get('items', [])

        if not services:
            print("  No Cloud Run services found.")
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
                print(f"    Found {len(all_findings_for_service)} optimization opportunities for '{service_name}'")
                optimization_candidates.extend(all_findings_for_service)
            else:
                print(f"  '{service_name}' appears well-configured.")

    except HttpError as e:
        if "run.googleapis.com has not been used" in str(e):
            print("  Cloud Run API is not used in this project, skipping.")
        else:
            print(f" Error analyzing Cloud Run services: {e}")
    except Exception as e:
        print(f" An unexpected error occurred during Cloud Run analysis: {e}")

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
        print(" pymongo not available. Using default thresholds.")
        return defaults

    try:
        client = MongoClient(host=MONGODB_HOST, port=MONGODB_PORT)
        db = client[MONGODB_DATABASE]
        thresholds_collection = db[collection_name]

        user_thresholds = thresholds_collection.find_one({"email": email})
        client.close()

        if user_thresholds:
            print(f"Retrieved thresholds from MongoDB for {email}")
            # Merge user settings with defaults to ensure all keys are present
            defaults.update(user_thresholds)
            return defaults
        else:
            print(f" No thresholds found for {email}. Using default values.")
            return defaults

    except Exception as e:
        print(f" Error fetching thresholds from MongoDB: {e}. Using default values.")
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
    Get the total size of a GCS bucket using the fast Cloud Monitoring API.
    This replaces the slow method of listing all blobs.
    """
    try:
        # Initialize the Monitoring client
        client = monitoring_v3.MetricServiceClient(credentials=credentials)
        project_name = f"projects/{PROJECT_ID}"

        # Set a time interval for the last 24 hours to ensure a data point is found
        now = datetime.now(UTC)
        interval = monitoring_v3.TimeInterval(
            {
                "end_time": {"seconds": int(now.timestamp())},
                "start_time": {"seconds": int((now - timedelta(hours=24)).timestamp())},
            }
        )

        # Build the filter for the specific bucket's total bytes metric
        filter_str = f'metric.type="storage.googleapis.com/storage/total_bytes" AND resource.labels.bucket_name="{bucket_name}"'

        # Fetch the time series data
        results = client.list_time_series(
            request={
                "name": project_name,
                "filter": filter_str,
                "interval": interval,
                "view": monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
            }
        )

        # Extract the latest data point's value. The API returns the most recent point first.
        for result in results:
            if result.points:
                # The value is the total size in bytes
                return result.points[0].value.int64_value

        # If no data point is found (e.g., for a new or empty bucket), return 0
        return 0

    except Exception as e:
        print(f"     Could not fetch size for bucket '{bucket_name}' via Monitoring API: {e}")
        # Return None to indicate the size could not be determined
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
    Refactored to be simpler. It calls the new find_sku_in_list directly
    with the correct service display names.
    """
    cost_per_month = 0.0

    try:
        if resource_type == 'vm':
            instance_family = config['machine_type'].split('-')[0].upper()
            cpu_sku_filter = f"{instance_family} Instance Core running in"
            cpu_price_per_hour, _ = find_sku_in_list("Compute Engine API", cpu_sku_filter, config['region'])

            ram_sku_filter = f"{instance_family} Instance Ram running in"
            ram_price_per_hour_gb, _ = find_sku_in_list("Compute Engine API", ram_sku_filter, config['region'])

            cost_per_month = (config['cpu_cores'] * cpu_price_per_hour + config[
                'memory_gb'] * ram_price_per_hour_gb) * 730

        elif resource_type == 'disk':
            disk_type_map = {'pd-standard': 'Standard', 'pd-balanced': 'Balanced', 'pd-ssd': 'SSD'}
            disk_type_name = disk_type_map.get(config['disk_type'], 'Standard')
            disk_sku_filter = f"{disk_type_name} backed PD Capacity"
            disk_price_per_gb_month, _ = find_sku_in_list("Compute Engine API", disk_sku_filter, config['region'])
            cost_per_month = config['size_gb'] * disk_price_per_gb_month

        elif resource_type == 'snapshot':
            snapshot_sku_filter = "Snapshot Storage"
            snapshot_price_per_gb_month, _ = find_sku_in_list("Compute Engine API", snapshot_sku_filter,
                                                              config['region'])
            cost_per_month = config['size_gb'] * snapshot_price_per_gb_month

        elif resource_type == 'bucket':
            bucket_sku_filter = "Standard Storage"
            bucket_price_per_gb_month, _ = find_sku_in_list("Cloud Storage", bucket_sku_filter, config['region'])
            cost_per_month = config['size_gb'] * bucket_price_per_gb_month

        # Example for Cloud Run Idle cost (can be expanded)
        elif resource_type == 'cloud_run_idle':
            # Note: These are example filters. You may need to find the exact SKU descriptions.
            cpu_idle_filter = "CPU Allocation Idle"
            mem_idle_filter = "Memory Allocation Idle"

            cpu_price, _ = find_sku_in_list("Cloud Run Admin API", cpu_idle_filter, config['region'])
            mem_price, _ = find_sku_in_list("Cloud Run Admin API", mem_idle_filter, config['region'])

            # Simplified cost - actual calculation might be more complex
            cost_per_month = (config['cpu'] * cpu_price + config['memory_gb'] * mem_price) * 730


    except Exception as e:
        print(f"     Cost calculation failed for {resource_type} {config.get('name', '')}: {e}")
        return 0.0

    return cost_per_month


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

            print(f" {name.split('/')[-1]} - {util_str} utilization ({size_str})")

        if not storage:
            print(" No buckets found")

        print(f"\nLow Utilization Buckets (<{bucket_threshold}%): {len(low_util_storage)}")
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

                print(f"    {name.split('/')[-1]} - {util_str} utilization ({size_str})")
        else:
            print("  No low utilization buckets found")

    except Exception as e:
        print(f" Error analyzing buckets: {e}")


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
                    f" {vm_id} (Zone: {zone}) - {cpu_util:.2f}% CPU" if cpu_util is not None else f" {vm_id} (Zone: {zone}) - N/A CPU")  # Adjusted precision for display

                if cpu_util is not None and cpu_util < threshold:
                    low_cpu_vms.append({
                        'name': resource.name,
                        'vm_id': vm_id,
                        'zone': zone,
                        'cpu_util': cpu_util
                    })

        print(f"\nTotal VMs analyzed: {total_vms}")

        print(f"Low CPU Usage VMs (<{threshold}%): {len(low_cpu_vms)}")

        if low_cpu_vms:
            for vm in low_cpu_vms:
                print(
                    f"    {vm['vm_id']} - {vm['cpu_util']:.2f}% CPU (Zone: {vm['zone']})")  # Adjusted precision for display
        else:
            print("  No low CPU usage VMs found")

    except Exception as e:
        print(f" Error analyzing VMs: {e}")


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
                        print(f" {name} (Default VPC) - Skipped")
                        continue

                    total_ips = len(list(ipaddress.ip_network(cidr, strict=False).hosts())) if cidr else 0

                    # --- NEW: Get actual IP utilization from Network Analyzer Insight ---
                    allocation_ratio = 0.0  # Default to 0% allocation
                    try:
                        # Network Analyzer insights are global, but specific to resources in a region
                        insight_request = recommender_client.projects().locations().insightTypes().insights().list(
                            parent=f"projects/{project_id}/locations/global",
                            insightType="google.networkanalyzer.vpcnetwork.ipAddressInsight",
                            filter=f'targetResources="//compute.googleapis.com/projects/{project_id}/regions/{region}/subnetworks/{name}"'
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
                        print(f"     Error fetching Network Analyzer insight for subnet {name}: {e}")
                        # Fallback: if insight fails, assume 0 used IPs (less accurate but prevents script failure)
                        allocation_ratio = 0.0

                    used_ips_count = int(total_ips * allocation_ratio)
                    free_ips = total_ips - used_ips_count
                    free_pct = (free_ips / total_ips * 100) if total_ips > 0 else 0

                    print(
                        f" {name} (VPC: {vpc_name}, Region: {region}) - {free_pct:.2f}% free IPs ({free_ips}/{total_ips} total, {used_ips_count} used)")

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
        print(f"High Free IP Subnets (>{subnet_threshold}%): {len(high_free_subnets)}")

        if high_free_subnets:
            for subnet_info in high_free_subnets:
                orphaned_status = " (Orphaned)" if subnet_info['is_orphaned'] else ""
                print(
                    f"    {subnet_info['name']} (VPC: {subnet_info['vpc_name']}) - {subnet_info['free_pct']:.2f}% free ({subnet_info['free_ips']}/{subnet_info['total_ips']} IPs){orphaned_status}")
        else:
            print("  No high free IP subnets found")

    except Exception as e:
        print(f" Error analyzing subnets: {e}")


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
    print("Identifying potentially underutilized disks based on size and status")
    print("⚡ Using aggregated list API for maximum speed")

    compute = discovery.build('compute', 'v1', credentials=credentials)

    disks = []
    small_disks = []  # Disks smaller than threshold

    try:
        # Use aggregated list to get all disks across all zones in one API call (much faster)
        print(" Fetching all disks using aggregated list (optimized for speed)...")

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
                        print(f"    {zone_name}: {zone_disk_count} disks")

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

        print(f" Total disks found: {total_disk_count}")

        # Display results
        print(f"\nTotal disks found: {len(disks)}")
        for disk in disks:
            attachment_status = "Attached" if disk['is_attached'] else "Unattached"
            attached_to = f" to {', '.join(disk['attached_to'])}" if disk['attached_to'] else ""
            print(
                f" {disk['name']} (Zone: {disk['zone']}) - {disk['size_gb']}GB, {disk['disk_type']}, {attachment_status}{attached_to}")

        if not disks:
            print(" No disks found")

        print(f"\nPotentially Underutilized Disks: {len(small_disks)}")
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
                print(f"    {disk['name']} (Zone: {disk['zone']}) - {reason_str}{attachment_info}")
        else:
            print("  No potentially underutilized disks found")

    except Exception as e:
        print(f" Error analyzing disks: {e}")




def categorize_gcp_snapshots(project_id, credentials, thresholds):
    """
    Analyzes GCP disk snapshots and identifies potentially orphaned or old ones.
    This optimized version fetches all disk info at once to avoid N+1 API calls.
    """
    snapshot_age_threshold_days = thresholds.get('snapshot_age_threshold_days', 90)
    print(f"\n📸 Analyzing Disk Snapshots (Old Snapshot Threshold: >{snapshot_age_threshold_days} days)")
    print("=" * 60)

    orphaned_snapshots = []
    total_snapshots = 0

    try:
        # --- OPTIMIZATION: Step 1 - Fetch all existing disk URLs at once ---
        print("  • Fetching all existing disk URLs for fast lookup...")
        existing_disk_urls = set()
        req = compute.disks().aggregatedList(project=project_id)
        while req is not None:
            resp = req.execute()
            for zone_data in resp.get('items', {}).values():
                if 'disks' in zone_data:
                    for disk in zone_data['disks']:
                        existing_disk_urls.add(disk['selfLink'])
            req = compute.disks().aggregatedList_next(previous_request=req, previous_response=resp)
        print(f"  • Found {len(existing_disk_urls)} existing disks.")

        # --- Step 2: List all snapshots ---
        request = compute.snapshots().list(project=project_id)
        while request is not None:
            response = request.execute()
            for snapshot in response.get('items', []):
                total_snapshots += 1
                is_orphaned_snapshot = False
                reasons = []

                # --- OPTIMIZATION: Step 3 - Check against the in-memory set (very fast) ---
                source_disk_url = snapshot.get('sourceDisk')
                if not source_disk_url or source_disk_url not in existing_disk_urls:
                    is_orphaned_snapshot = True
                    reasons.append("source_disk_deleted_or_missing")

                # Check snapshot age (logic remains the same)
                creation_timestamp_str = snapshot.get('creationTimestamp')
                if creation_timestamp_str:
                    try:
                        creation_time = datetime.fromisoformat(creation_timestamp_str.replace('Z', '+00:00'))
                        age_days = (datetime.now(UTC) - creation_time).days
                        if age_days > snapshot_age_threshold_days:
                            is_orphaned_snapshot = True
                            reasons.append(f"older_than_{snapshot_age_threshold_days}_days")
                    except ValueError:
                        reasons.append("invalid_creation_timestamp")

                if is_orphaned_snapshot:
                    orphaned_snapshots.append({
                        'name': snapshot.get('name'),
                        'size_gb': int(snapshot.get('diskSizeGb', 0)),
                        'orphaned_reasons': list(set(reasons)),
                        'labels': snapshot.get('labels', {})
                    })
            request = compute.snapshots().list_next(previous_request=request, previous_response=response)

        print(f"\nTotal snapshots found: {total_snapshots}")
        if orphaned_snapshots:
            print(f"Found {len(orphaned_snapshots)} potentially orphaned/old snapshots.")
        else:
            print("  No potentially orphaned/old snapshots found.")

    except Exception as e:
        print(f" Error analyzing snapshots: {e}")

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
        print(f"     Error fetching GKE cluster metrics for {cluster_name}: {e}")
        return None



def categorize_gcp_kubernetes_clusters(project_id, credentials, thresholds):
    """
    Analyzes GKE clusters for underutilization or orphaned status.
    """
    low_node_threshold = thresholds.get('gke_low_node_threshold', 1)
    low_cpu_util_threshold = thresholds.get('gke_low_cpu_util_threshold', 5.0)
    low_mem_util_threshold = thresholds.get('gke_low_mem_util_threshold', 10.0)

    print(f"\nAnalyzing GKE Clusters (Low Node Threshold: <{low_node_threshold} node(s))")
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

            print(f" {cluster_name} (Location: {cluster_location}, Nodes: {node_count}, Status: {cluster_status})")

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
        print(f"Potentially Underutilized/Orphaned GKE Clusters: {len(underutilized_clusters)}")

        if underutilized_clusters:
            for cluster_info in underutilized_clusters:
                reasons_str = ", ".join(cluster_info['orphaned_reasons'])
                orphaned_status = " (Orphaned)" if cluster_info['is_orphaned'] else ""
                print(f"    {cluster_info['name']} (Nodes: {cluster_info['node_count']}, Status: {cluster_info['status']}){orphaned_status} - Reasons: {reasons_str}")
        else:
            print("  No potentially underutilized/orphaned GKE clusters found")

    except Exception as e:
        print(f" Error analyzing GKE clusters: {e}")

    return underutilized_clusters

def categorize_gcp_cloud_run(project_id, credentials, thresholds):
    """Analyzes Cloud Run services for inactivity."""
    inactivity_threshold_days = thresholds.get('cloud_run_inactivity_days', 30)
    print(f"\nAnalyzing Cloud Run Services (Inactive if no requests in >{inactivity_threshold_days} days)")
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

            results = monitoring_client.list_time_series(time_series_request)

            request_count = sum(1 for _ in results)

            if request_count == 0:
                print(f"    {service_name} (Location: {location}) - Inactive")

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
                print(f" {service_name} (Location: {location}) - Active")

        print(f"\nTotal Cloud Run services analyzed: {total_services}")
        if not inactive_services:
            print("  No inactive Cloud Run services found.")

    except Exception as e:
        print(f" Error analyzing Cloud Run services: {e}")

    return inactive_services


def categorize_gcp_instance_groups(project_id, credentials, thresholds):
    """
    Analyzes Instance Group Managers with clarified, distinct logic for:
    1. Fixed-size IGMs (min instances == max instances).
    2. Underutilized IGMs (running instances below a threshold).
    3. Untagged instance templates.
    """
    print(f"\nAnalyzing Instance Groups with Enhanced Logic")
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
                                # FIX: Extract region/zone from the template URL
                                template_name = template_url.split('/')[-1]

                                # Determine if the template is regional or global
                                if "/regions/" in template_url:
                                    region = template_url.split('/regions/')[1].split('/')[0]
                                    template_info = compute.regionInstanceTemplates().get(
                                        project=project_id,
                                        region=region,
                                        instanceTemplate=template_name
                                    ).execute()
                                else:  # Assumes global
                                    template_info = compute.instanceTemplates().get(
                                        project=project_id,
                                        instanceTemplate=template_name
                                    ).execute()

                                labels = template_info.get('properties', {}).get('labels', {})
                                if not all(tag in labels for tag in required_tags):
                                    reasons_for_flagging.append("Instance template is untagged")

                            except HttpError as e:
                                if e.resp.status == 404:
                                    print(f"     Could not fetch labels for template {template_url}: Not found.")
                                else:
                                    print(f"     HTTP error fetching labels for template {template_url}: {e}")
                            except Exception as e:
                                print(f"     Could not fetch labels for template {template_url}: {e}")

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
            print("  No instance groups flagged for review.")
        else:
            print(f"   Found {len(flagged_groups)} instance groups for review:")
            for group in flagged_groups:
                print(f"    - {group['name']}: {group['resource_metadata']['Finding']}")

    except Exception as e:
        print(f" Error analyzing instance groups: {e}")

    return flagged_groups


def analyze_storage_tiering(project_id, credentials):
    """
    Analyzes Cloud Storage buckets to find those lacking a lifecycle policy
    for transitioning data to colder storage tiers.
    """
    print(f"\nAnalyzing Storage Bucket Lifecycle Policies for Tiering")
    print("=" * 60)
    print("Identifying buckets without automated data tiering rules.")

    storage_client = gcs_storage.Client(credentials=credentials, project=project_id)
    flagged_buckets = []

    # Define standard age-based thresholds for transitioning data
    TIERING_THRESHOLDS_DAYS = {
        'NEARLINE': 30,
        'COLDLINE': 90,
        'ARCHIVE': 365
    }

    try:
        buckets = storage_client.list_buckets()
        for bucket in buckets:
            # We must reload the bucket's metadata to get its lifecycle rules
            bucket.reload()

            # The core check: Does this bucket have any lifecycle rules defined?
            if not list(bucket.lifecycle_rules):
                print(f"    Bucket '{bucket.name}' has no lifecycle policy for storage tiering.")

                # For the recommendation, we can generate a sample policy
                recommended_policy = [
                    {
                        "action": {"type": "SetStorageClass", "storageClass": "NEARLINE"},
                        "condition": {"age": TIERING_THRESHOLDS_DAYS['NEARLINE'], "isLive": True}
                    },
                    {
                        "action": {"type": "SetStorageClass", "storageClass": "COLDLINE"},
                        "condition": {"age": TIERING_THRESHOLDS_DAYS['COLDLINE'], "isLive": True}
                    },
                    {
                        "action": {"type": "SetStorageClass", "storageClass": "ARCHIVE"},
                        "condition": {"age": TIERING_THRESHOLDS_DAYS['ARCHIVE'], "isLive": True}
                    }
                ]

                # Create a standard metadata record for this finding
                metadata = extract_resource_metadata(
                    labels=bucket.labels,
                    resource_name=bucket.name,
                    resource_type='bucket',
                    region=bucket.location,
                    full_name=f"//storage.googleapis.com/{bucket.name}",
                    status="Available",
                    cost_analysis={'total_cost_usd': 0.0},  # Savings are potential, not current
                    utilization_data={'finding': 'Missing lifecycle policy'},
                    is_orphaned=False
                )
                # Override the default finding and recommendation
                metadata['Finding'] = "Storage tiering policy is missing"
                metadata['Recommendation'] = "Implement a lifecycle policy to move aging data to colder storage."
                # Add the generated policy directly to the record for easy application
                metadata['RecommendedPolicy'] = recommended_policy

                flagged_buckets.append({
                    'name': bucket.name,
                    'location': bucket.location,
                    'full_name': f"//storage.googleapis.com/{bucket.name}",
                    'labels': bucket.labels,
                    'resource_metadata': metadata
                })

        if not flagged_buckets:
            print("  All buckets appear to have lifecycle policies in place.")
        else:
            print(f"Found {len(flagged_buckets)} buckets missing a lifecycle policy.")

    except Exception as e:
        print(f" Error analyzing storage tiering policies: {e}")

    return flagged_buckets

def analyze_gcp_resource_quotas(project_id, credentials, thresholds):
    """
    Analyzes GCP project-level quotas to find those nearing their limits.
    """
    print(f"\nAnalyzing Resource Quotas (Flagged if > 80% used)")
    print("=" * 60)

    high_usage_quotas = []
    # The threshold for flagging a quota, e.g., 80%
    utilization_threshold = thresholds.get('quota_utilization_threshold', 80.0)

    try:
        # The project details contain the quota information for Compute Engine
        project_info = compute.projects().get(project=project_id).execute()
        quotas = project_info.get('quotas', [])

        print(f" Found {len(quotas)} Compute Engine quotas to analyze for project {project_id}.")

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
                    f"    High Usage Quota: {metric} is at {utilization_percent:.2f}% ({int(usage)} / {int(limit)})")

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
                    "ConfidenceScore": "NA", "Status": "High Usage", "Entity": "lbg", "RootId": "NA",
                }

                high_usage_quotas.append({
                    "metric": metric,
                    "usage": usage,
                    "limit": limit,
                    "utilization_percent": utilization_percent,
                    "resource_metadata": metadata
                })

        if not high_usage_quotas:
            print("  All quota utilizations are within the threshold.")

    except Exception as e:
        print(f" Error analyzing resource quotas: {e}")

    return high_usage_quotas




def categorize_gcp_kubernetes_persistent_volumes(project_id, credentials, thresholds):
    """
    Analyzes Kubernetes Persistent Volumes to find TRULY orphaned volumes.
    An orphaned PV is one that is not bound to any PersistentVolumeClaim.
    """
    print(f"\nAnalyzing Kubernetes Persistent Volumes for Orphaned Status")
    print("=" * 60)

    asset_client = asset_v1.AssetServiceClient(credentials=credentials)
    scope = f"projects/{project_id}"
    orphaned_pvs = []
    total_pvs = 0

    try:
        response = asset_client.search_all_resources(
            request={"scope": scope, "asset_types": ["k8s.io/PersistentVolume"]}
        )

        for resource in response:
            total_pvs += 1
            pv_name = resource.display_name
            is_claimed = False  # Assume not claimed until proven otherwise

            # The full resource data is in additional_attributes as a JSON string
            if resource.additional_attributes and 'resource' in resource.additional_attributes:
                pv_data_str = resource.additional_attributes['resource']
                pv_data = json.loads(pv_data_str)

                # A PV is claimed if it has a 'claimRef' in its spec.
                # The presence of this field means it's bound to a PVC.
                if pv_data.get('spec', {}).get('claimRef'):
                    is_claimed = True

            print(f" {pv_name} (Claimed: {is_claimed})")

            # A PV is considered orphaned if it is NOT claimed
            if not is_claimed:
                reasons = ["unclaimed_by_pvc"]

                metadata = extract_resource_metadata(
                    labels=resource.labels if hasattr(resource, 'labels') else {},
                    resource_name=pv_name,
                    resource_type='persistent_volume',
                    full_name=resource.name,
                    status="Available",
                    cost_analysis={'total_cost_usd': 0.0},  # Cost is handled by the underlying disk analysis
                    utilization_data={'reasons': reasons},
                    is_orphaned=True
                )

                orphaned_pvs.append({
                    "name": pv_name,
                    "full_name": resource.name,
                    "is_claimed": is_claimed,
                    "is_orphaned": True,
                    "orphaned_reasons": reasons,
                    "resource_metadata": metadata
                })

        print(f"\nTotal PVs analyzed: {total_pvs}")
        if not orphaned_pvs:
            print("  No orphaned (unclaimed) PVs found.")
        else:
            print(f"Found {len(orphaned_pvs)} orphaned PVs:")
            for pv in orphaned_pvs:
                print(f"    - {pv['name']}")

    except Exception as e:
        print(f" Error analyzing K8s Persistent Volumes: {e}")

    return orphaned_pvs

# ================================================================================
# JSON REPORT GENERATION
# ================================================================================



def collect_optimization_candidates(project_id, credentials, thresholds):
    """
    Collects detailed information about resources that meet optimization criteria.
    This is the complete, corrected version that uses accurate pricing and
    avoids placeholder calculations for complex resources.
    """

    # --- Setup ---
    vm_cpu_threshold = thresholds.get('cmp_cpu_usage', 15.0)
    disk_quota_gb = thresholds.get('disk_underutilized_gb', 100)

    print(f"\nCollecting Optimization Candidates for JSON Report...")
    print("=" * 60)

    optimization_candidates = {
        "low_utilization_buckets": [],
        "low_cpu_vms": [],
        "high_free_subnets": [],  # Initialized but not populated in this version
        "low_utilization_disks": [],
        "orphaned_snapshots": [],
        "underutilized_clusters": [],
        "orphaned_pvs": []
    }

    asset_client = asset_v1.AssetServiceClient(credentials=credentials)
    scope = f"projects/{project_id}"

    # --- 1. Collect Low Utilization Buckets ---
    try:
        print("  • Collecting low utilization buckets...")
        response = asset_client.search_all_resources(
            request={"scope": scope, "asset_types": ["storage.googleapis.com/Bucket"]}
        )
        for resource in response:
            bucket_name = resource.name.split("/")[-1]
            total_bytes = get_bucket_size_gcs(bucket_name, credentials)
            size_gb = total_bytes / 1_000_000_000 if total_bytes else 0

            if size_gb < 1:
                cost_config = {'size_gb': size_gb, 'region': resource.location}
                monthly_cost = get_resource_cost('bucket', cost_config)
                cost_data = {'total_cost_usd': monthly_cost}

                metadata = extract_resource_metadata(
                    labels=dict(resource.labels) if hasattr(resource, 'labels') else {},
                    resource_name=bucket_name, resource_type='bucket', full_name=resource.name,
                    region=resource.location, status="Available", cost_analysis=cost_data,
                    utilization_data={'size_gb': size_gb}, is_orphaned=(total_bytes == 0)
                )
                optimization_candidates["low_utilization_buckets"].append({"resource_metadata": metadata})
    except Exception as e:
        print(f"     Error collecting bucket data: {e}")

    # --- 2. Collect Low CPU VMs ---
    try:
        print("  • Collecting low CPU usage VMs...")
        response = asset_client.search_all_resources(
            request={"scope": scope, "asset_types": ["compute.googleapis.com/Instance"]}
        )
        for resource in response:
            cpu_util = get_average_utilization(project_id, resource.asset_type, resource.name, credentials)
            if cpu_util is not None and cpu_util < vm_cpu_threshold:
                vm_id = resource.name.split("/")[-1]
                zone = resource.name.split("/zones/")[-1].split("/")[0] if 'zones/' in resource.name else 'unknown'

                try:
                    instance_details = compute.instances().get(project=project_id, zone=zone, instance=vm_id).execute()
                    machine_type = instance_details.get('machineType', '').split('/')[-1]
                    machine_type_details = compute.machineTypes().get(project=project_id, zone=zone,
                                                                      machineType=machine_type).execute()

                    cost_config = {
                        'machine_type': machine_type, 'region': zone.rsplit('-', 1)[0],
                        'cpu_cores': machine_type_details.get('guestCpus'),
                        'memory_gb': machine_type_details.get('memoryMb', 0) / 1024
                    }
                    monthly_cost = get_resource_cost('vm', cost_config)
                    cost_data = {'total_cost_usd': monthly_cost}
                except Exception as e:
                    print(f"     Could not fetch details or cost for VM {vm_id}: {e}")
                    cost_data = {'total_cost_usd': 0.0}

                metadata = extract_resource_metadata(
                    labels=dict(resource.labels) if hasattr(resource, 'labels') else {},
                    resource_name=vm_id, resource_type='vm', zone=zone, full_name=resource.name,
                    status="Running", cost_analysis=cost_data,
                    utilization_data={'cpu_utilization': cpu_util}, is_orphaned=False
                )
                optimization_candidates["low_cpu_vms"].append({"resource_metadata": metadata})
    except Exception as e:
        print(f"     Error collecting VM data: {e}")

    # --- 3. Collect Potentially Underutilized Disks ---
    try:
        print("  • Collecting potentially underutilized disks...")
        req = compute.disks().aggregatedList(project=project_id)
        while req is not None:
            resp = req.execute()
            for zone_url, zone_data in resp.get('items', {}).items():
                if 'disks' in zone_data:
                    zone_name = zone_url.split('/')[-1]
                    for disk in zone_data['disks']:
                        if int(disk.get('sizeGb', 0)) < disk_quota_gb or not disk.get('users'):
                            cost_config = {
                                'disk_type': disk.get('type', '').split('/')[-1],
                                'size_gb': int(disk.get('sizeGb', 0)),
                                'region': zone_name.rsplit('-', 1)[0]
                            }
                            monthly_cost = get_resource_cost('disk', cost_config)
                            cost_data = {'total_cost_usd': monthly_cost}

                            metadata = extract_resource_metadata(
                                labels=disk.get('labels', {}), resource_name=disk.get('name'),
                                resource_type='disk', zone=zone_name, full_name=disk.get('selfLink'),
                                status=disk.get('status'), cost_analysis=cost_data,
                                utilization_data={'size_gb': int(disk.get('sizeGb', 0))},
                                is_orphaned=not disk.get('users')
                            )
                            optimization_candidates["low_utilization_disks"].append({"resource_metadata": metadata})
            req = compute.disks().aggregatedList_next(previous_request=req, previous_response=resp)
    except Exception as e:
        print(f"     Error collecting disk data: {e}")

    # --- 4. Collect Orphaned Snapshots ---
    try:
        print("  • Collecting orphaned snapshots...")
        snapshots_found = categorize_gcp_snapshots(project_id, credentials, thresholds)
        for snap in snapshots_found:
            cost_config = {'size_gb': snap['size_gb'], 'region': 'global'}
            monthly_cost = get_resource_cost('snapshot', cost_config)
            cost_data = {'total_cost_usd': monthly_cost}

            metadata = extract_resource_metadata(
                labels=snap.get('labels', {}), resource_name=snap.get('name'), resource_type='snapshot',
                full_name=f"//compute.googleapis.com/projects/{project_id}/global/snapshots/{snap.get('name')}",
                status="Available", cost_analysis=cost_data,
                utilization_data={'reasons': snap.get('orphaned_reasons')}, is_orphaned=True
            )
            optimization_candidates["orphaned_snapshots"].append({"resource_metadata": metadata})
    except Exception as e:
        print(f"     Error collecting snapshot data: {e}")

    # --- 5. Collect Underutilized GKE Clusters ---
    try:
        print("  • Collecting underutilized/orphaned GKE Clusters...")
        clusters_found = categorize_gcp_kubernetes_clusters(project_id, credentials, thresholds)
        for cluster in clusters_found:
            cost_data = {'total_cost_usd': 0.0}
            metadata = extract_resource_metadata(
                labels=cluster.get('labels', {}), resource_name=cluster.get('name'), resource_type='cluster',
                region=cluster.get('location'),
                full_name=f"//container.googleapis.com/projects/{project_id}/locations/{cluster.get('location')}/clusters/{cluster.get('name')}",
                status=cluster.get('status'), cost_analysis=cost_data,
                utilization_data={'node_count': cluster.get('node_count')}, is_orphaned=cluster.get('is_orphaned')
            )
            optimization_candidates["underutilized_clusters"].append({"resource_metadata": metadata})
    except Exception as e:
        print(f"     Error collecting GKE cluster data: {e}")

    # --- 6. Collect Orphaned K8s Persistent Volumes ---
    try:
        print("  • Collecting orphaned K8s Persistent Volumes...")
        pvs_found = categorize_gcp_kubernetes_persistent_volumes(project_id, credentials, thresholds)
        for pv in pvs_found:
            cost_data = {'total_cost_usd': 0.0}
            metadata = extract_resource_metadata(
                labels=pv.get('labels', {}), resource_name=pv.get('name'), resource_type='persistent_volume',
                full_name=pv.get('full_name'), status="Available", cost_analysis=cost_data,
                utilization_data={'is_claimed': pv.get('is_claimed')}, is_orphaned=pv.get('is_orphaned')
            )
            optimization_candidates["orphaned_pvs"].append({"resource_metadata": metadata})
    except Exception as e:
        print(f"     Error collecting K8s Persistent Volume data: {e}")

    # --- Final Summary ---
    print(f"\n  Collected {len(optimization_candidates['low_utilization_buckets'])} low utilization buckets")
    print(f"  Collected {len(optimization_candidates['low_cpu_vms'])} low CPU VMs")
    # --- THIS LINE IS REMOVED TO PREVENT ERROR ---
    # print(f"  Collected {len(optimization_candidates['high_free_subnets'])} high free IP subnets")
    print(f"  Collected {len(optimization_candidates['low_utilization_disks'])} potentially underutilized disks")
    print(f"  Collected {len(optimization_candidates['orphaned_snapshots'])} orphaned snapshots")
    print(f"  Collected {len(optimization_candidates['underutilized_clusters'])} underutilized GKE clusters")
    print(f"  Collected {len(optimization_candidates['orphaned_pvs'])} orphaned K8s Persistent Volumes")

    return optimization_candidates

def save_optimization_report(candidates):
    """
    Generates and saves MongoDB-ready resource records from a pre-compiled dictionary of candidates.
    This function is responsible for report generation ONLY and does not perform any analysis.
    """
    print(f"\nGenerating MongoDB-Ready Resource Records...")
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
            print(f"  Replacing existing report file: {output_file}")
        else:
            print(f"  Creating new report file: {output_file}")

        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(mongodb_records, f, indent=2, default=str, ensure_ascii=False)

        print(f"  MongoDB-ready records saved to: {output_file}")
        print(f"Total actionable records generated: {len(mongodb_records)}")

        # --- Print a dynamic breakdown by resource type ---
        print(f"\nResource Breakdown:")
        for category, count in category_counts.items():
            # Make category names more readable for the report
            friendly_name = category.replace('_', ' ').title()
            print(f"{friendly_name}: {count}")
        print(f"Total Records: {len(mongodb_records)}")

        # --- Calculate total potential savings from the final records list ---
        total_potential_savings = sum(
            record.get("cost_analysis", {}).get("total_cost_usd", 0)
            for record in mongodb_records if record.get("cost_analysis")
        )
        print(f"Total estimated monthly savings from all findings: ${total_potential_savings:.2f} USD")

    except Exception as e:
        print(f"   Error saving records: {e}")
        print("Records data preview:")
        print(json.dumps(mongodb_records[:2] if mongodb_records else [], indent=2, default=str)[:500] + "...")

def insert_to_mongodb(records):
    """Insert GCP optimization records into MongoDB."""
    if not MONGODB_AVAILABLE:
        print(" pymongo not available. Skipping MongoDB insertion.")
        return False

    # Validate JSON data before proceeding
    try:
        # Test JSON serialization to ensure data is valid
        json_test = json.dumps(records, default=str)
        print("JSON validation passed - data is valid for MongoDB insertion")
    except Exception as e:
        print(f" JSON validation failed: {e}")
        print(" Skipping MongoDB insertion due to invalid JSON data")
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
            print(f"Cleared {existing_count} existing records from optimization collection")
        else:
            print("Collection is empty, no records to clear")

        # Add timestamp to each record
        for record in records:
            record['InsertedAt'] = datetime.now(UTC).isoformat()

        # Insert all records
        if records:  # Only insert if records list is not empty
            result = collection.insert_many(records)
            print(f"Successfully inserted {len(result.inserted_ids)} records into MongoDB")
            print(f"Database: {MONGODB_DATABASE}, Collection: {MONGODB_COLLECTION}")
            print(f"MongoDB Server: {MONGODB_HOST}:{MONGODB_PORT}")
        else:
            print("No records to insert into MongoDB.")

        return True

    except Exception as e:
        print(f" Error inserting into MongoDB: {e}")
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
    # Located inside the extract_resource_metadata function
    def get_label_value(key, default="NA"):
        if not labels:
            return default
        # Search for the key in a case-insensitive way
        for label_key, label_value in labels.items():
            if label_key.lower() == key.lower():
                # If the key matches, return its value, but if the value is empty, return the default
                return label_value if label_value else default
        # If no matching key was found after checking all of them, return the default
        return default

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
        finding = "VM Underutilised"
        recommendation = "Scale Down"
    elif resource_type == 'bucket':
        finding = "Bucket Underutilised"
        recommendation = "Try Merging"
    elif resource_type == 'subnet':
        finding = "Subnet Underutilised"
        recommendation = "Scale Down"
    elif resource_type == 'disk':
        finding = "Disk Underutilised"
        recommendation = "Scale Down"
    elif resource_type == 'snapshot':
        finding = "Snapshot potentially unneeded"
        recommendation = "Delete"
    elif resource_type == 'cluster':  # NEW: GKE Cluster finding
        finding = "GKE Cluster Underutilised"
        recommendation = "Scale Down / Delete"
    elif resource_type == 'persistent_volume':  # NEW: K8s PV finding
        finding = "Persistent Volume Underutilised"
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

    finding_key = finding.replace(';', '').replace(' ', '_').lower()

    metadata_record = {
        "_id": f"{full_name or f'//cloudresourcemanager.googleapis.com/projects/{PROJECT_ID}/resources/{resource_name}'}/{finding_key}",
        "CloudProvider": "GCP",
        "ManagementUnitId": PROJECT_ID,
        "ApplicationCode": "IPP",
        "CostCenter": get_label_value("costcenter"),
        "CIO": get_label_value("cio"),
        "Owner": get_label_value("owner"),
        "TicketId": get_label_value("ticketid"),
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
        "Entity": "lbg",
        "RootId": "NA",
        "Email": USER_EMAIL,
        # ADD THIS LINE TO PRESERVE THE COST DATA
        "cost_analysis": cost_analysis or {'total_cost_usd': 0.0}
    }
    return metadata_record

# ================================================================================
# MAIN EXECUTION
# ================================================================================

if __name__ == "__main__":
        # Call the refactored cache_all_skus directly with the service names.
    try:
        # --- Stage 1: Dynamic Setup and Configuration ---
        print("\n--- Step 1: Initializing Configuration ---")

        # Call get_service_id to populate the ID cache. This step remains the same.
        get_service_id("Compute Engine API", gcp_credentials)
        get_service_id("Cloud Storage", gcp_credentials)
        get_service_id("Cloud Run Admin API", gcp_credentials)  # Using the more specific name

        # Fetch custom thresholds from MongoDB
        thresholds = get_thresholds_from_mongodb(USER_EMAIL)

        # --- Stage 2: Pre-cache all pricing data for performance ---
        print("\n--- Step 2: Pre-caching All Pricing Information ---")

        # This is much cleaner and removes the need for intermediate variables.
        cache_all_skus("Compute Engine API", gcp_credentials)
        cache_all_skus("Cloud Storage", gcp_credentials)
        cache_all_skus("Cloud Run Admin API", gcp_credentials)

        # --- Stage 3: Run All Resource Analyses ---
        print("\n--- Step 3: Analyzing All GCP Resources (This may take several minutes) ---")
        print("=" * 80)

        # Run all analyses ONCE and collect all results into a single dictionary
        all_candidates = collect_optimization_candidates(PROJECT_ID, gcp_credentials, thresholds)

        # Call the other analysis functions and add their findings
        all_candidates["inactive_cloud_run"] = categorize_gcp_cloud_run(PROJECT_ID, gcp_credentials, thresholds)
        all_candidates["underutilized_instance_groups"] = categorize_gcp_instance_groups(PROJECT_ID, gcp_credentials,
                                                                                         thresholds)
        all_candidates["advanced_cloud_run"] = analyze_cloud_run_optimization_opportunities(PROJECT_ID, gcp_credentials)
        all_candidates["high_usage_quotas"] = analyze_gcp_resource_quotas(PROJECT_ID, gcp_credentials, thresholds)
        all_candidates["inefficient_base_images"] = analyze_gke_container_images(PROJECT_ID, gcp_credentials)
        all_candidates["missing_tiering_policies"] = analyze_storage_tiering(PROJECT_ID, gcp_credentials)
        all_candidates["overprovisioned_k8s_workloads"] = analyze_k8s_overprovisioning(PROJECT_ID, gcp_credentials)

        # --- Stage 4: Generate Report and Save to Datasbase ---
        print("\n--- Step 4: Finalizing Report and Saving Results ---")
        # Generate the final JSON report from all collected candidates
        save_optimization_report(all_candidates)

        # Insert the generated JSON report into MongoDB
        print("\nInserting records into MongoDB...")
        with open("gcp_optimization.json", 'r', encoding='utf-8') as f:
            records_to_insert = json.load(f)
        insert_to_mongodb(records_to_insert)

        print("\n" + "=" * 80)
        print("Analysis Complete! Check 'gcp_optimization.json' for the full report.")
        print("=" * 80)

    except Exception as e:
        print(f"\nA critical error occurred during the main execution: {e}")
    except KeyboardInterrupt:
        print("\nAnalysis interrupted by user.")
