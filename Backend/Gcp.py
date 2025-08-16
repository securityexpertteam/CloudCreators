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
from google.cloud import asset_v1, monitoring_v3, billing_v1 # Add billing_v1 here

# Ensure stdout is UTF-8 for proper printing
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

SKU_CACHE = {}
BILLING_SERVICE_NAME = ""
CACHED_SKU_LISTS = {}
SERVICE_ID_CACHE = {}
CLOUD_SQL_TIER_CACHE = {}

FALLBACK_SERVICE_IDS = {
    "Compute Engine API": "6F81-5844-456A",
    "Google Cloud Storage": "95FF-2EF5-5EA1",
    "Cloud Run Admin API": "152E-C115-5EA1",
    "Cloud SQL": "9662-B51E-5089",
    "Artifact Registry API": "A192-421F-40A3",
    "Cloud Logging": "5490-F7B7-8DF6",
    "Kubernetes Engine": "CCD8-9BF1-090E",
    "Networking": "E505-1604-58F8",
    "Secret Manager": "EE82-7A5E-871C",
    "VM Manager": "5E18-9A83-2867",
    "Cloud Monitoring": "58CD-E7C3-72CA",
    "Cloud Run": "152E-C115-5142"
}
resource_type_mapping = {
    'vm': 'Virtual Machine',
    'disk': 'Persistent Disk',
    'snapshot': 'Disk Snapshot',
    'bucket': 'Cloud Storage Bucket',
    'nic': 'Network Interface',
    'cluster': 'GKE Cluster',
    'load_balancer': 'Cloud Load Balancer', # ADD THIS LINE
    'persistent_volume': 'Kubernetes Persistent Volume',
    'cloud_run': 'Cloud Run Service',
    'cloud_sql': 'Database',
    'instance_group': 'Managed Instance Group',
    'container': 'Kubernetes Container'
}

sub_resource_mapping = {
    'vm': 'Instance',
    'disk': 'Disk',
    'snapshot': 'Snapshot',
    'bucket': 'Bucket',
    'nic': 'Interface',
    'cluster': 'Cluster',
    'load_balancer': 'Forwarding Rule', # ADD THIS LINE
    'persistent_volume': 'Persistent Volume',
    'cloud_run': 'Service',
    'cloud_sql': 'SQL Instance',
    'instance_group': 'Instance Group Manager',
    'container': 'Container'
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
    print("Authentication successful.")

    compute = discovery.build('compute', 'v1', credentials=gcp_credentials)
    monitoring_client = monitoring_v3.MetricServiceClient(credentials=gcp_credentials)
    asset_client = asset_v1.AssetServiceClient(credentials=gcp_credentials)
    billing_client = discovery.build('cloudbilling', 'v1', credentials=gcp_credentials)
    sql_client = discovery.build('sqladmin', 'v1beta4', credentials=gcp_credentials)
    recommender_client_service = discovery.build('recommender', 'v1', credentials=gcp_credentials)
    run_admin_client = discovery.build('run', 'v1', credentials=gcp_credentials) # <--- ADD THIS LINE
    artifact_registry_client = discovery.build('artifactregistry', 'v1', credentials=gcp_credentials)
    storage_client = gcs_storage.Client(credentials=gcp_credentials, project=PROJECT_ID)

    print("All GCP clients initialized.")

except Exception as e:
    print(f"Critical Error: Failed to create credentials from arguments. Please check your inputs. Error: {e}")
    exit(1)


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


def get_service_id(display_name, credentials, billing_account_id):
    """
    Dynamically fetches the service ID for a given display name from a specific billing account.
    """
    if not billing_account_id:
        print(f"  CRITICAL: Cannot get Service ID for '{display_name}' without a Billing Account ID.")
        return FALLBACK_SERVICE_IDS.get(display_name)

    cache_key = f"{billing_account_id}-{display_name}"
    if cache_key in SERVICE_ID_CACHE:
        return SERVICE_ID_CACHE[cache_key]

    print(f"Discovering Service ID for '{display_name}'...")
    try:
        billing_client = discovery.build('cloudbilling', 'v1', credentials=credentials)
        billing_account_name = f"billingAccounts/{billing_account_id}"
        request = billing_client.services().list(parent=billing_account_name)
        response = request.execute()

        for service in response.get('services', []):
            if service.get('displayName') == display_name:
                service_id = service['name'].split('/')[-1]
                SERVICE_ID_CACHE[cache_key] = service_id
                print(f"  SUCCESS: Found matching Service ID: {service_id}")
                return service_id

        raise ValueError("Service not found in dynamic lookup.")

    except Exception as e:
        print(f"     Dynamic lookup failed: {e}")
        if display_name in FALLBACK_SERVICE_IDS:
            fallback_id = FALLBACK_SERVICE_IDS[display_name]
            SERVICE_ID_CACHE[cache_key] = fallback_id
            print(f"  WARNING: Using hardcoded fallback ID: {fallback_id}")
            return fallback_id
        else:
            print(f"  CRITICAL: No fallback ID available for '{display_name}'.")
            return None


def cache_all_skus(display_name, credentials, billing_account_id):
    """
    Caches all SKUs for a given service from a specific billing account.
    """
    global CACHED_SKU_LISTS
    if not billing_account_id:
        print(f"  CRITICAL: Cannot cache SKUs for '{display_name}' without a Billing Account ID.")
        return

    print(f"Caching all SKUs for service '{display_name}'...")
    try:
        billing_client = discovery.build('cloudbilling', 'v1', credentials=credentials)
        service_id = get_service_id(display_name, credentials, billing_account_id)

        if not service_id:
            raise ValueError(f"Could not get a service ID for '{display_name}'")

        all_skus = []
        page_token = None
        while True:
            request = billing_client.services().skus().list(parent=f"services/{service_id}", pageToken=page_token)
            response = request.execute()
            all_skus.extend(response.get('skus', []))
            page_token = response.get('nextPageToken')
            if not page_token:
                break

        CACHED_SKU_LISTS[display_name] = all_skus
        print(f"  [DEBUG] Found {len(all_skus)} SKUs for service '{display_name}'")

    except Exception as e:
        print(f"  CRITICAL: Could not cache SKUs for service '{display_name}': {e}")

def find_sku_in_list(display_name, sku_description_filter, region="global"):
    """
    Refactored to accept a display_name. Finds a specific SKU
    from the pre-cached list. Includes enhanced debugging.
    """
    cache_key = (display_name, sku_description_filter, region)
    if cache_key in SKU_CACHE:
        return SKU_CACHE[cache_key]

    sku_list = CACHED_SKU_LISTS.get(display_name, [])
    if not sku_list:
        return 0.0, "unknown"

    # First, try for an exact match
    for sku in sku_list:
        if sku_description_filter.lower() in sku.get('description', '').lower() and \
                (region in sku.get('serviceRegions', []) or "global" in sku.get('serviceRegions', []) or not sku.get(
                    'serviceRegions')):
            pricing_info = sku.get('pricingInfo', [{}])[0]
            pricing_expression = pricing_info.get('pricingExpression', {})
            price_nanos = pricing_expression.get('tieredRates', [{}])[0].get('unitPrice', {}).get('nanos', 0)
            price_usd = price_nanos / 1_000_000_000
            usage_unit = pricing_expression.get('usageUnitDescription', 'per unit')

            SKU_CACHE[cache_key] = (price_usd, usage_unit)
            return price_usd, usage_unit

    # --- NEW: If no match is found, print debug suggestions ---
    print(f"     [DEBUG] SKU lookup failed for '{sku_description_filter}' in region '{region}'.")
    print(f"     [DEBUG] Searching for potential matches...")

    # Try to find partial matches to suggest a better filter
    suggestions = []
    search_terms = sku_description_filter.split()
    for sku in sku_list:
        description = sku.get('description', '').lower()
        if all(term.lower() in description for term in search_terms) and (region in sku.get('serviceRegions', [])):
            suggestions.append(sku.get('description'))

    if suggestions:
        print(
            f"     [DEBUG] Found {len(suggestions)} possible SKU matches. You may need to update the filter. Examples:")
        for s in suggestions[:5]:  # Print top 5 suggestions
            print(f"       -> {s}")
    else:
        print(f"     [DEBUG] No close matches found for '{sku_description_filter}'.")
    # --- END NEW ---

    SKU_CACHE[cache_key] = (0.0, "unknown")
    return 0.0, "unknown"


def cache_cloud_sql_tiers(project_id, credentials):
    """
    Fetches all available Cloud SQL tiers and caches their vCPU and RAM configurations.
    """
    global CLOUD_SQL_TIER_CACHE
    CLOUD_SQL_TIER_CACHE = {}
    print("  Caching all Cloud SQL tier configurations...")
    try:
        sqladmin = discovery.build('sqladmin', 'v1beta4', credentials=credentials)
        tiers = sqladmin.tiers().list(project=project_id).execute().get('items', [])

        for tier in tiers:
            tier_name = tier.get('tier')
            ram_gb = tier.get('Ram', 0) / 1024  # Convert MB to GB
            vcpus = 0

            # Dynamically determine vCPUs from standard tier naming conventions
            parts = tier_name.split('-')
            if len(parts) > 1:
                try:
                    # For tiers like 'db-n1-standard-8' or 'db-custom-4-16384', the last part is often the vCPU count
                    potential_vcpu = int(parts[-1])
                    if 'custom' in tier_name:
                        # For custom, the second to last part is vCPUs, e.g., db-custom-CPU-RAM
                        vcpus = int(parts[-2])
                    elif potential_vcpu > 0:
                        vcpus = potential_vcpu
                except (ValueError, IndexError):
                    # If parsing fails, we'll leave it as 0 and rely on cost calculation to handle it
                    pass

            CLOUD_SQL_TIER_CACHE[tier_name] = {'vcpus': vcpus, 'ram_gb': ram_gb}
        print("  SUCCESS: Cloud SQL tiers cached.")
    except Exception as e:
        print(f"  WARNING: Could not cache Cloud SQL tiers: {e}")


def has_scaling_activity(project_id, location, igm_name, credentials):
    """
    Checks if an Instance Group has had any change in size over the last 30 days.
    Returns True if there was activity, False otherwise.
    """
    try:
        end_time = datetime.now(UTC)
        start_time = end_time - timedelta(days=30)
        interval = monitoring_v3.TimeInterval(end_time=end_time, start_time=start_time)

        # MQL query to get the distinct count of instance group sizes over the period
        mql_query = f"""
        fetch gce_instance_group
        | metric 'compute.googleapis.com/instance_group/size'
        | filter resource.instance_group_name == '{igm_name}'
        | group_by 30d, [value: count_distinct(val())]
        """

        request = monitoring_v3.QueryTimeSeriesRequest(
            name=f"projects/{project_id}",
            query=mql_query,
        )
        results = monitoring_client.query_time_series(request=request)

        # If the result shows a distinct count of more than 1, it means the size changed.
        for result in results:
            for point in result.point_data:
                if point.values[0].int64_value > 1:
                    return True  # Scaling activity detected

    except Exception as e:
        print(f"     Warning: Could not fetch scaling activity for {igm_name}: {e}")
        # Default to False if metrics are unavailable, to be safe.
        return False

    return False  # No scaling activity


def get_project_billing_info(project_id, credentials):
    """
    Dynamically fetches the billing account ID linked to the specified project
    using the Google Cloud Billing API.
    """
    print("  Dynamically discovering linked Billing Account ID...")
    try:
        # Use the correct CloudBillingClient
        billing_client = billing_v1.CloudBillingClient(credentials=credentials)
        project_name = f"projects/{project_id}"

        # Call the get_project_billing_info method from the billing client
        response = billing_client.get_project_billing_info(name=project_name)

        billing_account_name = response.billing_account_name
        if billing_account_name:
            billing_account_id = billing_account_name.split('/')[-1]
            print(f"  SUCCESS: Found Billing Account ID: {billing_account_id}")
            return billing_account_id
        else:
            print("  WARNING: No billing account is linked to this project or billing is disabled.")
            return None
    except Exception as e:
        print(
            f"  CRITICAL: Could not determine billing account. Ensure the 'Cloud Billing API' is enabled for your project.")
        print(f"  Error: {e}")
        return None


# In Gcp.py, replace the existing analyze_vms function

def analyze_vms(project_id, credentials, thresholds):
    """
    Analyzes all GCE VMs for underutilization and missing tags.
    """
    print(f"\nAnalyzing VMs")
    print("=" * 60)

    asset_client = asset_v1.AssetServiceClient(credentials=credentials)
    compute = discovery.build('compute', 'v1', credentials=gcp_credentials)
    flagged_vms = []

    # --- FIX: Ensure thresholds are numbers (floats) to prevent crashes ---
    try:
        cpu_threshold = float(thresholds.get('cmp_cpu_usage', 15.0))
        mem_threshold = float(thresholds.get('cmp_memory_usage', 15.0))
    except (ValueError, TypeError):
        print("  [WARN] Could not parse thresholds from config. Using default values.")
        cpu_threshold = 15.0
        mem_threshold = 15.0
    # --- END FIX ---

    try:
        response = asset_client.search_all_resources(
            request={"scope": f"projects/{project_id}", "asset_types": ["compute.googleapis.com/Instance"]}
        )
        all_vms = list(response)

        if not all_vms:
            print("  No VM instances found.")
            return []

        for vm in all_vms:
            vm_name = vm.display_name
            instance_id = vm.name.split('/')[-1]
            zone = vm.location
            reasons = []
            recommendations = []

            cpu_util = None
            mem_util = None

            if not vm.labels:
                reasons.append("Untagged")
                recommendations.append("Add appropriate labels for cost tracking and resource management.")

            if vm.state == "RUNNING":
                cpu_util = get_average_utilization(project_id, vm.asset_type, vm.name, credentials)
                mem_util = get_vm_memory_utilization(project_id, instance_id, zone, credentials)

                util_parts = []
                if cpu_util is not None:
                    util_parts.append(f"CPU: {cpu_util:.2f}%")
                if mem_util is not None:
                    util_parts.append(f"Memory: {mem_util:.2f}%")

                # This comparison will now work correctly
                if (cpu_util is not None and cpu_util < cpu_threshold) or \
                        (mem_util is not None and mem_util < mem_threshold):
                    reason_str = f"VM Underutilized ({', '.join(util_parts)})"
                    reasons.append(reason_str)
                    recommendations.append("Scale Down")

            if reasons:
                final_reasons = "; ".join(reasons)
                print(f"  - Flagged VM: {vm_name} | Reasons: {final_reasons}")

                try:
                    instance_details = compute.instances().get(project=project_id, zone=zone,
                                                               instance=instance_id).execute()
                    machine_type = instance_details.get('machineType', '').split('/')[-1]
                    machine_type_details = compute.machineTypes().get(project=project_id, zone=zone,
                                                                      machineType=machine_type).execute()

                    cost_config = {
                        'machine_type': machine_type,
                        'region': zone.rsplit('-', 1)[0],
                        'cpu_cores': machine_type_details.get('guestCpus'),
                        'memory_gb': machine_type_details.get('memoryMb', 0) / 1024
                    }
                    cost = get_resource_cost('vm', cost_config)

                    metadata = extract_resource_metadata(
                        labels=vm.labels,
                        resource_name=vm_name,
                        resource_type='vm',
                        region=zone,
                        full_name=vm.name,
                        status=vm.state,
                        cost_analysis={'total_cost_usd': cost},
                        utilization_data={
                            'finding': final_reasons,
                            'recommendation': "; ".join(recommendations),
                            'cpu_utilization': cpu_util,
                            'memory_utilization': mem_util
                        },
                        is_orphaned=False
                    )
                    flagged_vms.append({"resource_metadata": metadata})

                except Exception as e:
                    print(f"     Could not get details or cost for VM {vm_name}: {e}")

    except Exception as e:
        print(f" Error during VM analysis: {e}")

    return flagged_vms

def analyze_reserved_ips(project_id, credentials, thresholds):
    """
    Analyzes reserved static external IP addresses to find any that are unused.
    """
    print(f"\nAnalyzing Unused Reserved IP Addresses")
    print("=" * 60)

    flagged_ips = []

    try:
        request = compute.addresses().aggregatedList(project=project_id)
        while request is not None:
            response = request.execute()
            for region_url, region_data in response.get('items', {}).items():
                if 'addresses' in region_data:
                    region = region_url.split('/')[-1]
                    for address in region_data['addresses']:
                        # An unused reserved IP has the status 'RESERVED'
                        if address.get('status') == 'RESERVED':
                            ip_name = address.get('name')
                            ip_address = address.get('address')

                            # Calculate the cost of this unused IP
                            cost_config = {'region': region}
                            monthly_cost = get_resource_cost('public_ip', cost_config)

                            print(
                                f"    - Flagged Unused IP: {ip_name} ({ip_address}) in {region} | Cost: ${monthly_cost:.2f}/mo")

                            metadata = extract_resource_metadata(
                                labels=address.get('labels', {}),
                                resource_name=ip_name,
                                resource_type='public_ip',
                                region=region,
                                full_name=address.get('selfLink'),
                                status="UNATTACHED",
                                cost_analysis={'total_cost_usd': monthly_cost},
                                utilization_data={
                                    'finding': "Unused Reserved IP Address",
                                    'recommendation': "Release the IP address if no longer needed",
                                    'ip_address': ip_address
                                },
                                is_orphaned=True
                            )
                            flagged_ips.append({"resource_metadata": metadata})

            request = compute.addresses().aggregatedList_next(previous_request=request, previous_response=response)

        print(f"\nFinished analysis. Found {len(flagged_ips)} unused reserved IP addresses.")

    except Exception as e:
        print(f" Error analyzing reserved IP addresses: {e}")

    return flagged_ips

def analyze_cloud_sql_untagged(project_id, credentials):
    """
    Analyzes Cloud SQL instances to find those missing required labels/tags.
    """
    print(f"\nAnalyzing Cloud SQL Instances for Missing Tags")
    print("=" * 60)
    print(
        "Identifying instances missing one of the required tags: 'features', 'lab', 'platform', 'cio', 'ticketid', 'environment'")

    sql_client = discovery.build('sqladmin', 'v1beta4', credentials=credentials)
    flagged_instances = []
    required_tags = ["features", "lab", "platform", "cio", "ticketid", "environment"]

    try:
        request = sql_client.instances().list(project=project_id)
        response = request.execute()

        if not response.get('items'):
            print("  No Cloud SQL instances found.")
            return []

        for instance in response['items']:
            instance_name = instance.get('name')
            instance_region = instance.get('region', 'global')
            labels = instance.get('settings', {}).get('userLabels', {})

            is_untagged = not all(tag in labels for tag in required_tags)

            if is_untagged:
                missing_tags = [tag for tag in required_tags if tag not in labels]
                print(
                    f"    Cloud SQL instance '{instance_name}' is missing the following tags: {', '.join(missing_tags)}")

                # --- NEW: Cloud SQL Cost Calculation ---
                instance_tier = instance.get('settings', {}).get('tier', 'unknown')
                storage_size_gb = int(instance.get('settings', {}).get('storageSize', 0))
                storage_type = instance.get('settings', {}).get('storageType', 'SSD')

                cost_config = {
                    'name': instance_name,
                    'tier': instance_tier,
                    'storage_size_gb': storage_size_gb,
                    'storage_type': storage_type,
                    'region': instance_region
                }
                monthly_cost = get_resource_cost('cloud_sql', cost_config)
                # --- END NEW: Cloud SQL Cost Calculation ---

                metadata = extract_resource_metadata(
                    labels=labels,
                    resource_name=instance_name,
                    resource_type='cloud_sql',
                    region=instance_region,
                    full_name=instance.get('selfLink', ''),
                    status=instance.get('state'),
                    cost_analysis={'total_cost_usd': monthly_cost},
                    utilization_data={'finding': "Untagged", 'missing_tags': missing_tags,
                                      'recommendation': "Apply tags"},
                    is_orphaned=False
                )

                flagged_instances.append({
                    'name': instance_name,
                    'region': instance_region,
                    'labels': labels,
                    'resource_metadata': metadata
                })

        if not flagged_instances:
            print("  All Cloud SQL instances have the required tags.")
        else:
            print(f"Found {len(flagged_instances)} untagged Cloud SQL instances.")

    except Exception as e:
        print(f" Error analyzing Cloud SQL instances for tags: {e}")

    return flagged_instances


def analyze_cloud_sql_read_replicas(project_id, credentials, thresholds):
    """
    Analyzes Cloud SQL read replicas to find any that are idle (no connections).
    """
    inactivity_threshold_days = thresholds.get('cloud_sql_inactivity_days', 30)
    print(f"\n⚡ Analyzing Cloud SQL Read Replicas for Inactivity (> {inactivity_threshold_days} days)")
    print("=" * 60)

    idle_replicas = []

    try:
        request = sql_client.instances().list(project=project_id)
        response = request.execute()
        all_instances = response.get('items', [])

        if not all_instances:
            print("  No Cloud SQL instances found.")
            return []

        for instance in all_instances:
            # We are only interested in read replicas
            if instance.get('instanceType') != 'READ_REPLICA_INSTANCE':
                continue

            instance_name = instance.get('name')
            instance_region = instance.get('region', 'global')
            db_version = instance.get('databaseVersion', '')

            # Determine the correct metric based on the database engine
            if 'POSTGRES' in db_version:
                metric_type = 'cloudsql.googleapis.com/database/postgresql/num_backends'
            elif 'MYSQL' in db_version:
                metric_type = 'cloudsql.googleapis.com/database/mysql/queries'
            else:
                continue  # Skip unsupported types for this check

            end_time = datetime.now(UTC)
            start_time = end_time - timedelta(days=inactivity_threshold_days)
            interval = monitoring_v3.TimeInterval(end_time=end_time, start_time=start_time)

            filter_str = f'metric.type="{metric_type}" AND resource.labels.database_id="{project_id}:{instance_name}"'

            request = monitoring_v3.ListTimeSeriesRequest(
                name=f"projects/{project_id}", filter=filter_str, interval=interval,
                view=monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.HEADERS
            )
            results = monitoring_client.list_time_series(request)

            # If the iterator is empty, there were no data points, meaning no activity
            has_activity = any(results)

            if not has_activity:
                # This replica is idle, so we flag it
                cost_config = {
                    'tier': instance.get('settings', {}).get('tier', 'unknown'),
                    'storage_size_gb': int(instance.get('settings', {}).get('dataDiskSizeGb', 0)),
                    'storage_type': instance.get('settings', {}).get('dataDiskType', 'PD_SSD'),
                    'region': instance_region
                }
                monthly_cost = get_resource_cost('cloud_sql', cost_config)

                print(f"    - Flagged Idle Read Replica: {instance_name} | Cost: ${monthly_cost:.2f}/mo")

                metadata = extract_resource_metadata(
                    labels=instance.get('settings', {}).get('userLabels', {}),
                    resource_name=instance_name,
                    resource_type='cloud_sql',
                    status="IDLE_REPLICA",
                    full_name=instance.get('selfLink', ''),
                    region=instance_region,
                    cost_analysis={'total_cost_usd': monthly_cost},
                    utilization_data={
                        'finding': "Idle Read Replica",
                        'recommendation': "Delete the read replica if it is no longer needed",
                        'details': f"No connections or queries detected in the last {inactivity_threshold_days} days."
                    },
                    is_orphaned=True
                )
                idle_replicas.append({"resource_metadata": metadata})

        print(f"\nFinished analysis. Found {len(idle_replicas)} idle read replicas.")

    except Exception as e:
        print(f" Error analyzing Cloud SQL read replicas: {e}")

    return idle_replicas

def analyze_disks(project_id, credentials, thresholds):
    """
    Analyzes Persistent Disks to find underutilized, orphaned, or untagged ones.
    """
    disk_quota_gb = thresholds.get('disk_underutilized_gb', 100)
    required_tags = thresholds.get('required_tags', ["features", "lab", "platform", "cio", "ticketid", "environment"])
    print(f"\nAnalyzing Persistent Disks (Flagged if < {disk_quota_gb}GB, Unattached, or Untagged)")
    print("=" * 60)

    flagged_disks = []
    try:
        req = compute.disks().aggregatedList(project=project_id)
        while req is not None:
            resp = req.execute()
            for zone_url, zone_data in resp.get('items', {}).items():
                if 'disks' in zone_data:
                    zone_name = zone_url.split('/')[-1]
                    for disk in zone_data['disks']:
                        finding_types = []
                        recommendations = []

                        size_gb = int(disk.get('sizeGb', 0))
                        labels = disk.get('labels', {})
                        is_orphaned_disk = not disk.get('users')

                        # 1. Check for Underutilization
                        if size_gb < disk_quota_gb:
                            finding_types.append("Disk Underutilized")
                            recommendations.append("Try Merging")

                        # 2. Check if Orphaned
                        if is_orphaned_disk:
                            finding_types.append("Orphaned Disk")
                            recommendations.append("Delete if no longer needed")

                        # 3. Check for Missing Tags
                        missing_tags = [tag for tag in required_tags if tag not in labels]
                        if missing_tags:
                            finding_types.append("Untagged")
                            recommendations.append("Apply required tags")

                        # Consolidate if any findings exist
                        if finding_types:
                            final_finding = "; ".join(sorted(list(set(finding_types))))
                            final_recommendation = "; ".join(sorted(list(set(recommendations))))

                            print(f"    - Flagged Disk: {disk.get('name')} | Reason: {final_finding}")

                            cost_config = {
                                'disk_type': disk.get('type', '').split('/')[-1],
                                'size_gb': size_gb,
                                'region': zone_name.rsplit('-', 1)[0]
                            }
                            monthly_cost = get_resource_cost('disk', cost_config)
                            cost_data = {'total_cost_usd': monthly_cost}

                            metadata = extract_resource_metadata(
                                labels=labels, resource_name=disk.get('name'),
                                resource_type='disk', region=zone_name.rsplit('-', 1)[0],
                                full_name=disk.get('selfLink'),
                                status=disk.get('status'), cost_analysis=cost_data,
                                utilization_data={
                                    'size_gb': size_gb,
                                    'finding': final_finding,
                                    'recommendation': final_recommendation,
                                    'missing_tags': missing_tags
                                },
                                is_orphaned=is_orphaned_disk
                            )
                            flagged_disks.append({"resource_metadata": metadata})
            req = compute.disks().aggregatedList_next(previous_request=req, previous_response=resp)

        print(f"  Finished analysis. Found {len(flagged_disks)} disks with optimization opportunities.")
    except Exception as e:
        print(f"     Error collecting disk data: {e}")
    return flagged_disks

def analyze_snapshots(project_id, credentials, thresholds):
    """
    Analyzes Snapshots to find old or orphaned ones.
    """
    snapshots_found = categorize_gcp_snapshots(project_id, credentials, thresholds)
    flagged_snapshots = []

    if not snapshots_found:
        return []

    print("\n  Analyzing details for flagged snapshots...")
    try:
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
            flagged_snapshots.append({"resource_metadata": metadata})

        print(f"  Finished analysis. Found {len(flagged_snapshots)} snapshots with optimization opportunities.")

    except Exception as e:
        print(f"     Error collecting snapshot data: {e}")

    return flagged_snapshots

def analyze_cloud_sql_underutilized(project_id, credentials, thresholds):
    """
    Analyzes Cloud SQL instances for underutilization based on disk usage.
    """
    underutilization_threshold = thresholds.get('cloud_sql_underutilization_percent', 10.0)
    print(f"\nAnalyzing Cloud SQL Instances (Disk Utilization Threshold: <{underutilization_threshold}%)")
    print("=" * 60)
    print("Identifying instances where disk usage is significantly lower than allocated size.")

    sql_client = discovery.build('sqladmin', 'v1beta4', credentials=credentials)
    monitoring_client = monitoring_v3.MetricServiceClient(credentials=credentials)
    flagged_instances = []

    end_time = datetime.now(UTC)
    start_time = end_time - timedelta(days=7)  # Look at the last 7 days of data
    interval = monitoring_v3.TimeInterval(end_time=end_time, start_time=start_time)

    try:
        instances = sql_client.instances().list(project=project_id).execute().get('items', [])

        if not instances:
            print("  No Cloud SQL instances found.")
            return []

        for instance in instances:
            instance_name = instance.get('name')
            instance_region = instance.get('region', 'global')
            allocated_size_gb = int(instance.get('settings', {}).get('storageSize', 0))

            # Fetch disk usage metric from Cloud Monitoring
            filter_str = f'metric.type="cloudsql.googleapis.com/database/disk/bytes_used" AND resource.labels.database_id="{project_id}:{instance_name}"'
            time_series_request = monitoring_v3.ListTimeSeriesRequest(
                name=f"projects/{project_id}",
                filter=filter_str,
                interval=interval,
                view=monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
            )

            used_bytes = 0
            count = 0
            for ts in monitoring_client.list_time_series(time_series_request):
                for point in ts.points:
                    used_bytes += point.value.int64_value
                    count += 1

            avg_used_gb = (used_bytes / count) / 1_000_000_000 if count > 0 else 0

            utilization_percent = (avg_used_gb / allocated_size_gb) * 100 if allocated_size_gb > 0 else 0

            print(
                f"    Cloud SQL '{instance_name}': {utilization_percent:.2f}% utilization ({avg_used_gb:.2f}GB used of {allocated_size_gb}GB allocated)")

            if utilization_percent < underutilization_threshold:
                # --- NEW: Cloud SQL Cost Calculation ---
                instance_tier = instance.get('settings', {}).get('tier', 'unknown')
                storage_size_gb = int(instance.get('settings', {}).get('storageSize', 0))
                storage_type = instance.get('settings', {}).get('storageType', 'SSD')

                cost_config = {
                    'name': instance_name,
                    'tier': instance_tier,
                    'storage_size_gb': storage_size_gb,
                    'storage_type': storage_type,
                    'region': instance_region
                }
                monthly_cost = get_resource_cost('cloud_sql', cost_config)
                # --- END NEW: Cloud SQL Cost Calculation ---

                metadata = extract_resource_metadata(
                    labels=instance.get('settings', {}).get('userLabels', {}),
                    resource_name=instance_name,
                    resource_type='cloud_sql',
                    region=instance_region,
                    full_name=instance.get('selfLink', ''),
                    status=instance.get('state'),
                    cost_analysis={'total_cost_usd': monthly_cost},
                    utilization_data={
                        'finding': f"DB Underutilized (disk usage < {underutilization_threshold}%)",
                        'recommendation': "Right-size the disk",
                        'allocated_size_gb': allocated_size_gb,
                        'used_size_gb': avg_used_gb,
                        'utilization_percent': utilization_percent
                    },
                    is_orphaned=False
                )

                flagged_instances.append({
                    'name': instance_name,
                    'resource_metadata': metadata
                })

        if not flagged_instances:
            print("  No underutilized Cloud SQL instances found.")
        else:
            print(f"Found {len(flagged_instances)} underutilized Cloud SQL instances.")

    except Exception as e:
        print(f" Error analyzing Cloud SQL instances for underutilization: {e}")
        return []

    return flagged_instances

def analyze_cloud_sql_orphaned(project_id, credentials, thresholds):
    """
    Analyzes Cloud SQL instances to find those that are orphaned (inactive).
    An instance is considered orphaned if there's no connection activity.
    """
    inactivity_threshold_days = thresholds.get('cloud_sql_inactivity_days', 30)
    print(f"\nAnalyzing Cloud SQL Instances for Orphaned Status (Inactivity > {inactivity_threshold_days} days)")
    print("=" * 60)
    print("Identifying instances with no connection activity over the specified period.")

    sql_client = discovery.build('sqladmin', 'v1beta4', credentials=credentials)
    monitoring_client = monitoring_v3.MetricServiceClient(credentials=credentials)
    orphaned_instances = []

    end_time = datetime.now(UTC)
    start_time = end_time - timedelta(days=inactivity_threshold_days)
    interval = monitoring_v3.TimeInterval(end_time=end_time, start_time=start_time)

    try:
        instances = sql_client.instances().list(project=project_id).execute().get('items', [])

        if not instances:
            print("  No Cloud SQL instances found.")
            return []

        for instance in instances:
            instance_name = instance.get('name')
            instance_region = instance.get('region', 'global')

            # Use 'num_backends' as a proxy for connection activity
            filter_str = f'metric.type="cloudsql.googleapis.com/database/postgresql/num_backends" AND resource.labels.database_id="{project_id}:{instance_name}"'
            time_series_request = monitoring_v3.ListTimeSeriesRequest(
                name=f"projects/{project_id}",
                filter=filter_str,
                interval=interval,
                view=monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.HEADERS,
            )

            has_activity = any(monitoring_client.list_time_series(time_series_request))

            if not has_activity:
                print(f"    Cloud SQL instance '{instance_name}' appears to be orphaned (no activity).")

                # --- NEW: Cloud SQL Cost Calculation ---
                instance_tier = instance.get('settings', {}).get('tier', 'unknown')
                storage_size_gb = int(instance.get('settings', {}).get('storageSize', 0))
                storage_type = instance.get('settings', {}).get('storageType', 'SSD')

                cost_config = {
                    'name': instance_name,
                    'tier': instance_tier,
                    'storage_size_gb': storage_size_gb,
                    'storage_type': storage_type,
                    'region': instance_region
                }
                monthly_cost = get_resource_cost('cloud_sql', cost_config)
                # --- END NEW: Cloud SQL Cost Calculation ---

                metadata = extract_resource_metadata(
                    labels=instance.get('settings', {}).get('userLabels', {}),
                    resource_name=instance_name,
                    resource_type='cloud_sql',
                    region=instance_region,
                    full_name=instance.get('selfLink', ''),
                    status=instance.get('state'),
                    cost_analysis={'total_cost_usd': monthly_cost},
                    utilization_data={'finding': "Orphaned",
                                      'recommendation': "Review DB",
                                      'details': 'No activity for over 30 days'},
                    is_orphaned=True
                )

                orphaned_instances.append({
                    'name': instance_name,
                    'resource_metadata': metadata
                })

        if not orphaned_instances:
            print("  No orphaned Cloud SQL instances found.")
        else:
            print(f"Found {len(orphaned_instances)} orphaned Cloud SQL instances.")

    except Exception as e:
        print(f" Error analyzing Cloud SQL instances for orphaned status: {e}")
        return []

    return orphaned_instances

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
    """
    print(f"\nAnalyzing GKE Container Base Images (Enhanced Sizing Logic)")
    print("=" * 60)
    print("Identifying images that could be smaller and more secure (recommending alpine, slim, distroless)")

    asset_client = asset_v1.AssetServiceClient(credentials=credentials)
    flagged_containers = []
    total_pods_analyzed = 0

    MINIMAL_IMAGE_MAP = {
        'ubuntu': 'ubuntu:minimal or gcr.io/distroless/base-debian11',
        'debian': 'debian:slim or gcr.io/distroless/base-debian11',
        'centos': 'Consider a smaller base like debian:slim or Alpine',
        'python': 'python:slim or python:alpine. For non-native dependencies, use gcr.io/distroless/python3-debian11',
        'node': 'node:alpine or gcr.io/distroless/nodejs18-debian11',
        'golang': 'golang:alpine (for builder) and gcr.io/distroless/static-debian11 (for final image)',
        'openjdk': 'openjdk:alpine or gcr.io/distroless/java17-debian11',
        'nginx': 'nginx:alpine'
    }

    try:
        response = asset_client.search_all_resources(
            request={"scope": f"projects/{project_id}", "asset_types": ["k8s.io/Pod"], "page_size": 500}
        )

        for resource in response:
            total_pods_analyzed += 1
            if not resource.additional_attributes or not resource.additional_attributes.get('resource'):
                continue

            pod_data = json.loads(resource.additional_attributes.get('resource'))
            pod_name = pod_data.get('metadata', {}).get('name', 'unknown-pod')
            namespace = pod_data.get('metadata', {}).get('namespace', 'default')

            try:
                cluster_name = resource.name.split('/clusters/')[1].split('/')[0]
                location = resource.location
            except IndexError:
                cluster_name = "unknown-cluster"
                location = "unknown-location"

            containers = pod_data.get('spec', {}).get('containers', [])
            for container in containers:
                image_used = container.get('image')
                if not image_used:
                    continue

                base_image = image_used.split(':')[0].split('/')[-1]
                if base_image in MINIMAL_IMAGE_MAP:
                    recommended_image = MINIMAL_IMAGE_MAP[base_image]
                    container_name = container.get('name', 'unknown-container')

                    print(f"    - Flagged Container: {pod_name}/{container_name} (uses bloated image: {image_used})")

                    metadata = extract_resource_metadata(
                        labels=pod_data.get('metadata', {}).get('labels', {}),
                        resource_name=f"{pod_name}/{container_name}",
                        resource_type='container',
                        region=location,
                        full_name=f"{resource.name}/containers/{container_name}",
                        status="Inefficient Image",
                        cost_analysis={'total_cost_usd': 0.0},  # Cost is indirect (storage/network)
                        utilization_data={
                            'finding': 'Inefficient Container Image',
                            'recommendation': f"Replace '{image_used}' with a minimal alternative like '{recommended_image}'"
                        },
                        is_orphaned=False
                    )
                    flagged_containers.append({"resource_metadata": metadata})

        print(f"\nFinished analysis. Found {len(flagged_containers)} containers using inefficient base images.")

    except Exception as e:
        print(f" Error analyzing GKE container images: {e}")

    return flagged_containers


def analyze_cloud_run_optimization_opportunities(project_id, credentials, thresholds):
    """Analyzes Cloud Run services for right-sizing, concurrency, and min-instance costs using Asset Inventory."""
    print("\nAnalyzing Cloud Run Services for Advanced Optimization...")
    print("=" * 60)

    optimization_candidates = []

    try:
        # Use Asset Inventory to find all Cloud Run services (v1 and v2)
        response = asset_client.search_all_resources(
            request={"scope": f"projects/{project_id}", "asset_types": ["run.googleapis.com/Service"]}
        )
        services = list(response)

        if not services:
            print("  No Cloud Run services found.")
            return []

        print(f"  Found {len(services)} Cloud Run services to analyze.")

        for service in services:
            service_name = service.display_name
            location = service.location
            all_findings_for_service = []

            # The full resource data is in additional_attributes as a JSON string
            if not service.additional_attributes or not service.additional_attributes.get('resource'):
                continue

            service_data = json.loads(service.additional_attributes.get('resource'))
            template = service_data.get('template', {})

            # --- 1. Min Instances Analysis (Cost of Idle) ---
            min_instances = template.get('scaling', {}).get('minInstanceCount', 0)

            if min_instances > 0:
                container = template.get('containers', [{}])[0]
                cpu_limit_str = container.get('resources', {}).get('limits', {}).get('cpu', '1000m')
                cpu_limit = float(cpu_limit_str.replace('m', '')) / 1000 if 'm' in cpu_limit_str else float(
                    cpu_limit_str)

                mem_limit_str = container.get('resources', {}).get('limits', {}).get('memory', '512Mi')
                mem_value = int(''.join(filter(str.isdigit, mem_limit_str)))
                mem_limit_gb = mem_value / 1024 if 'Mi' in mem_limit_str else mem_value

                idle_cost_config = {'name': service_name, 'cpu': cpu_limit, 'memory_gb': mem_limit_gb,
                                    'region': location}
                idle_cost = get_resource_cost('cloud_run_idle', idle_cost_config) * min_instances

                metadata = extract_resource_metadata(
                    labels=service.labels,
                    resource_name=service_name,
                    resource_type='cloud_run',
                    region=location,
                    full_name=service.name,
                    status="ACTIVE_IDLE_COST",
                    cost_analysis={'total_cost_usd': idle_cost},
                    utilization_data={'finding': f'Idle cost for {min_instances} min-instance(s)'},
                    is_orphaned=False
                )
                metadata['Recommendation'] = "Set min-instances to 0"
                all_findings_for_service.append({"resource_metadata": metadata})

            # --- 2. Concurrency Analysis ---
            concurrency = template.get('containerConcurrency', 80)
            if concurrency > 0 and concurrency < 10:
                metadata = extract_resource_metadata(
                    labels=service.labels,
                    resource_name=service_name,
                    resource_type='cloud_run',
                    region=location,
                    full_name=service.name,
                    status="LOW_CONCURRENCY",
                    cost_analysis={'total_cost_usd': 0.0},
                    utilization_data={'finding': f'Low concurrency set to {concurrency}'},
                    is_orphaned=False
                )
                metadata['Recommendation'] = "Increase concurrency if I/O bound"
                all_findings_for_service.append({"resource_metadata": metadata})

            if all_findings_for_service:
                print(f"    Found {len(all_findings_for_service)} optimization opportunities for '{service_name}'")
                optimization_candidates.extend(all_findings_for_service)
            else:
                print(f"  '{service_name}' appears well-configured.")

    except Exception as e:
        if "run.googleapis.com has not been used" in str(e):
            print("  Cloud Run API is not enabled for this project, skipping.")
        else:
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
        'cmp_memory_usage': 15.0,
        'nic_idle_threshold_mb_per_day': 1.0,
        'cmp_network_usage': 5.0,
        'sc_stor_size_in_gb': 1.0,
        'stor_access_frequency': 'Infrequent',
        'disk_underutilized_gb': 100,
        'snapshot_age_threshold_days': 90,
        'gke_low_node_threshold': 1,
        'cloud_sql_underutilization_percent': 10.0,
        'cloud_sql_inactivity_days': 30,
        'image_size_threshold_mb': 1024,
        'required_tags': ["features", "lab", "platform", "cio", "ticketid", "environment"]
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


def cache_cloud_sql_tiers(project_id, credentials):
    """
    Fetches all available Cloud SQL tiers and caches their vCPU and RAM configurations.
    """
    global CLOUD_SQL_TIER_CACHE
    CLOUD_SQL_TIER_CACHE = {}
    print("  Caching all Cloud SQL tier configurations...")
    try:
        sqladmin = discovery.build('sqladmin', 'v1beta4', credentials=credentials)
        tiers = sqladmin.tiers().list(project=project_id).execute().get('items', [])

        for tier in tiers:
            tier_name = tier.get('tier')
            ram_gb = tier.get('Ram', 0) / 1024  # Convert MB to GB
            vcpus = 0

            parts = tier_name.split('-')
            if len(parts) > 1:
                try:
                    if 'custom' in tier_name:
                        vcpus = int(parts[-2])
                    else:
                        vcpus = int(parts[-1])
                except (ValueError, IndexError):
                    pass  # Keep vcpus as 0 if parsing fails

            CLOUD_SQL_TIER_CACHE[tier_name] = {'vcpus': vcpus, 'ram_gb': ram_gb}
        print("  SUCCESS: Cloud SQL tiers cached.")
    except Exception as e:
        print(f"  WARNING: Could not cache Cloud SQL tiers: {e}")

# ================================================================================
# PRICING LOOKUP
# ================================================================================



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

    print(f"\nAnalyzing Storage Buckets (Quota: {bucket_quota_gb}GB)")
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


def analyze_cloud_sql_holistically(project_id, credentials, thresholds):
    """
    Analyzes Cloud SQL instances for multiple issues (untagged, underutilized, orphaned)
    and consolidates all findings for a single instance into one record.
    """
    print(f"\nAnalyzing Cloud SQL Instances Holistically")
    print("=" * 60)

    flagged_instances = []
    try:
        sqladmin = discovery.build('sqladmin', 'v1beta4', credentials=credentials)
        instances = sqladmin.instances().list(project=project_id).execute().get('items', [])

        if not instances:
            print("  No Cloud SQL instances found.")
            return []

        for instance in instances:
            instance_name = instance['name']
            instance_region = instance['region']
            instance_tier = instance.get('settings', {}).get('tier')
            db_version = instance.get('databaseVersion', '')

            reasons = []
            recommendations = []

            # Check 1: Untagged
            if not instance.get('settings', {}).get('userLabels'):
                reasons.append("Untagged")
                recommendations.append("Add labels for cost tracking.")

            # Check 2: Underutilized Disk
            disk_util_threshold = thresholds.get('sql_disk_usage', 20.0)
            disk_size_gb = int(instance.get('settings', {}).get('dataDiskSizeGb', 0))
            # Assuming currentDiskSize is in bytes from the API
            disk_usage_gb = int(instance.get('currentDiskSize', 0)) / (1024 ** 3)

            if disk_size_gb > 0:
                disk_util_percentage = (disk_usage_gb / disk_size_gb) * 100
                if disk_util_percentage < disk_util_threshold:
                    reasons.append(f"Low Disk Utilization ({disk_util_percentage:.2f}%)")
                    recommendations.append("Consider reducing disk size.")

            # Check 3: Orphaned (No Connections) can be added here if needed

            if reasons:
                final_reasons = "; ".join(reasons)
                final_recommendations = "; ".join(recommendations)

                print(f"  - Flagged Instance: {instance_name} | Reasons: {final_reasons}")

                tier_details = CLOUD_SQL_TIER_CACHE.get(instance_tier, {'vcpus': 0, 'ram_gb': 0})

                storage_type = instance.get('settings', {}).get('dataDiskType', 'PD_SSD')
                cost_config = {
                    'name': instance_name,
                    'vcpus': tier_details['vcpus'],
                    'memory_gb': tier_details['ram_gb'],
                    'storage_size_gb': disk_size_gb,
                    'storage_type': 'SSD' if 'SSD' in storage_type else 'HDD',
                    'region': instance_region,
                    'db_version': db_version
                }

                cost = get_resource_cost('cloud_sql', cost_config)

                metadata = extract_resource_metadata(
                    labels=instance.get('settings', {}).get('userLabels', {}),
                    resource_name=instance_name,
                    resource_type='cloud_sql',
                    region=instance_region,
                    full_name=instance.get('selfLink'),
                    status=instance.get('state'),
                    cost_analysis={'total_cost_usd': cost},
                    utilization_data={
                        'finding': final_reasons,
                        'recommendation': final_recommendations,
                    },
                    is_orphaned="Orphaned" in final_reasons
                )

                # --- THIS IS THE FIX ---
                flagged_instances.append({"resource_metadata": metadata})

    except Exception as e:
        print(f" Error during holistic Cloud SQL analysis: {e}")

    return flagged_instances


def analyze_gke_clusters_holistically(project_id, credentials, thresholds):
    """
    Analyzes GKE clusters by consolidating node utilization and the cluster management fee
    into a single, actionable finding.
    """
    print(f"\nAnalyzing GKE Clusters Holistically")
    print("=" * 60)

    asset_client = asset_v1.AssetServiceClient(credentials=credentials)
    compute = discovery.build('compute', 'v1', credentials=credentials)
    flagged_clusters = []

    cpu_threshold = thresholds.get('cmp_cpu_usage', 15.0)
    mem_threshold = thresholds.get('cmp_memory_usage', 15.0)

    try:
        response = asset_client.search_all_resources(
            request={"scope": f"projects/{project_id}", "asset_types": ["container.googleapis.com/Cluster"]}
        )
        all_clusters = list(response)

        if not all_clusters:
            print("  No GKE clusters found.")
            return []

        # Find all GKE node VMs at once for efficiency
        vm_response = asset_client.search_all_resources(
            request={"scope": f"projects/{project_id}", "asset_types": ["compute.googleapis.com/Instance"]}
        )

        # --- THIS IS THE FIX ---
        # Ensure vm.labels exists before trying to access it
        all_vms = [vm for vm in vm_response if vm.labels and 'goog-gke-cluster-name' in vm.labels]

        for cluster in all_clusters:
            cluster_name = cluster.display_name
            location = cluster.location
            underutilized_nodes = []
            underutilized_node_cost = 0.0
            total_node_count = 0

            for vm in all_vms:
                if vm.labels.get('goog-gke-cluster-name') == cluster_name:
                    total_node_count += 1
                    vm_name = vm.display_name
                    instance_id = vm.name.split('/')[-1]
                    zone = vm.location

                    cpu_util = get_average_utilization(project_id, vm.asset_type, vm.name, credentials)
                    mem_util = get_vm_memory_utilization(project_id, instance_id, zone, credentials)

                    is_underutilized = (cpu_util is not None and cpu_util < cpu_threshold) or \
                                       (mem_util is not None and mem_util < mem_threshold)

                    if is_underutilized:
                        try:
                            instance_details = compute.instances().get(project=project_id, zone=zone,
                                                                       instance=instance_id).execute()

                            machine_type_uri = instance_details.get('machineType')
                            if not machine_type_uri:
                                print(
                                    f"     WARNING: Could not determine machine type for node {vm_name}. Skipping cost calculation.")
                                continue

                            machine_type = machine_type_uri.split('/')[-1]
                            machine_type_details = compute.machineTypes().get(project=project_id, zone=zone,
                                                                              machineType=machine_type).execute()
                            cost_config = {
                                'machine_type': machine_type, 'region': zone.rsplit('-', 1)[0],
                                'cpu_cores': machine_type_details.get('guestCpus'),
                                'memory_gb': machine_type_details.get('memoryMb', 0) / 1024
                            }
                            node_cost = get_resource_cost('vm', cost_config)
                            underutilized_node_cost += node_cost
                            underutilized_nodes.append(vm_name)
                        except Exception as e:
                            print(f"     Could not calculate cost for node {vm_name}: {e}")

            if underutilized_nodes:
                management_fee = get_resource_cost('gke_cluster', {})
                total_potential_savings = underutilized_node_cost + management_fee

                finding_text = f"Cluster Overprovisioned ({len(underutilized_nodes)} of {total_node_count} nodes underutilized)"
                recommendation_text = "Scale down the node pool or consolidate workloads."

                print(
                    f"  - Flagged Cluster: {cluster_name} | Reason: {finding_text} | Potential Saving: ${total_potential_savings:.2f}/mo")

                metadata = extract_resource_metadata(
                    labels=cluster.labels,
                    resource_name=cluster_name,
                    resource_type='cluster',
                    region=location,
                    full_name=cluster.name,
                    status="RUNNING",
                    cost_analysis={'total_cost_usd': total_potential_savings},
                    utilization_data={
                        'finding': finding_text,
                        'recommendation': recommendation_text,
                    },
                    is_orphaned=False
                )
                flagged_clusters.append({"resource_metadata": metadata})

    except Exception as e:
        print(f" Error during holistic GKE analysis: {e}")

    return flagged_clusters

def get_resource_cost(resource_type, config):
    """
    Calculates the estimated monthly cost for a given GCP resource.
    This enhanced version uses more robust SKU matching patterns for all resource types
    to ensure accurate cost lookup and prevent '0' cost calculations.
    """
    cost_per_month = 0.0
    HOURS_PER_MONTH = 730  # Average hours in a month

    try:
        if resource_type == 'vm':
            machine_type = config.get('machine_type', '')
            region = config.get('region', 'global')

            # Example: 'e2-medium' -> family is 'E2'
            machine_family = machine_type.split('-')[0].upper()

            # Find price for CPU cores and RAM using more reliable search patterns
            cpu_sku_pattern = f"{machine_family} Instance Core"
            ram_sku_pattern = f"{machine_family} Instance Ram"

            cpu_price_per_hour, _ = find_sku_in_list("Compute Engine API", cpu_sku_pattern, region)
            ram_price_per_gb_hour, _ = find_sku_in_list("Compute Engine API", ram_sku_pattern, region)

            cpu_cores = config.get('cpu_cores', 0)
            memory_gb = config.get('memory_gb', 0)

            cost_per_month = (cpu_cores * cpu_price_per_hour + memory_gb * ram_price_per_gb_hour) * HOURS_PER_MONTH

        elif resource_type == 'disk':
            disk_type = config.get('disk_type', 'pd-standard')
            size_gb = config.get('size_gb', 0)
            region = config.get('region', 'global')

            storage_type_map = {
                'pd-standard': 'Standard',
                'pd-balanced': 'Balanced',
                'pd-ssd': 'SSD',
                'pd-extreme': 'Extreme'
            }
            mapped_type = storage_type_map.get(disk_type, 'SSD')
            # Broader search term for persistent disks
            sku_pattern = f"{mapped_type} backed PD Capacity"

            price_per_gb_month, _ = find_sku_in_list("Compute Engine API", sku_pattern, region)
            cost_per_month = size_gb * price_per_gb_month

        elif resource_type == 'snapshot':
            size_gb = config.get('size_gb', 0)
            price_per_gb_month, _ = find_sku_in_list("Compute Engine API", "Storage PD Snapshot", "global")
            cost_per_month = size_gb * price_per_gb_month

        elif resource_type == 'bucket':
            size_gb = config.get('size_gb', 0)
            region = config.get('region', 'global')
            sku_pattern = "Standard Storage US Regional"  # Example for standard storage
            price_per_gb_month, _ = find_sku_in_list("Google Cloud Storage", sku_pattern, region)
            cost_per_month = size_gb * price_per_gb_month

        elif resource_type == 'public_ip':
            region = config.get('region', 'global')
            price_per_hour, _ = find_sku_in_list("Compute Engine API", "Static IP Charge", region)
            cost_per_month = price_per_hour * HOURS_PER_MONTH

        elif resource_type == 'load_balancer':
            region = config.get('region', 'global')
            price_per_hour, _ = find_sku_in_list("Compute Engine API", "Forwarding Rule Minimum Fee", region)
            cost_per_month = price_per_hour * HOURS_PER_MONTH

        elif resource_type == 'gke_cluster':
            price_per_hour, _ = find_sku_in_list("Kubernetes Engine", "Cluster Management Fee", "global")
            cost_per_month = price_per_hour * HOURS_PER_MONTH

        elif resource_type == 'cloud_run_idle':
            cpu = config.get('cpu', 0)
            memory_gb = config.get('memory_gb', 0)
            region = config.get('region', 'global')

            cpu_price_per_hour, _ = find_sku_in_list("Cloud Run", "CPU allocation time", region)
            ram_price_per_gb_hour, _ = find_sku_in_list("Cloud Run", "Memory allocation time", region)

            cost_per_month = (cpu * cpu_price_per_hour + memory_gb * ram_price_per_gb_hour) * HOURS_PER_MONTH

        elif resource_type == 'cloud_sql':
            vcpus = config.get('vcpus', 0)
            memory_gb = config.get('memory_gb', 0)
            storage_type = config.get('storage_type', 'SSD')
            storage_size_gb = config.get('storage_size_gb', 0)
            region = config.get('region', 'us-central1')
            db_version = config.get('db_version', '')

            engine_map = {'POSTGRES': 'PostgreSQL', 'MYSQL': 'MySQL', 'SQLSERVER': 'SQL Server'}
            engine_sku_name = "MySQL"
            for k, v in engine_map.items():
                if k in db_version.upper():
                    engine_sku_name = v
                    break

            instance_cost_monthly = 0.0
            if vcpus > 0 and memory_gb > 0:
                # Use a list of patterns for better matching
                cpu_patterns = [f"{engine_sku_name} vCPU", f"Core for {engine_sku_name}", "custom vCPU"]
                ram_patterns = [f"{engine_sku_name} RAM", f"Memory for {engine_sku_name}", "custom RAM"]

                cpu_price, _ = (0.0, "")
                for pattern in cpu_patterns:
                    cpu_price, _ = find_sku_in_list("Cloud SQL", pattern, region)
                    if cpu_price > 0: break

                ram_price, _ = (0.0, "")
                for pattern in ram_patterns:
                    ram_price, _ = find_sku_in_list("Cloud SQL", pattern, region)
                    if ram_price > 0: break

                instance_cost_monthly = (vcpus * cpu_price + memory_gb * ram_price) * HOURS_PER_MONTH

            # Try multiple patterns for storage to ensure a match
            storage_patterns = [f"{storage_type} storage for Cloud SQL", f"{storage_type} storage", "Storage PD"]
            storage_price, _ = (0.0, "")
            for pattern in storage_patterns:
                storage_price, _ = find_sku_in_list("Cloud SQL", pattern, region)
                if storage_price > 0: break

            storage_cost_monthly = storage_size_gb * storage_price
            cost_per_month = instance_cost_monthly + storage_cost_monthly

    except Exception as e:
        print(f"     [ERROR] Cost calculation failed for {resource_type} {config.get('name', '')}: {e}")
        return 0.0

    return cost_per_month

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

    print(f"\nAnalyzing Persistent Disks (Small Disk Threshold: <{disk_quota_gb}GB)")
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
    print(f"\nAnalyzing Disk Snapshots (Old Snapshot Threshold: >{snapshot_age_threshold_days} days)")
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
    print(f"\nAnalyzing GKE Clusters (Low Node Threshold: <{low_node_threshold} node(s))")
    print("=" * 60)

    container_client = discovery.build('container', 'v1', credentials=credentials)
    underutilized_clusters = []
    total_clusters = 0

    try:
        parent = f"projects/{project_id}/locations/-"
        request = container_client.projects().locations().clusters().list(parent=parent)
        response = request.execute()

        for cluster in response.get('clusters', []):
            total_clusters += 1
            cluster_name = cluster.get('name')
            node_count = cluster.get('currentNodeCount', 0)
            cluster_status = cluster.get('status', 'RUNNING')
            cluster_location = cluster.get('location')

            reasons = []
            is_flagged = False

            if cluster_status in ['STOPPING', 'ERROR', 'DEGRADED']:
                is_flagged = True
                reasons.append(f"status is {cluster_status}")
            elif node_count == 0 and cluster_status == 'RUNNING':
                is_flagged = True
                reasons.append("zero nodes")
            elif node_count < low_node_threshold and cluster_status == 'RUNNING':
                is_flagged = True
                reasons.append("very low node count")

            print(f"  {cluster_name} (Location: {cluster_location}, Nodes: {node_count}, Status: {cluster_status})")

            if is_flagged:
                metadata = extract_resource_metadata(
                    labels=cluster.get('resourceLabels', {}),
                    resource_name=cluster_name,
                    resource_type='cluster',
                    region=cluster_location,
                    full_name=cluster.get('selfLink'),
                    status=cluster_status,
                    cost_analysis={'total_cost_usd': 0.0}, # Cost is in the nodes, not the cluster entity itself
                    utilization_data={
                        'finding': "Underutilized Cluster",
                        'recommendation': "Delete cluster if no longer needed",
                        'details': "; ".join(reasons)
                    },
                    is_orphaned=(node_count == 0)
                )
                underutilized_clusters.append({
                    'name': cluster_name,
                    'resource_metadata': metadata
                })

        print(f"\nTotal GKE clusters analyzed: {total_clusters}")
        print(f"Potentially Underutilized/Orphaned GKE Clusters: {len(underutilized_clusters)}")

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
    Analyzes Instance Group Managers for untagged templates, fixed-size configs,
    and orphaned status based on 30-day inactivity.
    """
    print(f"\nAnalyzing Instance Groups with Enhanced Logic")
    print("=" * 60)

    flagged_groups = []
    total_groups = 0
    required_tags = thresholds.get('required_tags', ["features", "lab", "platform", "cio", "ticketid", "environment"])

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
                        target_size = igm.get('targetSize', 0)

                        finding_types = []
                        recommendations = []
                        labels = {}

                        # Check 1: Is the instance template untagged?
                        template_url = igm.get('instanceTemplate')
                        if template_url:
                            try:
                                template_name = template_url.split('/')[-1]
                                if "/regions/" in template_url:
                                    region = template_url.split('/regions/')[1].split('/')[0]
                                    template_info = compute.regionInstanceTemplates().get(project=project_id,
                                                                                          region=region,
                                                                                          instanceTemplate=template_name).execute()
                                else:
                                    template_info = compute.instanceTemplates().get(project=project_id,
                                                                                    instanceTemplate=template_name).execute()

                                labels = template_info.get('properties', {}).get('labels', {})
                                missing_tags = [tag for tag in required_tags if tag not in labels]
                                if missing_tags:
                                    finding_types.append("Untagged")
                                    recommendations.append("Review Tags")
                            except Exception as e:
                                print(f"     Warning: Could not fetch labels for template {template_url}: {e}")

                        # Check 2: Is it a fixed-size (non-autoscaling) group?
                        autoscaling_policy = igm.get('autoscalingPolicy', {})
                        min_replicas = autoscaling_policy.get('minNumReplicas')
                        max_replicas = autoscaling_policy.get('maxNumReplicas')
                        if min_replicas is not None and min_replicas == max_replicas and target_size > 0:
                            finding_types.append("Stop InstanceGroup")
                            recommendations.append("Scale Down")

                        # Check 3: Is it orphaned (scaled to 0 and inactive for 30 days)?
                        if target_size == 0:
                            if not has_scaling_activity(project_id, location, igm_name, credentials):
                                finding_types.append("Orphan")
                                recommendations.append("Review Instance Group")

                        # If we have any reason to flag this IGM, create a record
                        if finding_types:
                            final_finding = "; ".join(sorted(list(set(finding_types))))
                            final_recommendation = "; ".join(sorted(list(set(recommendations))))

                            # Note: Cost of an IGM is the cost of its running VMs, which is complex.
                            # We report $0 as the cost is captured by the VM analysis itself.
                            cost_data = {'total_cost_usd': 0.0}

                            metadata = extract_resource_metadata(
                                labels=labels,
                                resource_name=igm_name,
                                resource_type='instance_group',
                                region=location,
                                full_name=igm.get('selfLink'),
                                status="FLAGGED",
                                cost_analysis=cost_data,
                                utilization_data={
                                    'finding': final_finding,
                                    'recommendation': final_recommendation,
                                    'target_size': target_size
                                },
                                is_orphaned=("Orphan" in finding_types)
                            )
                            flagged_groups.append({"resource_metadata": metadata})

            request = compute.instanceGroupManagers().aggregatedList_next(previous_request=request,
                                                                          previous_response=response)

        print(f"\nFinished analysis. Found {len(flagged_groups)} instance groups for review.")

    except Exception as e:
        print(f" Error analyzing instance groups: {e}")

    return flagged_groups


def get_vm_memory_utilization(project_id, instance_id, zone, credentials):
    """
    Fetches the average memory utilization for a specific VM over the last 30 days.
    """
    try:
        client = monitoring_v3.MetricServiceClient(credentials=credentials)
        project_name = f"projects/{project_id}"

        end_time = datetime.now(UTC)
        start_time = end_time - timedelta(days=30)

        # MQL query to get memory utilization percentage
        # This requires the Ops Agent to be installed on the VM
        mql_query = f"""
        fetch gce_instance
        | metric 'agent.googleapis.com/memory/percent_used'
        | filter (metadata.system_labels.instance_id == '{instance_id}')
        | group_by 30d, [value_percent_used_mean: mean(value.percent_used)]
        | every 30d
        """

        request = monitoring_v3.QueryTimeSeriesRequest(
            name=project_name,
            query=mql_query,
        )

        results = client.query_time_series(request=request)

        # --- THIS IS THE FIX ---
        # Check if any time series data was returned before processing
        time_series_data = list(results)
        if not time_series_data or not time_series_data[0].points:
            # print(f"     [DEBUG] No memory metrics found for VM {instance_id}.")
            return 0.0

        # Extract the mean value from the first point of the first series
        # The result of the MQL query should be a single point with the 30-day mean
        mean_utilization = time_series_data[0].points[0].value.double_value
        return mean_utilization

    except Exception as e:
        if 'Cannot find metric' in str(e) or 'is not available' in str(e):
            pass  # Suppress common errors for VMs without the Ops Agent
        else:
            print(f"     Could not get memory utilization for {instance_id}: {e}")
        return 0.0

def analyze_load_balancers(project_id, credentials, thresholds):
    """
    Analyzes Cloud Load Balancers to find any that are idle.
    An idle load balancer has a forwarding rule with no healthy backends.
    """
    print(f"\nAnalyzing Idle Cloud Load Balancers")
    print("=" * 60)

    flagged_lbs = []

    try:
        request = compute.forwardingRules().aggregatedList(project=project_id)
        while request is not None:
            response = request.execute()
            for region_url, region_data in response.get('items', {}).items():
                if 'forwardingRules' in region_data:
                    region = region_url.split('/')[-1]
                    for rule in region_data['forwardingRules']:
                        is_idle = False
                        backend_service_url = rule.get('backendService')

                        if not backend_service_url:
                            # If it has no backend service, it's not a typical LB we can check for health.
                            continue

                        backend_service_name = backend_service_url.split('/')[-1]

                        try:
                            # Determine if it's a regional or global backend service
                            if "/regions/" in backend_service_url:
                                bs_region = backend_service_url.split('/regions/')[1].split('/')[0]
                                service_details = compute.regionBackendServices().get(project=project_id,
                                                                                      region=bs_region,
                                                                                      backendService=backend_service_name).execute()
                            else:
                                service_details = compute.backendServices().get(project=project_id,
                                                                                backendService=backend_service_name).execute()

                            backends = service_details.get('backends', [])
                            if not backends:
                                is_idle = True  # Idle because there are no backends attached.
                            else:
                                # Check the health of the attached backends (instance groups)
                                all_backends_unhealthy = True
                                for backend in backends:
                                    group_url = backend.get('group')
                                    if not group_url:
                                        continue

                                    group_name = group_url.split('/')[-1]
                                    group_zone = group_url.split('/zones/')[1].split('/')[
                                        0] if '/zones/' in group_url else None

                                    health_check_request = compute.backendServices().getHealth(project=project_id,
                                                                                               backendService=backend_service_name,
                                                                                               resource={
                                                                                                   'group': group_url})
                                    health_status_response = health_check_request.execute()

                                    health_states = health_status_response.get('healthStatus', [])
                                    # If we find even one healthy instance, the LB is not idle.
                                    if any(state.get('healthState') == 'HEALTHY' for state in health_states):
                                        all_backends_unhealthy = False
                                        break
                                if all_backends_unhealthy:
                                    is_idle = True  # Idle because no backends are healthy.

                        except Exception:
                            # If we can't get backend health, assume it's not idle to be safe.
                            is_idle = False

                        if is_idle:
                            rule_name = rule.get('name')
                            cost_config = {'region': region}
                            monthly_cost = get_resource_cost('load_balancer', cost_config)

                            print(
                                f"    - Flagged Idle Load Balancer: {rule_name} in {region} | Cost: ${monthly_cost:.2f}/mo")

                            metadata = extract_resource_metadata(
                                labels=rule.get('labels', {}),
                                resource_name=rule_name,
                                resource_type='load_balancer',
                                region=region,
                                full_name=rule.get('selfLink'),
                                status="IDLE",
                                cost_analysis={'total_cost_usd': monthly_cost},
                                utilization_data={
                                    'finding': "Idle Load Balancer",
                                    'recommendation': "Delete load balancer and its components if no longer needed",
                                    'details': "No healthy backends were found for this load balancer's forwarding rule."
                                },
                                is_orphaned=True
                            )
                            flagged_lbs.append({"resource_metadata": metadata})

            request = compute.forwardingRules().aggregatedList_next(previous_request=request,
                                                                    previous_response=response)

        print(f"\nFinished analysis. Found {len(flagged_lbs)} idle load balancers.")

    except Exception as e:
        print(f" Error analyzing load balancers: {e}")

    return flagged_lbs

def analyze_storage_buckets(project_id, credentials, thresholds):
    """
    Analyzes Cloud Storage buckets for underutilization, missing tags, and orphaned status.
    """
    print("\nAnalyzing Cloud Storage Buckets...")
    print("=" * 60)

    # Get thresholds from the config dictionary
    size_threshold_gb = thresholds.get('sc_stor_size_in_gb', 1.0)
    required_tags = thresholds.get('required_tags', ["features", "lab", "platform", "cio", "ticketid", "environment"])

    asset_client = asset_v1.AssetServiceClient(credentials=credentials)
    flagged_buckets = []
    scope = f"projects/{project_id}"

    try:
        response = asset_client.search_all_resources(
            request={"scope": scope, "asset_types": ["storage.googleapis.com/Bucket"]}
        )

        all_buckets = list(response)
        print(f"  Found {len(all_buckets)} buckets to analyze.")

        for resource in all_buckets:
            bucket_name = resource.name.split("/")[-1]
            labels = dict(resource.labels) if hasattr(resource, 'labels') else {}
            location = resource.location
            full_name = resource.name

            # --- Get bucket size ---
            total_bytes = get_bucket_size_gcs(bucket_name, credentials)
            size_gb = (total_bytes / 1_000_000_000) if total_bytes is not None else 0

            # --- Dynamically build findings and recommendations ---
            finding_types = []
            recommendation_types = []

            is_orphaned = (total_bytes == 0)

            # 1. Check for Underutilization
            if total_bytes is not None and size_gb < size_threshold_gb:
                finding_types.append("Storage Underutilized")
                recommendation_types.append("Try Reduction")

            # 2. Check for Orphaned Status
            if is_orphaned:
                finding_types.append("Orphaned Storage")
                recommendation_types.append("Review Bucket")
                # If orphaned, it's also underutilized, but "Orphaned" is more specific.
                if "Storage Underutilized" in finding_types:
                    finding_types.remove("Storage Underutilized")
                    recommendation_types.remove("Try Reduction")

            # 3. Check for Missing Tags
            missing_tags = [tag for tag in required_tags if tag not in labels]
            if missing_tags:
                finding_types.append("Untagged Storage")
                recommendation_types.append("Apply Tags")

            # --- Consolidate and create metadata if any findings exist ---
            if finding_types:
                final_finding = "; ".join(sorted(list(set(finding_types))))
                final_recommendation = "; ".join(sorted(list(set(recommendation_types))))

                cost_config = {'size_gb': size_gb, 'region': location}
                monthly_cost = get_resource_cost('bucket', cost_config)
                cost_analysis = {'total_cost_usd': monthly_cost}

                utilization_data = {
                    'finding': final_finding,
                    'recommendation': final_recommendation,
                    'size_gb': size_gb,
                    'is_orphaned': is_orphaned,
                    'missing_tags': missing_tags
                }

                print(f"    - Flagged Bucket: {bucket_name} | Reasons: {utilization_data['finding']}")

                metadata = extract_resource_metadata(
                    labels=labels,
                    resource_name=bucket_name,
                    resource_type='bucket',
                    full_name=full_name,
                    region=location,
                    status="Available",
                    cost_analysis=cost_analysis,
                    utilization_data=utilization_data,
                    is_orphaned=is_orphaned
                )
                flagged_buckets.append({"resource_metadata": metadata})

        if not flagged_buckets:
            print("  No issues found with storage buckets based on current criteria.")
        else:
            print(f"  Finished analysis. Found {len(flagged_buckets)} buckets with optimization opportunities.")

    except Exception as e:
        print(f"  Error analyzing storage buckets: {e}")

    return flagged_buckets



def analyze_nics(project_id, credentials, thresholds):
    """
    Analyzes VM Network Interfaces (NICs) to find those with low network traffic.
    """
    # Threshold in MB per day. If average daily traffic is below this, flag the NIC.
    nic_idle_threshold_mb = thresholds.get('nic_idle_threshold_mb_per_day', 1.0)
    nic_idle_threshold_bytes = nic_idle_threshold_mb * 1024 * 1024

    print(f"\nAnalyzing Network Interfaces (Idle Threshold: < {nic_idle_threshold_mb} MB/day)")
    print("=" * 60)

    flagged_nics = []

    try:
        # Get all VM instances to inspect their NICs
        response = asset_client.search_all_resources(
            request={"scope": f"projects/{project_id}", "asset_types": ["compute.googleapis.com/Instance"]}
        )
        all_vms = list(response)
        print(f"  Found {len(all_vms)} VMs to inspect for NIC activity...")

        for vm_resource in all_vms:
            instance_id = vm_resource.name.split("/")[-1]
            vm_name = vm_resource.display_name
            zone = vm_resource.location

            # Query the total network traffic for the instance
            end_time = datetime.now(UTC)
            start_time = end_time - timedelta(days=7)
            interval = monitoring_v3.TimeInterval(end_time=end_time, start_time=start_time)

            filter_str = f'metric.type="compute.googleapis.com/instance/network/total_bytes_count" AND resource.labels.instance_id="{instance_id}"'

            request = monitoring_v3.ListTimeSeriesRequest(
                name=f"projects/{project_id}", filter=filter_str, interval=interval,
                view=monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL
            )
            time_series = monitoring_client.list_time_series(request)

            total_bytes = 0
            point_count = 0
            for ts in time_series:
                for point in ts.points:
                    total_bytes += point.value.int64_value
                    point_count += 1

            # If there's no data, we can't make a determination
            if point_count == 0:
                continue

            avg_bytes_per_day = total_bytes / 7

            if avg_bytes_per_day < nic_idle_threshold_bytes:
                avg_mb_str = f"{avg_bytes_per_day / 1024 / 1024:.4f}"
                print(f"    - Flagged Idle NIC on VM: {vm_name} (Average traffic: {avg_mb_str} MB/day)")

                metadata = extract_resource_metadata(
                    labels=dict(vm_resource.labels) if hasattr(vm_resource, 'labels') else {},
                    resource_name=f"{vm_name}/nic0",  # Assuming the primary NIC
                    resource_type='nic',
                    zone=zone,
                    full_name=f"{vm_resource.name}/networkInterfaces/nic0",
                    status="IDLE",
                    cost_analysis={'total_cost_usd': 0.0},
                    utilization_data={
                        'finding': "Idle Network Interface",
                        'recommendation': "Review VM network activity for potential shutdown or consolidation",
                        'average_mb_per_day': float(avg_mb_str),
                        'threshold_mb_per_day': nic_idle_threshold_mb
                    },
                    is_orphaned=False
                )
                flagged_nics.append({"resource_metadata": metadata})

        print(f"\nFinished analysis. Found {len(flagged_nics)} idle network interfaces.")

    except Exception as e:
        print(f" Error analyzing network interfaces: {e}")

    return flagged_nics

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
                metadata['Finding'] = "Missing Storage policy"
                metadata['Recommendation'] = "Implement a lifecycle policy."
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
def extract_resource_metadata(labels, resource_name, resource_type, region, full_name, status, cost_analysis, utilization_data, is_orphaned=False):
    """
    Extracts metadata from GCP resource data and labels, ensuring a unique ID
    for each optimization finding to prevent MongoDB insertion errors.

    Args:
        labels (dict): A dictionary of the resource's labels.
        resource_name (str): The display name of the resource.
        resource_type (str): The type of the resource (e.g., 'vm', 'cloud_sql').
        region (str, optional): The region of the resource.
        zone (str, optional): The zone of the resource.
        full_name (str, optional): The full API selfLink of the resource.
        status (str, optional): The current status of the resource.
        cost_analysis (dict, optional): A dictionary containing cost data, e.g., {'total_cost_usd': 123.45}.
        utilization_data (dict, optional): A dictionary with utilization findings and recommendations.
        is_orphaned (bool, optional): A flag indicating if the resource is considered orphaned.

    Returns:
        dict: A dictionary containing the formatted metadata for a MongoDB record.
    """

    def get_label_value(label_key):
        """Helper to safely get a label value from the provided labels dict."""
        # This helper is now correctly defined within the function scope.
        return labels.get(label_key, "NA")

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
        resource_status = status or "Running"
    elif resource_type == 'bucket':
        resource_status = "Available"
    elif resource_type == 'disk':
        resource_status = status or "Available"
    elif resource_type == 'subnet':
        resource_status = "Available"
    elif resource_type == 'snapshot':
        resource_status = status or "Ready"
    elif resource_type == 'cluster':
        resource_status = status or "RUNNING"
    elif resource_type == 'persistent_volume':
        resource_status = status or "Available"
    elif resource_type == 'cloud_sql':
        resource_status = status or "RUNNABLE"

    # This ensures a unique _id is generated for each specific finding
    # by incorporating the finding description into the ID.
    finding_summary = utilization_data.get('finding', 'no_finding').replace(' ', '_').replace('<', 'lt_').replace('>',
                                                                                                                  'gt_').lower()

    # Use full_name if available, otherwise construct a general one
    base_id = full_name or f"//cloudresourcemanager.googleapis.com/projects/{PROJECT_ID}/resources/{resource_name}"

    metadata_record = {
        "_id": f"{base_id}/{resource_type}_{finding_summary}",
        "CloudProvider": "GCP",
        "ManagementUnitId": PROJECT_ID,
        "ApplicationCode": get_label_value("application_code"),
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
        "Region": region if region else 'global',
        "TotalCost": total_cost,
        "Currency": currency,
        "Finding": utilization_data.get('finding', "Underutilized"),
        "Recommendation": utilization_data.get('recommendation', "Scale Down"),
        "Environment": get_label_value("environment"),
        "Timestamp": datetime.now(UTC).isoformat().replace('+00:00', 'Z'),
        "ConfidenceScore": "NA",
        "Status": resource_status,
        "Entity": "lbg",
        "RootId": "NA",
        "Email": USER_EMAIL,
        "cost_analysis": cost_analysis or {'total_cost_usd': 0.0},
        "utilization_data": utilization_data
    }
    return metadata_record

# ================================================================================
# MAIN EXECUTION
# ================================================================================

if __name__ == "__main__":
    try:
        # Stage 1: Dynamic Setup and Configuration
        print("\n--- Step 1: Initializing Configuration ---")

        BILLING_ACCOUNT_ID = get_project_billing_info(PROJECT_ID, gcp_credentials)

        if not BILLING_ACCOUNT_ID:
            print("\nCRITICAL: Script cannot proceed without a valid Billing Account ID.")
            sys.exit(1)

        print(f"\nCONFIRMATION: Script will use Billing Account ID: {BILLING_ACCOUNT_ID}")

        print("\n--- Verifying Script Credentials ---")
        print(f"Script is running as: {gcp_credentials.service_account_email}")

        get_service_id("Compute Engine API", gcp_credentials, BILLING_ACCOUNT_ID)
        get_service_id("Google Cloud Storage", gcp_credentials, BILLING_ACCOUNT_ID)
        get_service_id("Cloud Run", gcp_credentials, BILLING_ACCOUNT_ID)
        get_service_id("Cloud SQL", gcp_credentials, BILLING_ACCOUNT_ID)
        get_service_id("Kubernetes Engine", gcp_credentials, BILLING_ACCOUNT_ID)

        thresholds = get_thresholds_from_mongodb(USER_EMAIL)

        # Stage 2: Pre-caching All Information
        print("\n--- Step 2: Pre-caching All Information ---")
        cache_all_skus("Compute Engine API", gcp_credentials, BILLING_ACCOUNT_ID)
        cache_all_skus("Google Cloud Storage", gcp_credentials, BILLING_ACCOUNT_ID)
        cache_all_skus("Cloud Run", gcp_credentials, BILLING_ACCOUNT_ID)
        cache_all_skus("Cloud SQL", gcp_credentials, BILLING_ACCOUNT_ID)
        cache_all_skus("Kubernetes Engine", gcp_credentials, BILLING_ACCOUNT_ID)

        cache_cloud_sql_tiers(PROJECT_ID, gcp_credentials)

        # Stage 3: Run All Resource Analyses
        print("\n--- Step 3: Analyzing All GCP Resources ---")
        print("=" * 80)

        all_candidates = {}

        all_candidates["flagged_vms"] = analyze_vms(PROJECT_ID, gcp_credentials, thresholds)
        all_candidates["flagged_disks"] = analyze_disks(PROJECT_ID, gcp_credentials, thresholds)
        all_candidates["idle_nics"] = analyze_nics(PROJECT_ID, gcp_credentials, thresholds)
        all_candidates["flagged_snapshots"] = analyze_snapshots(PROJECT_ID, gcp_credentials, thresholds)
        all_candidates["flagged_buckets"] = analyze_storage_buckets(PROJECT_ID, gcp_credentials, thresholds)
        all_candidates["missing_tiering_policies"] = analyze_storage_tiering(PROJECT_ID, gcp_credentials)
        all_candidates["inactive_cloud_run"] = categorize_gcp_cloud_run(PROJECT_ID, gcp_credentials, thresholds)
        all_candidates["advanced_cloud_run"] = analyze_cloud_run_optimization_opportunities(PROJECT_ID, gcp_credentials,
                                                                                            thresholds)
        all_candidates["underutilized_instance_groups"] = categorize_gcp_instance_groups(PROJECT_ID, gcp_credentials,
                                                                                         thresholds)
        all_candidates["underutilized_clusters"] = analyze_gke_clusters_holistically(PROJECT_ID, gcp_credentials,
                                                                                     thresholds)
        all_candidates["orphaned_pvs"] = categorize_gcp_kubernetes_persistent_volumes(PROJECT_ID, gcp_credentials,
                                                                                      thresholds)
        all_candidates["inefficient_base_images"] = analyze_gke_container_images(PROJECT_ID, gcp_credentials)
        all_candidates["overprovisioned_k8s_workloads"] = analyze_k8s_overprovisioning(PROJECT_ID, gcp_credentials)
        all_candidates["flagged_cloud_sql"] = analyze_cloud_sql_holistically(PROJECT_ID, gcp_credentials, thresholds)
        all_candidates["unused_reserved_ips"] = analyze_reserved_ips(PROJECT_ID, gcp_credentials, thresholds)

        # --- THIS IS THE FIX ---
        all_candidates["idle_read_replicas"] = analyze_cloud_sql_read_replicas(PROJECT_ID, gcp_credentials, thresholds)

        all_candidates["idle_load_balancers"] = analyze_load_balancers(PROJECT_ID, gcp_credentials, thresholds)

        # Stage 4: Generate Report and Save to Database
        print("\n--- Step 4: Finalizing Report and Saving Results ---")
        save_optimization_report(all_candidates)

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
