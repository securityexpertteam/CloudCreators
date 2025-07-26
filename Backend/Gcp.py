# ================================================================================
# GCP Resource Optimization Analysis Script
# ================================================================================
# 
# This script analyzes GCP resources to identify optimization opportunities:
# - Storage buckets with low utilization (<20% of quota)
# - VM instances with low CPU usage (<15% average over 7 days)
# - Subnets with high free IP addresses (>90% free, excluding default VPC)
# - Persistent disks with low utilization (uses aggregated list API for maximum speed)
#
# The script generates a detailed JSON report with resource IDs, names, labels,
# and cost analysis for all optimization candidates.
#
# Performance Optimization: Disk analysis uses aggregated list API for maximum speed,
# significantly reducing analysis time for large projects.
#
# Cost Analysis: Integrated cost estimation based on standard GCP pricing for each
# resource type to help prioritize optimization efforts. Note: For production use,
# consider integrating with BigQuery billing export for actual cost data.
# ================================================================================

# Import required Google Cloud libraries
from google.cloud import asset_v1, monitoring_v3
from google.cloud import storage as gcs_storage
from google.oauth2 import service_account
from datetime import datetime, timedelta, UTC
import time
from googleapiclient import discovery
import ipaddress
import json
import os
import os

# MongoDB integration
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
MONGODB_PORT = 27017        # Change this to your MongoDB port
MONGODB_DATABASE = "myDB"   # Change this to your database name
MONGODB_COLLECTION = "Cost_Insights"  # Change this to your collection name

def get_project_id_from_mongodb():
    """
    Fetch the PROJECT_ID from MongoDB users collection.
    
    Returns:
        str: Project ID from ManagementUnit field, or None if not found
    """
    if not MONGODB_AVAILABLE:
        print("❌ pymongo not available. Cannot fetch PROJECT_ID from MongoDB.")
        return None
    
    try:
        # Connect to MongoDB
        client = MongoClient(host=MONGODB_HOST, port=MONGODB_PORT)
        db = client[MONGODB_DATABASE]
        users_collection = db['users']
        
        # Get the latest user record (you can modify this query as needed)
        user_record = users_collection.find_one(sort=[('_id', -1)])  # Get latest record
        
        if user_record and 'ManagementUnit' in user_record:
            project_id = user_record['ManagementUnit']
            print(f"✅ Retrieved PROJECT_ID from MongoDB: {project_id}")
            client.close()
            return project_id
        else:
            print("❌ No ManagementUnit found in users collection")
            client.close()
            return None
            
    except Exception as e:
        print(f"❌ Error fetching PROJECT_ID from MongoDB: {e}")
        return None

def get_email_from_environment_onboarding():
    """
    Fetch the email from the latest entry in environmentOnboarding collection.
    
    Returns:
        str: Email address from the latest record, or "NA" if not found
    """
    if not MONGODB_AVAILABLE:
        print("❌ pymongo not available. Cannot fetch email from environmentOnboarding.")
        return "NA"
    
    try:
        # Connect to MongoDB
        client = MongoClient(host=MONGODB_HOST, port=MONGODB_PORT)
        db = client[MONGODB_DATABASE]
        environment_collection = db['environmentOnboarding']
        
        # Get the latest environment record
        env_record = environment_collection.find_one(sort=[('_id', -1)])  # Get latest record
        
        if env_record and 'email' in env_record:
            email = env_record['email']
            print(f"✅ Retrieved email from environmentOnboarding: {email}")
            client.close()
            return email
        else:
            print("❌ No email found in environmentOnboarding collection")
            client.close()
            return "NA"
            
    except Exception as e:
        print(f"❌ Error fetching email from environmentOnboarding: {e}")
        return "NA"

# Get PROJECT_ID from MongoDB or fallback to hardcoded value
PROJECT_ID = get_project_id_from_mongodb()
if not PROJECT_ID:
    PROJECT_ID = "pro-plasma-465515-k1"  # Fallback to hardcoded value
    print(f"⚠️  Using fallback PROJECT_ID: {PROJECT_ID}")

# Get email from environmentOnboarding collection
USER_EMAIL = get_email_from_environment_onboarding()

CREDENTIALS_PATH = r"C:\Users\dasar\OneDrive\Documents\cloud_optimisation\pro-plasma-465515-k1-833ec1affeb6.json"

# Analysis configuration
DISK_QUOTA_GB = 100  # Disk quota for utilization calculation

# Initialize compute service for subnet and disk analysis
credentials_compute = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH)
compute = discovery.build('compute', 'v1', credentials=credentials_compute)

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
        start_date = end_date - timedelta(days=days)
        
        # Format dates for Cloud Billing API
        date_filter = {
            'start_date': {
                'year': start_date.year,
                'month': start_date.month,
                'day': start_date.day
            },
            'end_date': {
                'year': end_date.year,
                'month': end_date.month,
                'day': end_date.day
            }
        }
        
        # Build request for Cloud Billing export (this requires BigQuery export to be set up)
        # For now, we'll use a placeholder approach since direct billing API has limitations
        # In production, you would typically:
        # 1. Enable Cloud Billing export to BigQuery
        # 2. Query the BigQuery billing export tables
        # 3. Or use Cloud Asset Inventory for cost center labels
        
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
        resource_type (str): Type of resource (vm, disk, bucket, subnet)
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
            
            if 'micro' in machine_type:
                daily_cost = 2.50
            elif 'small' in machine_type:
                daily_cost = 5.00
            elif 'medium' in machine_type:
                daily_cost = 10.00
            else:
                daily_cost = 2.50  # Default for unknown types
            
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
            
            # Pricing per GB per month (simplified)
            if 'ssd' in disk_type or 'pd-ssd' in disk_type:
                monthly_cost_per_gb = 0.17
            elif 'balanced' in disk_type:
                monthly_cost_per_gb = 0.10
            else:  # standard
                monthly_cost_per_gb = 0.04
            
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
            
            # Standard storage pricing (~$0.020 per GB per month)
            monthly_cost_per_gb = 0.020
            daily_cost = (size_gb * monthly_cost_per_gb * days) / 30
            
            cost_data['daily_cost_usd'] = daily_cost / days if days > 0 else 0
            cost_data['total_cost_usd'] = daily_cost
            cost_data['cost_breakdown'] = {
                'storage_cost': cost_data['total_cost_usd'] * 0.85,
                'operations_cost': cost_data['total_cost_usd'] * 0.15
            }
            
        elif resource_type == 'subnet':
            # Subnet/network cost (minimal for most cases)
            cost_data['daily_cost_usd'] = 0.10  # Minimal network cost
            cost_data['total_cost_usd'] = 0.10 * days
            cost_data['cost_breakdown'] = {
                'network_cost': cost_data['total_cost_usd']
            }
        
        return cost_data
        
    except Exception as e:
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

def categorize_gcp_resources(project_id, credentials_path, bucket_quota_gb=100):
    """
    Analyze GCS buckets and identify those with low utilization.
    
    Args:
        project_id (str): GCP project ID
        credentials_path (str): Path to service account credentials
        bucket_quota_gb (int): Bucket quota in GB for utilization calculation
    """
    print(f"\n📦 Analyzing Storage Buckets (Quota: {bucket_quota_gb}GB)")
    print("=" * 60)
    
    credentials = service_account.Credentials.from_service_account_file(credentials_path)
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
                
                if total_bytes is not None:
                    utilization = min((total_bytes / bucket_quota_bytes) * 100, 100.0)
                else:
                    utilization = None
                
                storage.append((resource.asset_type, resource.name, utilization, total_bytes))
                
                if utilization is not None and utilization < 20.0:
                    low_util_storage.append((resource.asset_type, resource.name, utilization, total_bytes))
        
        # Display results
        print(f"Total buckets found: {len(storage)}")
        for asset_type, name, util, size in storage:
            util_str = f"{util:.8f}%" if util is not None else "N/A"
            
            # Smart size formatting (KB, MB, GB)
            if size:
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
        
        print(f"\n🔍 Low Utilization Buckets (<20%): {len(low_util_storage)}")
        if low_util_storage:
            for asset_type, name, util, size in low_util_storage:
                util_str = f"{util:.8f}%" if util is not None else "N/A"
                
                # Smart size formatting (KB, MB, GB)
                if size:
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

def categorize_gcp_vm_cpu_utilization(project_id, credentials_path, threshold=15.0):
    """
    Analyze VM instances and identify those with low CPU utilization.
    
    Args:
        project_id (str): GCP project ID
        credentials_path (str): Path to service account credentials
        threshold (float): CPU utilization threshold percentage
    """
    print(f"\n💻 Analyzing VM Instances (CPU Threshold: {threshold}%)")
    print("=" * 60)
    
    credentials = service_account.Credentials.from_service_account_file(credentials_path)
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
                
                print(f"  • {vm_id} (Zone: {zone}) - {cpu_util:.8f}% CPU" if cpu_util is not None else f"  • {vm_id} (Zone: {zone}) - N/A CPU")
                
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
                print(f"  ⚠️  {vm['vm_id']} - {vm['cpu_util']:.8f}% CPU (Zone: {vm['zone']})")
        else:
            print("  ✅ No low CPU usage VMs found")
            
    except Exception as e:
        print(f"❌ Error analyzing VMs: {e}")

def list_subnets_with_cidr_and_ip_usage(project_id):
    """
    Analyze subnets and identify those with high free IP addresses.
    
    Args:
        project_id (str): GCP project ID
    """
    print(f"\n🌐 Analyzing Subnets (Free IP Threshold: >90%)")
    print("=" * 60)
    
    high_free_subnets = []
    total_subnets = 0
    
    try:
        request = compute.subnetworks().aggregatedList(project=project_id)
        while request is not None:
            response = request.execute()
            for region, region_data in response.get('items', {}).items():
                for subnet in region_data.get('subnetworks', []):
                    total_subnets += 1
                    name = subnet.get('name')
                    cidr = subnet.get('ipCidrRange')
                    network = subnet.get('network').split('/')[-1]
                    vpc_name = network
                    
                    # Skip default VPC subnets
                    if vpc_name == 'default':
                        print(f"  • {name} (Default VPC) - Skipped")
                        continue
                    
                    total_ips = len(list(ipaddress.ip_network(cidr, strict=False).hosts())) if cidr else 0
                    used_ips_count = 0  # Not available from basic API
                    free_ips = total_ips - used_ips_count
                    free_pct = (free_ips / total_ips * 100) if total_ips > 0 else 0
                    
                    print(f"  • {name} (VPC: {vpc_name}) - {free_pct:.8f}% free IPs ({free_ips}/{total_ips})")
                    
                    if free_pct > 90:
                        high_free_subnets.append((name, vpc_name, cidr, total_ips, used_ips_count, free_ips, free_pct))
            
            request = compute.subnetworks().aggregatedList_next(previous_request=request, previous_response=response)
        
        print(f"\nTotal subnets analyzed: {total_subnets}")
        print(f"🔍 High Free IP Subnets (>90%): {len(high_free_subnets)}")
        
        if high_free_subnets:
            for name, vpc_name, cidr, total_ips, used_ips_count, free_ips, free_pct in high_free_subnets:
                print(f"  ⚠️  {name} (VPC: {vpc_name}) - {free_pct:.8f}% free ({free_ips}/{total_ips} IPs)")
        else:
            print("  ✅ No high free IP subnets found")
            
    except Exception as e:
        print(f"❌ Error analyzing subnets: {e}")

def categorize_gcp_disk_utilization(project_id, credentials_path, disk_quota_gb=100):
    """
    Analyze persistent disks and identify those with low utilization based on allocated size.
    Since actual disk usage metrics aren't available via basic Compute API, we identify
    potentially underutilized disks based on small size or specific criteria.
    Uses aggregated list API for maximum speed across all zones.
    
    Args:
        project_id (str): GCP project ID
        credentials_path (str): Path to service account credentials
        disk_quota_gb (int): Size threshold in GB for identifying small disks
    """
    print(f"\n💿 Analyzing Persistent Disks (Small Disk Threshold: <{disk_quota_gb}GB)")
    print("=" * 60)
    print("📋 Identifying potentially underutilized disks based on size and status")
    print("⚡ Using aggregated list API for maximum speed")
    
    credentials = service_account.Credentials.from_service_account_file(credentials_path)
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
            print(f"  • {disk['name']} (Zone: {disk['zone']}) - {disk['size_gb']}GB, {disk['disk_type']}, {attachment_status}{attached_to}")
        
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

# ================================================================================
# JSON REPORT GENERATION
# ================================================================================

def collect_optimization_candidates():
    """
    Collect detailed information about resources that meet optimization criteria.
    
    Returns:
        dict: Dictionary containing optimization candidates with IDs, names, and labels
    """
    print(f"\n📊 Collecting Optimization Candidates for JSON Report...")
    print("=" * 60)
    
    optimization_candidates = {
        "low_utilization_buckets": [],
        "low_cpu_vms": [],
        "high_free_subnets": [],
        "low_utilization_disks": []
    }
    
    # Initialize credentials and clients
    credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH)
    asset_client = asset_v1.AssetServiceClient(credentials=credentials)
    scope = f"projects/{PROJECT_ID}"
    bucket_quota_gb = 100
    bucket_quota_bytes = bucket_quota_gb * 1_000_000_000
    disk_quota_bytes = DISK_QUOTA_GB * 1_000_000_000
    
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
                
                if total_bytes is not None:
                    utilization = min((total_bytes / bucket_quota_bytes) * 100, 100.0)
                    if utilization < 20.0:
                        # Get cost data for bucket
                        cost_data = get_detailed_resource_costs(
                            PROJECT_ID, 
                            'bucket', 
                            {'size_bytes': total_bytes}
                        )
                        
                        # Smart size formatting for JSON
                        if total_bytes < 1_000:
                            size_formatted = f"{total_bytes:.2f}B"
                        elif total_bytes < 1_000_000:
                            size_formatted = f"{total_bytes / 1_000:.2f}KB"
                        elif total_bytes < 1_000_000_000:
                            size_formatted = f"{total_bytes / 1_000_000:.2f}MB"
                        else:
                            size_formatted = f"{total_bytes / 1_000_000_000:.2f}GB"
                        
                        # Extract labels and create structured metadata
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
                            utilization_data=utilization_data
                        )
                        
                        # Update metadata with specific recommendation
                        metadata["Recommendation"] = "Try Merging"
                        
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
                if cpu_util is not None and cpu_util < 15.0:
                    vm_id = resource.name.split("/")[-1]
                    zone = None
                    if 'zones/' in resource.name:
                        zone = resource.name.split("/zones/")[-1].split("/")[0]
                    
                    # Get cost data for VM (estimate based on standard machine types)
                    cost_data = get_detailed_resource_costs(
                        PROJECT_ID, 
                        'vm', 
                        {'machine_type': 'e2-micro'}  # Default assumption for cost estimation
                    )
                    
                    # Extract labels and create structured metadata
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
                        status="Running",  # Assume running since it has CPU metrics
                        cost_analysis=cost_data,
                        utilization_data=utilization_data
                    )
                    
                    # Update metadata with specific recommendation
                    metadata["Recommendation"] = "Scale down"
                    
                    optimization_candidates["low_cpu_vms"].append({
                        "name": vm_id,
                        "full_name": resource.name,
                        "zone": zone,
                        "cpu_utilization_percent": round(cpu_util, 8),
                        "cost_analysis": cost_data,
                        "recommendation": "Scale down",
                        "labels": labels,
                        "resource_metadata": metadata
                    })
    except Exception as e:
        print(f"    ❌ Error collecting VM data: {e}")
    
    # Collect high free IP subnets (>90% free IPs)
    try:
        print("  • Collecting high free IP subnets...")
        request = compute.subnetworks().aggregatedList(project=PROJECT_ID)
        while request is not None:
            response = request.execute()
            for region, region_data in response.get('items', {}).items():
                for subnet in region_data.get('subnetworks', []):
                    name = subnet.get('name')
                    cidr = subnet.get('ipCidrRange')
                    network = subnet.get('network').split('/')[-1]
                    vpc_name = network
                    
                    # Skip default VPC
                    if vpc_name == 'default':
                        continue
                    
                    total_ips = len(list(ipaddress.ip_network(cidr, strict=False).hosts())) if cidr else 0
                    used_ips_count = 0  # Not available from basic API
                    free_ips = total_ips - used_ips_count
                    free_pct = (free_ips / total_ips * 100) if total_ips > 0 else 0
                    
                    if free_pct > 90:
                        # Get cost data for subnet
                        cost_data = get_detailed_resource_costs(
                            PROJECT_ID, 
                            'subnet', 
                            {'total_ips': total_ips}
                        )
                        
                        # Extract labels and create structured metadata
                        labels = subnet.get('labels', {})
                        region_name = region.replace('regions/', '') if 'regions/' in region else region
                        utilization_data = {
                            'free_percent': free_pct
                        }
                        metadata = extract_resource_metadata(
                            labels=labels,
                            resource_name=name,
                            resource_type='subnet',
                            region=region_name,
                            full_name=subnet.get('selfLink'),
                            status="Available",
                            cost_analysis=cost_data,
                            utilization_data=utilization_data
                        )
                        
                        # Update metadata with specific recommendation
                        metadata["Recommendation"] = "Scale down"
                        
                        optimization_candidates["high_free_subnets"].append({
                            "name": name,
                            "vpc_name": vpc_name,
                            "cidr": cidr,
                            "region": region.replace('regions/', '') if 'regions/' in region else region,
                            "total_ips": total_ips,
                            "free_ips": free_ips,
                            "free_percent": round(free_pct, 8),
                            "cost_analysis": cost_data,
                            "recommendation": "Scale down",
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
        
        # Use aggregated list for fastest disk collection
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
                        
                        # Check if disk is attached to any instances
                        users = disk.get('users', [])
                        is_attached = len(users) > 0
                        
                        # Consider disks as potentially underutilized if they are:
                        # 1. Small (< threshold GB) OR
                        # 2. Not attached to any instance OR  
                        # 3. Status is not READY
                        if size_gb < DISK_QUOTA_GB or not is_attached or status != 'READY':
                            reasons = []
                            if size_gb < DISK_QUOTA_GB:
                                reasons.append(f"small_size")
                            if not is_attached:
                                reasons.append("unattached")
                            if status != 'READY':
                                reasons.append("not_ready")
                            
                            # Get cost data for disk
                            cost_data = get_detailed_resource_costs(
                                PROJECT_ID, 
                                'disk', 
                                {'size_gb': size_gb, 'disk_type': disk_type}
                            )
                            
                            # Generate recommendation based on disk status and attachment
                            recommendation = "Scale down"
                            

                            # Extract labels and create structured metadata
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
                                utilization_data=utilization_data
                            )
                            
                            # Update metadata with specific recommendation
                            metadata["Recommendation"] = "Scale down"
                            
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
                                "recommendation": "Scale down",
                                "labels": disk.get('labels', {}),
                                "resource_metadata": metadata
                            })
            
            req = compute.disks().aggregatedList_next(previous_request=req, previous_response=resp)
    except Exception as e:
        print(f"    ❌ Error collecting disk data: {e}")
    
    # Print collection summary
    print(f"  ✅ Collected {len(optimization_candidates['low_utilization_buckets'])} low utilization buckets")
    print(f"  ✅ Collected {len(optimization_candidates['low_cpu_vms'])} low CPU VMs")
    print(f"  ✅ Collected {len(optimization_candidates['high_free_subnets'])} high free IP subnets")
    print(f"  ✅ Collected {len(optimization_candidates['low_utilization_disks'])} potentially underutilized disks")
    
    return optimization_candidates

def save_optimization_report():
    """
    Generate and save MongoDB-ready resource records as JSON array.
    """
    print(f"\n💾 Generating MongoDB-Ready Resource Records...")
    print("=" * 60)
    
    # Collect optimization candidates
    candidates = collect_optimization_candidates()
    
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
    
    # Save to JSON file (always overwrite existing)
    output_file = "gcp_optimization.json"
    try:
        # Check if file exists and notify about replacement
        import os
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
        
        print(f"\n📋 Resource Breakdown:")
        print(f"  • Storage Buckets: {bucket_count}")
        print(f"  • Compute Instances: {vm_count}")
        print(f"  • Network Subnets: {subnet_count}")
        print(f"  • Storage Disks: {disk_count}")
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
        existing_count = collection.count_documents({})
        if existing_count > 0:
            collection.delete_many({})
            print(f"🗑️  Cleared {existing_count} existing records from optimization collection")
        else:
            print("📝 Collection is empty, no records to clear")
        
        # Add timestamp to each record
        for record in records:
            record['InsertedAt'] = datetime.now(UTC).isoformat()
        
        # Insert all records
        result = collection.insert_many(records)
        print(f"✅ Successfully inserted {len(result.inserted_ids)} records into MongoDB")
        print(f"📍 Database: {MONGODB_DATABASE}, Collection: {MONGODB_COLLECTION}")
        print(f"📍 MongoDB Server: {MONGODB_HOST}:{MONGODB_PORT}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error inserting into MongoDB: {e}")
        return False
        
    finally:
        # Close MongoDB connection in all cases
        try:
            client.close()
            print("✅ MongoDB connection closed successfully")
        except Exception as e:
            print(f"⚠️  Warning: Error closing MongoDB connection: {e}")

# ================================================================================
# METADATA EXTRACTION
# ================================================================================

def extract_resource_metadata(labels, resource_name, resource_type, region=None, zone=None, full_name=None, status=None, cost_analysis=None, utilization_data=None):
    """
    Extract Azure-style metadata from GCP resource data and labels.
    
    Args:
        labels (dict): Resource labels from GCP
        resource_name (str): Name of the resource
        resource_type (str): Type of resource (vm, bucket, disk, subnet)
        region (str, optional): Resource region
        zone (str, optional): Resource zone
        full_name (str, optional): Full resource name/path
        status (str, optional): Resource status
        cost_analysis (dict, optional): Cost analysis data
        utilization_data (dict, optional): Contains utilization metrics for generating findings
    
    Returns:
        dict: Structured metadata matching Azure format
    """
    # Helper function to safely get label value
    def get_label_value(key, default="NA"):
        return labels.get(key, default) if labels else default
    
    # Extract region from resource ID or use provided region/zone
    extracted_region = region or zone or "NA"
    if full_name and not extracted_region:
        # Try to extract region/zone from full resource name
        if "/zones/" in full_name:
            extracted_region = full_name.split("/zones/")[1].split("/")[0]
        elif "/regions/" in full_name:
            extracted_region = full_name.split("/regions/")[1].split("/")[0]
    
    # Determine ResourceType based on GCP resource type
    resource_type_mapping = {
        'vm': 'Compute',
        'bucket': 'Storage', 
        'disk': 'Storage',
        'subnet': 'Networking'
    }
    
    # Determine SubResourceType - actual GCP resource types
    sub_resource_mapping = {
        'vm': 'Virtual Machine',
        'bucket': 'bucket',
        'disk': 'Disk',
        'subnet': 'Subnet'
    }
    
    # Get total cost from cost analysis - remove unnecessary decimal places and handle scientific notation
    total_cost = "Unknown"
    if cost_analysis and 'total_cost_usd' in cost_analysis:
        cost_value = cost_analysis['total_cost_usd']
        if cost_value == 0:
            total_cost = "$0"
        elif cost_value == int(cost_value):  # If it's a whole number
            total_cost = f"${int(cost_value)}"
        elif cost_value < 0.01:  # For very small values, show as $0
            total_cost = "$0"
        else:
            # Round to 2 decimal places to avoid scientific notation
            total_cost = f"${cost_value:.2f}".rstrip('0').rstrip('.')
            if total_cost == "$":
                total_cost = "$0"
    
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
    
    # Generate simplified finding and recommendation based on resource type
    finding = "Resource identified for optimization"
    recommendation = "Review usage and consider optimization"
    
    if resource_type == 'vm':
        finding = "VM underutilised"
        recommendation = "Scale down"
    elif resource_type == 'bucket':
        finding = "Bucket underutilised"
        recommendation = "Try Merging"
    elif resource_type == 'subnet':
        finding = "Subnet underutilised"
        recommendation = "Scale down"
    elif resource_type == 'disk':
        finding = "Disk underutilised"
        recommendation = "Scale down"
    
    return {
        "_id": full_name or f"//cloudresourcemanager.googleapis.com/projects/{PROJECT_ID}/resources/{resource_name}",
        "CloudProvider": "gcp",
        "ManagementUnits": PROJECT_ID,
        "ApplicationCode": get_label_value("applicationcode"),
        "CostCenter": get_label_value("costcenter"),
        "Owner": get_label_value("owner"),
        "TicketId": "NA",
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
    
    try:
        # Run comprehensive resource analysis
        categorize_gcp_resources(PROJECT_ID, CREDENTIALS_PATH, bucket_quota_gb=100)
        categorize_gcp_vm_cpu_utilization(PROJECT_ID, CREDENTIALS_PATH, threshold=15.0)
        list_subnets_with_cidr_and_ip_usage(PROJECT_ID)
        
        # Run disk analysis
        categorize_gcp_disk_utilization(PROJECT_ID, CREDENTIALS_PATH, disk_quota_gb=DISK_QUOTA_GB)
        
        # Generate and save detailed JSON report
        save_optimization_report()
        
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
