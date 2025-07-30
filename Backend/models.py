from pydantic import BaseModel,EmailStr
from typing import Optional
from typing import List
from datetime import datetime

class User(BaseModel):
    cloudName: str
    environment: str
    rootId: str
    managementUnitId: str
    srvaccntName: str
    srvacctPass: str
    email: str = None
    gcpJsonFile: Optional[str] = None  # <-- Add this field

class Trigger(BaseModel):
    email: str
    ScheduledTimeStamp: datetime
    ScanCompletedTime: Optional[datetime] = None


class BulkSignupRequest(BaseModel):
    users: List[User]
    email: str  

class Resource(BaseModel):
    _id: str
    CloudProvider: str
    ManagementUnits: str
    ApplicationCode: str
    CostCenter: str
    CIO: str
    Platform: str
    Lab: str
    Feature: str
    Owner: str
    TicketId: str
    ResourceType: str
    SubResourceType: str
    ResourceName: str
    Region: str
    TotalCost: float
    Currency: str
    Current_Size: str
    Finding: str
    Recommendation: str
    Environment: str
    Timestamp: str
    ConfidenceScore: str
    Status: str
    Entity: str
    RootId: str
    Email: Optional[str] = None

# class StandardConfig(BaseModel):
#     email: str  # <-- Store user's email with every config!
#     type: str

#     # Fields for Compute Engine
#     cpu_usage: Optional[int] = None
#     memory_usage: Optional[int] = None
#     network_usage: Optional[int] = None

#     # Fields for Kubernetes
#     node_cpu_percentage: Optional[int] = None
#     node_memory_percentage: Optional[int] = None
#     node_count: Optional[int] = None
#     volume_percentage: Optional[int] = None

#     # Fields for Cloud Storage
#     storage_size: Optional[int] = None
#     access_frequency: Optional[str] = None
#     network_egress: Optional[int] = None
#     lifecycle_enabled: Optional[bool] = None

#     # Fields for General Configuration
#     untagged: Optional[bool] = None
#     orphaned: Optional[bool] = None

class StandardConfig(BaseModel):
    email: str
    type: str

    # Compute Engine
    cmp_cpu_usage: Optional[int] = None
    cmp_memory_usage: Optional[int] = None
    cmp_network_usage: Optional[int] = None

    # Kubernetes
    k8s_node_cpu_percentage: Optional[int] = None
    k8s_node_memory_percentage: Optional[int] = None
    k8s_node_count: Optional[int] = None
    k8s_volume_percentage: Optional[int] = None

    # Cloud Storage
    storage_size: Optional[int] = None
    access_frequency: Optional[str] = None
    network_egress: Optional[int] = None
    lifecycle_enabled: Optional[bool] = None

    # General
    untagged: Optional[bool] = None
    orphaned: Optional[bool] = None

class SigninUser(BaseModel):
    email: str
    password: str
    
class SignupUser(BaseModel):
    firstname: str
    lastname: str
    email: EmailStr
    password: str   