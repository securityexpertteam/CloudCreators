from pydantic import BaseModel
from typing import Optional
from typing import List

class User(BaseModel):
    cloudName: str
    environment: str
    rootId: str
    managementUnitId: str
    srvaccntName: str
    srvacctPass: str
    LoginId: str = None

class BulkSignupRequest(BaseModel):
    users: List[User]
    login_id: str  

class Resource(BaseModel):
    resource_id: str
    provider: str
    resource_type: str
    cpu_usage: int
    memory_usage: int
    network_usage: int
    scale_down_recommendation: str
    untagged_instances: str
    orphaned_vms: int

class StandardConfig(BaseModel):
    email: str  # <-- Store user's email with every config!
    type: str

    # Fields for Compute Engine
    cpu_usage: Optional[int] = None
    memory_usage: Optional[int] = None
    network_usage: Optional[int] = None

    # Fields for Kubernetes
    node_cpu_percentage: Optional[int] = None
    node_memory_percentage: Optional[int] = None
    node_count: Optional[int] = None
    volume_percentage: Optional[int] = None

    # Fields for Cloud Storage
    storage_size: Optional[int] = None
    access_frequency: Optional[str] = None
    network_egress: Optional[int] = None
    lifecycle_enabled: Optional[bool] = None

    # Fields for General Configuration
    untagged: Optional[bool] = None
    orphaned: Optional[bool] = None

class SignupUser(BaseModel):
    email: str
    password: str