from pydantic import BaseModel
from typing import Optional
from typing import List
from datetime import datetime

class User(BaseModel):
    username: str
    password: str
    cloudName: str
    project: str
    environment: str

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

# ✅ New model for /config form submissions


class StandardConfig(BaseModel):
    cpu_usage: Optional[int] = None
    memory_usage: Optional[int] = None
    network_usage: Optional[int] = None
    untagged: Optional[bool] = False
    orphaned: Optional[bool] = False
