# models.py
from pydantic import BaseModel
from typing import List
from datetime import datetime

class User(BaseModel):
    username: str
    password: str
    cloudName: str
    project: str
    environment: str


# backend/models.py



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


# class Resource(BaseModel):
#     ResourceType: str
#     SubResourceType: str
#     ResourceName: str
#     Region: str
#     TotalCost: float
#     Currency: str
#     Finding: str
#     Recommendation: str
#     Environment: str
#     Timestamp: datetime
#     Tags: List[str]
#     ConfidenceScore: float
#     Status: str
#     Entity: str