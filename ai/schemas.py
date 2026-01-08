from pydantic import BaseModel
from typing import List, Dict, Optional

class Cluster(BaseModel):
    название: str
    возраст: str
    интересы: List[str]
    предпочтения: List[str]
    мотивация: List[str]

class Event_ML(BaseModel):
    id: int
    title: str
    description: str
    category: Optional[str] = None
    tags: List[str] = []
    age_restriction: Optional[str] = None  #  for example: "18+"
    status_ml: str