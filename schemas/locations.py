from pydantic import BaseModel
from typing import Optional
from datetime import datetime

class LocationBase(BaseModel):
    kd_propinsi: Optional[str] = None
    kd_kabupaten: Optional[str] = None
    kd_kecamatan: Optional[str] = None
    kd_kelurahan: Optional[str] = None
    nm_propinsi: Optional[str] = None
    nm_kabupaten: Optional[str] = None
    nm_kecamatan: Optional[str] = None
    nm_kelurahan: Optional[str] = None
    geom: dict

class LocationCreate(LocationBase):
    pass

class LocationUpdate(LocationBase):
    pass

class LocationOut(BaseModel):
    kd_propinsi: Optional[str] = None
    kd_kabupaten: Optional[str] = None
    kd_kecamatan: Optional[str] = None
    kd_kelurahan: Optional[str] = None
    nm_propinsi: Optional[str] = None
    nm_kabupaten: Optional[str] = None
    nm_kecamatan: Optional[str] = None
    nm_kelurahan: Optional[str] = None
    category: str

    class Config:
        from_attributes = True  # for SQLAlchemy ORM
