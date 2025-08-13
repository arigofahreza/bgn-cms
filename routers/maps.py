from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
import database
from crud import maps as crud

router = APIRouter(prefix="/maps", tags=["Maps"])


@router.get("/geometry")
def geometry(kd_propinsi: Optional[str] = Query(default=None),
             kd_kabupaten: Optional[str] = Query(default=None),
             kd_kecamatan: Optional[str] = Query(default=None),
             kd_kelurahan: Optional[str] = Query(default=None),
             db: Session = Depends(database.get_db)):
    return crud.get_geometry(db, kd_propinsi, kd_kabupaten, kd_kecamatan, kd_kelurahan)


@router.get("/centroid")
def geometry(kd_propinsi: Optional[str] = Query(default=None),
             kd_kabupaten: Optional[str] = Query(default=None),
             kd_kecamatan: Optional[str] = Query(default=None),
             kd_kelurahan: Optional[str] = Query(default=None),
             db: Session = Depends(database.get_db)):
    return crud.get_centroid(db, kd_propinsi, kd_kabupaten, kd_kecamatan, kd_kelurahan)
