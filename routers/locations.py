from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
import database
from schemas import locations as schemas
from crud import locations as crud

router = APIRouter(prefix="/locations", tags=["Locations"])


@router.get("/list", response_model=list[schemas.LocationOut], response_model_exclude_none=True)
def list_locations(kd_propinsi: Optional[str] = Query(default=None),
                   kd_kabupaten: Optional[str] = Query(default=None),
                   kd_kecamatan: Optional[str] = Query(default=None),
                   db: Session = Depends(database.get_db)):
    return crud.get_locations(db, kd_propinsi, kd_kabupaten, kd_kecamatan)


@router.get("/select", response_model=schemas.LocationOut, response_model_exclude_none=True)
def read_location(kd_propinsi: Optional[str] = Query(default=None),
                  kd_kabupaten: Optional[str] = Query(default=None),
                  kd_kecamatan: Optional[str] = Query(default=None),
                  kd_kelurahan: Optional[str] = Query(default=None),
                  db: Session = Depends(database.get_db)):
    user = crud.get_location(db, kd_propinsi, kd_kabupaten, kd_kecamatan, kd_kelurahan)
    if not user:
        raise HTTPException(status_code=404, detail="Location not found")
    return user


@router.post("/create", response_model=schemas.LocationOut, response_model_exclude_none=True)
def create_location(user: schemas.LocationCreate, db: Session = Depends(database.get_db)):
    return crud.create_location(db, user)


@router.delete("/delete", response_model=schemas.LocationOut, response_model_exclude_none=True)
def delete_location(kd_propinsi: Optional[str] = Query(default=None),
                    kd_kabupaten: Optional[str] = Query(default=None),
                    kd_kecamatan: Optional[str] = Query(default=None),
                    kd_kelurahan: Optional[str] = Query(default=None),
                    db: Session = Depends(database.get_db)):
    deleted = crud.delete_location(db, kd_propinsi, kd_kabupaten, kd_kecamatan, kd_kelurahan)
    if not deleted:
        raise HTTPException(status_code=404, detail="Location not found")
    return deleted
