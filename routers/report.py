from typing import Optional

from fastapi import APIRouter, Query, Depends
from sqlalchemy.orm import Session

from crud.report import get_report_data, get_download_report, get_all_document, get_report_statistics
from database import get_db, get_llm

router = APIRouter(prefix="/report", tags=['report'])


@router.get("/generate")
def generate_report(db: Session = Depends(get_db), client = Depends(get_llm), location_id: str = Query(), start_date: str = Query(), end_date: str = Query(),
                          title: str = Query(default=None)):
    results = get_report_data(db, client, location_id, start_date, end_date, title)
    return results


@router.get('/download')
def download_report(db: Session = Depends(get_db), id: int = Query(), url: str = Query()):
    results = get_download_report(db, id, url)
    return results


@router.get('/get-all')
def get_all_report(db: Session = Depends(get_db), page: int = Query(), limit: int = Query()):
    results = get_all_document(db, page, limit)
    return results

@router.get("/statistics")
def get_all_report_statistics(db: Session = Depends(get_db)):
    return get_report_statistics(db)