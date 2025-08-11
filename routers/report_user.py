from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from database import get_db

router = APIRouter(
    tags=["Dashboard"],
)

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from database import get_db

from crud.report_user import get_report_user_statistics, get_total_by_category, get_total_by_date_last_14_days, get_total_by_created_by, get_trend_contributor

@router.get("/report_user/trend_contributor")
def report_user_trend_contributor(
    db: Session = Depends(get_db),
    category: str = Query(None, description="Optional category filter")
):
    return get_trend_contributor(db, category)

@router.get("/report_user/total_by_created_by")
def report_user_total_by_created_by(
    db: Session = Depends(get_db),
    category: str = Query(None, description="Optional category filter")
):
    return get_total_by_created_by(db, category)

@router.get("/report_user/statistics")
def report_user_statistics(db: Session = Depends(get_db)):
    return get_report_user_statistics(db)


@router.get("/report_user/total_by_category")
def report_user_total_by_category(db: Session = Depends(get_db)):
    return get_total_by_category(db)


@router.get("/report_user/total_by_date_last_14_days")
def report_user_total_by_date_last_14_days(
    db: Session = Depends(get_db),
    category: str = Query(None, description="Optional category filter")
):
    return get_total_by_date_last_14_days(db, category)
