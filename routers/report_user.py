from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from database import get_db

from crud.report_user import get_report_user_statistics, get_total_by_category, get_total_by_date_last_14_days, \
    get_total_by_created_by, get_trend_contributor, get_heatmap, get_wordcloud, get_total_by_sentiment, get_desc_data, \
    get_sentiment_category

router = APIRouter(
    tags=["Dashboard"],
)


@router.get("/report_user/wordcloud")
def report_user_wordcloud(
        db: Session = Depends(get_db),
        category: str = Query(None, description="Optional category filter")
):
    return get_wordcloud(db, category)


@router.get("/report_user/heatmap")
def report_user_heatmap(
        db: Session = Depends(get_db),
        category: str = Query(None, description="Optional category filter")
):
    return get_heatmap(db, category)


@router.get("/report_user/trend_contributor")
def report_user_trend_contributor(
        db: Session = Depends(get_db),
        category: str = Query(None, description="Optional category filter")
):
    return get_trend_contributor(db, category)


@router.get("/report_user/total_by_created_by")
def report_user_total_by_created_by(
        db: Session = Depends(get_db),
        category: str = Query(None, description="Optional category filter"),
        start_date: str = Query(None, description="Optional start date range"),
        end_date: str = Query(None, description="Optional end date range"),
        kd_propinsi: str = Query(None, description="Optional kode propinsi"),
        kd_kabupaten: str = Query(None, description="Optional kode kabupaten"),
        kd_kecamatan: str = Query(None, description="Optional kode kecamatan"),
        kd_kelurahan: str = Query(None, description="Optional kode kelurahan"),
):
    return get_total_by_created_by(db, category, start_date, end_date, kd_propinsi, kd_kabupaten, kd_kecamatan,
                                   kd_kelurahan)


@router.get("/report_user/statistics")
def report_user_statistics(db: Session = Depends(get_db),
                           kd_propinsi: str = Query(None, description="Optional kode propinsi"),
                           kd_kabupaten: str = Query(None, description="Optional kode kabupaten"),
                           kd_kecamatan: str = Query(None, description="Optional kode kecamatan"),
                           kd_kelurahan: str = Query(None, description="Optional kode kelurahan"),
                           ):
    return get_report_user_statistics(db, kd_propinsi, kd_kabupaten, kd_kecamatan, kd_kelurahan)


@router.get("/report_user/total_by_category")
def report_user_total_by_category(db: Session = Depends(get_db),
                                  start_date: str = Query(None, description="Optional start date range"),
                                  end_date: str = Query(None, description="Optional end date range"),
                                  kd_propinsi: str = Query(None, description="Optional kode propinsi"),
                                  kd_kabupaten: str = Query(None, description="Optional kode kabupaten"),
                                  kd_kecamatan: str = Query(None, description="Optional kode kecamatan"),
                                  kd_kelurahan: str = Query(None, description="Optional kode kelurahan")
                                  ):
    return get_total_by_category(db, start_date, end_date, kd_propinsi, kd_kabupaten, kd_kecamatan, kd_kelurahan)


@router.get("/report_user/total_by_date_last_14_days")
def report_user_total_by_date_last_14_days(
        db: Session = Depends(get_db),
        category: str = Query(None, description="Optional category filter")
):
    return get_total_by_date_last_14_days(db, category)


@router.get("/report_user/total_by_sentiment")
def report_user_total_by_sentiment(
        db: Session = Depends(get_db),
        category: str = Query(None, description="Optional category filter")
):
    return get_total_by_sentiment(db, category)


@router.get("/report_user/desc_data")
def report_user_desc_data(
        db: Session = Depends(get_db),
        category: str = Query(None, description="Optional category filter")
):
    return get_desc_data(db, category)


@router.get("/report_user/sentiment_category")
def report_user_total_by_sentiment(
        db: Session = Depends(get_db),
        category: str = Query(None, description="Optional category filter")
):
    return get_sentiment_category(db, category)
