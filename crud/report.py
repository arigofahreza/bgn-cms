from datetime import datetime
from io import BytesIO

from sqlalchemy import select, desc, func, and_
from sqlalchemy.orm import Session
from fastapi.responses import FileResponse
from sqlalchemy import func, cast, Date
from datetime import date, timedelta
import matplotlib.pyplot as plt
import pandas as pd
from models.report import ReportMetadata
from models import locations as l_models, ReportUser, User


def get_report_data(db: Session, client, location_id: str, start_date: str, end_date: str, title: str = None):
    location_name = []
    split_location_id = location_id.split('.')
    if len(split_location_id) == 1:
        location_result = (db.query(
            l_models.Propinsi.nm_propinsi,
        )
        .filter(
            and_(
                l_models.Propinsi.kd_propinsi == split_location_id[0]
            )
        )
        .first())
        if location_result:
            location_name.append(location_result.nm_propinsi)
    elif len(split_location_id) == 2:
        location_result = (
            db.query(
                l_models.Kabupaten.nm_kabupaten,
                l_models.Propinsi.nm_propinsi,
            )
            .join(
                l_models.Propinsi,
                and_(
                    l_models.Kabupaten.kd_propinsi == l_models.Propinsi.kd_propinsi
                )
            )
            .filter(
                and_(
                    l_models.Kabupaten.kd_kabupaten == split_location_id[1],
                    l_models.Kabupaten.kd_propinsi == split_location_id[0]
                )
            )
            .first()
        )
        if location_result:
            location_name.append(location_result.nm_propinsi)
            location_name.append(location_result.nm_kabupaten)
    elif len(split_location_id) == 3:
        location_result = (
            db.query(
                l_models.Kecamatan.nm_kecamatan,
                l_models.Kabupaten.nm_kabupaten,
                l_models.Propinsi.nm_propinsi,
            )
            .join(
                l_models.Kabupaten,
                and_(
                    l_models.Kecamatan.kd_kabupaten == l_models.Kabupaten.kd_kabupaten,
                    l_models.Kecamatan.kd_propinsi == l_models.Kabupaten.kd_propinsi
                )
            )
            .join(
                l_models.Propinsi,
                and_(
                    l_models.Kecamatan.kd_propinsi == l_models.Propinsi.kd_propinsi
                )
            )
            .filter(
                and_(
                    l_models.Kecamatan.kd_kecamatan == split_location_id[2],
                    l_models.Kecamatan.kd_kabupaten == split_location_id[1],
                    l_models.Kecamatan.kd_propinsi == split_location_id[0]
                )
            )
            .first()
        )
        if location_result:
            location_name.append(location_result.nm_propinsi)
            location_name.append(location_result.nm_kabupaten)
            location_name.append(location_result.nm_kecamatan)
    elif len(split_location_id) == 4:
        location_result = (
            db.query(
                l_models.Kelurahan.nm_kelurahan,
                l_models.Kecamatan.nm_kecamatan,
                l_models.Kabupaten.nm_kabupaten,
                l_models.Propinsi.nm_propinsi,
            )
            .join(
                l_models.Kecamatan,
                and_(
                    l_models.Kelurahan.kd_kecamatan == l_models.Kecamatan.kd_kecamatan,
                    l_models.Kelurahan.kd_kabupaten == l_models.Kecamatan.kd_kabupaten,
                    l_models.Kelurahan.kd_propinsi == l_models.Kecamatan.kd_propinsi
                )
            )
            .join(
                l_models.Kabupaten,
                and_(
                    l_models.Kelurahan.kd_kabupaten == l_models.Kabupaten.kd_kabupaten,
                    l_models.Kelurahan.kd_propinsi == l_models.Kabupaten.kd_propinsi
                )
            )
            .join(
                l_models.Propinsi,
                and_(
                    l_models.Kelurahan.kd_propinsi == l_models.Propinsi.kd_propinsi
                )
            )
            .filter(
                and_(
                    l_models.Kelurahan.kd_kelurahan == split_location_id[3],
                    l_models.Kelurahan.kd_kecamatan == split_location_id[2],
                    l_models.Kelurahan.kd_kabupaten == split_location_id[1],
                    l_models.Kelurahan.kd_propinsi == split_location_id[0]
                )
            )
            .first()
        )
        if location_result:
            location_name.append(location_result.nm_propinsi)
            location_name.append(location_result.nm_kabupaten)
            location_name.append(location_result.nm_kecamatan)
            location_name.append(location_result.nm_kabupaten)
    location_name_str = ', '.join(location_name)

    total = db.query(ReportUser).count()
    now = datetime.now()
    total_report_with_period = db.query(ReportUser).filter(
        ReportUser.when >= datetime.strptime(start_date, '%Y-%m-%d'),
        ReportUser.when <= datetime(datetime.strptime(end_date, '%Y-%m-%d'))
    ).count()

    ## ============= Trend Report =====================
    query = db.query(cast(ReportUser.when, Date), func.count(ReportUser.id))
    query = query.filter(
        cast(ReportUser.when, Date) >= datetime.strptime(start_date, '%Y-%m-%d'),
        cast(ReportUser.when, Date) <= datetime(datetime.strptime(end_date, '%Y-%m-%d'))
    )
    trend_report_results = query.group_by(cast(ReportUser.when, Date)).all()
    json_trend_report_results = [row.__dict__ for row in trend_report_results]
    df_trend_report = pd.DataFrame(json_trend_report_results)
    fig, ax = plt.subplots()
    ax.plot(df_trend_report['when'], df_trend_report['jumlah'], marker='o')
    ax.set_title(f"Jumlah Report Periode {start_date} - {end_date}")
    ax.set_xlabel("Tanggal")
    ax.set_ylabel("Jumlah")
    plt.xticks(rotation=45)
    plt.tight_layout()

    trend_img_stream = BytesIO()
    plt.savefig(trend_img_stream, format='png')
    trend_img_stream.seek(0)

    md_trend_data = df_trend_report.to_markdown(index=False)
    response_trend = client.chat.completions.create(
        model="gpt-4.0",
        messages=[
            {"role": "system",
             "content": "Kamu adalah analis intelijen yang memberikan insight insiden berdasarkan data yang diberikan."},
            {"role": "user",
             "content": f"Berikan deskripsi yang menjelaskan dari trend data berikut:\n{md_trend_data}. buat dalam bentuk 1 paragraf text"}
        ],
        temperature=0.5
    )

    ## =================== Kategori Laporan ===================

    results_category = (
        db.query(
            ReportUser.category,
            func.count(ReportUser.id).label("total")
        )
        .filter(
            cast(ReportUser.when, Date) >= datetime.strptime(start_date, '%Y-%m-%d'),
            cast(ReportUser.when, Date) <= datetime(datetime.strptime(end_date, '%Y-%m-%d'))
        )
        .group_by(ReportUser.category)
        .all()
    )

    json_category_results = [row.__dict__ for row in trend_report_results]
    df_category = pd.DataFrame(json_category_results)
    md_trend_data = df_category.to_markdown(index=False)
    response_category = client.chat.completions.create(
        model="gpt-4.0",
        messages=[
            {"role": "system",
             "content": "Kamu adalah analis intelijen yang memberikan insight insiden berdasarkan data yang diberikan."},
            {"role": "user",
             "content": f"Berikan deskripsi yang menjelaskan dari data berikut:\n{md_trend_data}. buat dalam bentuk 1 paragraf text"}
        ],
        temperature=0.5
    )

    ## =========== Laporan Per Lokasi =========================

    results_report_per_locations = (
        db.query(
            ReportUser.summary,
            ReportUser.category,
            User.name,
            User.location_id
        )
        .join(User, User.phone == ReportUser.created_by_phone)
        .filter(
            User.location_id.like(location_id),
            cast(ReportUser.when, Date) >= datetime.strptime(start_date, '%Y-%m-%d'),
            cast(ReportUser.when, Date) <= datetime(datetime.strptime(end_date, '%Y-%m-%d'))
        )
        .all()
    )
    json_report_per_locations = [row.__dict__ for row in trend_report_results]
    df_report_per_location = pd.DataFrame(json_report_per_locations)
    md_report_per_locaton_data = df_report_per_location.to_markdown(index=False)
    response_report_per_location = client.chat.completions.create(
        model="gpt-4.0",
        messages=[
            {"role": "system",
             "content": "Kamu adalah analis intelijen yang memberikan insight insiden berdasarkan data yang diberikan."},
            {"role": "user",
             "content": f"Berikan deskripsi per location_id yang menjelaskan dari data berikut:\n{md_report_per_locaton_data}. buat dalam bentuk 1 paragraf text dan bullet per location_id"}
        ],
        temperature=0.5
    )

    ## =============== Summary Laporan ===================
    df_summary = df_report_per_location[['summary', 'category']]
    md_summary_data = df_summary.to_markdown(index=False)
    response_summary = client.chat.completions.create(
        model="gpt-4.0",
        messages=[
            {"role": "system",
             "content": "Kamu adalah analis intelijen yang memberikan insight insiden berdasarkan data yang diberikan."},
            {"role": "user",
             "content": f"Berikan deskripsi secara keseluruhan dari data berikut:\n{md_summary_data}. buat dalam bentuk 1 paragraf text"}
        ],
        temperature=0.5
    )

    ## =============  Top Kontributor =======================
    query_contributor = db.query(User.name, func.count(ReportUser.id).label("total"))
    results = (
        query_contributor.join(
            User,
            (User.phone == ReportUser.created_by_phone)
        ).group_by(User.name)
        .order_by(desc("total"))
        .limit(5)
        .all()
    )
    contributors = [{"name": name, "total": total} for name, total in results]
    df_contributor = pd.DataFrame(contributors)
    md_contributor_data = df_contributor.to_markdown(index=False)

    fig, ax = plt.subplots(figsize=(8, 4))
    ax.bar(df_contributor["name"], df_contributor["total"], color='lightcoral')

    # Labels and titles
    ax.set_title("Jumlah Laporan per Contributor", fontsize=12)
    ax.set_xlabel("Nama")
    ax.set_ylabel("Total")
    plt.xticks(rotation=30, ha='right')
    plt.tight_layout()

    # Step 3: Save as binary image
    contributor_img_stream = BytesIO()
    plt.savefig(contributor_img_stream, format='png')
    contributor_img_stream.seek(0)

    response_contributor = client.chat.completions.create(
        model="gpt-4.0",
        messages=[
            {"role": "system",
             "content": "Kamu adalah analis intelijen yang memberikan insight insiden berdasarkan data yang diberikan."},
            {"role": "user",
             "content": f"Berikan deskripsi yang menjelaskan dari data berikut:\n{md_contributor_data}. buat dalam bentuk 1 paragraf text"}
        ],
        temperature=0.5
    )

    ## ============= Trend Kontributor ====================
    # Get top 5 contributors


    pass




def get_download_report(db: Session, id: int, url: str):
    result = db.query(select(ReportMetadata).where(
        and_(
            ReportMetadata.id == id,
        )
    )).first()
    json_result = result.__dict__
    url_data = json_result.get('url')
    if url_data != url:
        return {}
    filename = json_result.get('title')
    return FileResponse(
        path=f'./report/{filename}',
        filename=filename,
        media_type='application/octet-stream'
    )

def get_all_docummment(db: Session, page: int = 1, limit: int = 10):
    offset = (page - 1) * limit

    results = db.query(
        select(ReportMetadata)
        .order_by(desc(ReportMetadata.generated_at))
        .limit(limit)
        .offset(offset)
    ).all()

    total = db.query(ReportMetadata).count()

    return {
        "page": page,
        "limit": limit,
        "total": total,
        "total_page": (total + limit - 1) // limit,
        "data": [row.__dict__ for row in results]
    }