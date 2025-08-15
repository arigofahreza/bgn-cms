from datetime import datetime
from io import BytesIO

from docx.shared import Mm
from docxtpl import DocxTemplate, InlineImage
from sqlalchemy import select, desc, func, and_, insert
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
    total_report_with_period = db.query(ReportUser).filter(
        ReportUser.when >= datetime.strptime(start_date, '%Y-%m-%d'),
        ReportUser.when <= datetime.strptime(end_date, '%Y-%m-%d')
    ).count()

    ## ============= Trend Report =====================
    query = db.query(cast(ReportUser.when, Date), func.count(ReportUser.id))
    query = query.filter(
        cast(ReportUser.when, Date) >= datetime.strptime(start_date, '%Y-%m-%d'),
        cast(ReportUser.when, Date) <= datetime.strptime(end_date, '%Y-%m-%d')
    )
    trend_report_results = query.group_by(cast(ReportUser.when, Date)).order_by(
        cast(ReportUser.when, Date).desc()).all()
    json_trend_report_results = [
        {"date": date_val, "total": total} for date_val, total in trend_report_results
    ]
    df_trend_report = pd.DataFrame(json_trend_report_results)

    fig, ax = plt.subplots()
    ax.plot(df_trend_report['date'], df_trend_report['total'], marker='o')
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
        model="gpt-4o",
        messages=[
            {"role": "system",
             "content": "Kamu adalah analis intelijen yang memberikan insight berdasarkan data yang diberikan. bahasa yang kamu gunakan adalah bahasa yang formal dan to the point"},
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
            cast(ReportUser.when, Date) <= datetime.strptime(end_date, '%Y-%m-%d')
        )
        .group_by(ReportUser.category)
        .all()
    )

    json_category_results = [
        {"category": category, "total": total}
        for category, total in results_category
    ]
    category_dict = {
        row.category: row.total
        for row in results_category
    }
    df_category = pd.DataFrame(json_category_results)
    md_trend_data = df_category.to_markdown(index=False)
    response_category = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system",
             "content": "Kamu adalah analis intelijen yang memberikan insight berdasarkan data yang diberikan. bahasa yang kamu gunakan adalah bahasa yang formal dan to the point"},
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
            ReportUser.when,
            User.name,
            User.location_id
        )
        .join(User, User.phone == ReportUser.created_by_phone)
        .filter(
            User.location_id.like(f'{location_id}%'),
            cast(ReportUser.when, Date) >= datetime.strptime(start_date, '%Y-%m-%d'),
            cast(ReportUser.when, Date) <= datetime.strptime(end_date, '%Y-%m-%d')
        )
        .all()
    )
    json_report_per_locations = [
        {
            "summary": summary,
            "category": category,
            "when": when,
            "name": name,
            "location_id": location_id
        }
        for summary, category, when, name, location_id in results_report_per_locations
    ]
    df_report_per_location = pd.DataFrame(json_report_per_locations,
                                          columns=['summary', 'category', 'when', 'name', 'location_id'])
    md_report_per_location_data = df_report_per_location.to_markdown(index=False)
    response_report_per_location = client.chat.completions.create(
        model="gpt-4.1",
        messages=[
            {"role": "system",
             "content": "Kamu adalah analis intelijen yang memberikan insight berdasarkan data yang diberikan. bahasa yang kamu gunakan adalah bahasa yang formal dan to the point"},
            {"role": "user",
             "content": f"Berikut adalah data laporan per lokasi dengan column location_id :\n{md_report_per_location_data}. Buatkan rangkuman untuk setiap lokasi dan bentuk dalam 1 paragraf text yang menjelaskan data tersebut"}
        ],
        temperature=0.5
    )

    ## =============== Summary Laporan ===================
    df_summary = df_report_per_location[['summary', 'category', 'when']]
    md_summary_data = df_summary.to_markdown(index=False)
    response_summary = client.chat.completions.create(
        model="gpt-4.1",
        messages=[
            {"role": "system",
             "content": "Kamu adalah analis intelijen yang memberikan insight berdasarkan data yang diberikan. bahasa yang kamu gunakan adalah bahasa yang formal dan to the point"},
            {"role": "user",
             "content": f"Berikut adalah data rangkuman laporan di setiap waktu:\n{md_summary_data}. buatkan rangkuman dalam bentuk 1 paragraf text"}
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
        model="gpt-4o",
        messages=[
            {"role": "system",
             "content": "Kamu adalah analis intelijen yang memberikan insight berdasarkan data yang diberikan. bahasa yang kamu gunakan adalah bahasa yang formal dan to the point"},
            {"role": "user",
             "content": f"Berikan deskripsi yang menjelaskan dari data berikut:\n{md_contributor_data}. buat dalam bentuk 1 paragraf text"}
        ],
        temperature=0.5
    )

    ## ============= Trend Kontributor ====================
    top_query = db.query(User.name, func.count(ReportUser.id).label("total"))
    top_contributors = [cb for cb, _ in top_query.join(
        User,
        (User.phone == ReportUser.created_by_phone)
    ).group_by(User.name).order_by(desc("total")).limit(5).all()]
    query = (db.query(
        cast(ReportUser.when, Date).label("date"),
        User.name,
        func.count(ReportUser.id).label("total")
    ).join(
        User,
        (User.phone == ReportUser.created_by_phone)
    )
    .filter(
        User.location_id.like(f'{location_id}%'),
        cast(ReportUser.when, Date) >= datetime.strptime(start_date, '%Y-%m-%d'),
        cast(ReportUser.when, Date) <= datetime.strptime(end_date, '%Y-%m-%d')
    ))
    query = query.filter(User.name.in_(top_contributors))
    trend_contributor_results = query.group_by(cast(ReportUser.when, Date), User.name).all()
    json_trend_contributor = [
        {"date": date_val.strftime("%Y-%m-%d"), "name": name, "total": total}
        for date_val, name, total in trend_contributor_results
    ]
    df_trend_contributor = pd.DataFrame(json_trend_contributor, columns=['date', 'name', 'total'])
    md_trend_contributor_data = df_trend_contributor.to_markdown(index=False)

    response_trend_contributor = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system",
             "content": "Kamu adalah analis intelijen yang memberikan insight berdasarkan data yang diberikan. bahasa yang kamu gunakan adalah bahasa yang formal dan to the point"},
            {"role": "user",
             "content": f"Berikan deskripsi yang menjelaskan trend dari data berikut:\n{md_trend_contributor_data}. buat dalam bentuk 1 paragraf text"}
        ],
        temperature=0.5
    )

    df_trend_contributor["date"] = pd.to_datetime(df_trend_contributor["date"])
    df_trend_contributor = df_trend_contributor.sort_values('date', ascending=True)

    fig, ax = plt.subplots(figsize=(8, 4))

    for name, group in df_trend_contributor.groupby('name'):
        plt.plot(group['date'], group['total'], marker='o', label=name)

    ax.set_title("Tren Jumlah Laporan per Nama")
    ax.set_xlabel("Tanggal")
    ax.set_ylabel("Jumlah Laporan")
    ax.legend()
    ax.grid(True)
    plt.xticks(rotation=30)
    plt.tight_layout()

    trend_contributor_img_stream = BytesIO()
    plt.savefig(trend_contributor_img_stream, format='png')
    trend_contributor_img_stream.seek(0)

    ## =========== Analisa Sentiment =============

    sentiment_query = db.query(ReportUser.sentiment, func.count(ReportUser.id).label('total'))
    sentiment_results = sentiment_query.group_by(ReportUser.sentiment).all()
    json_sentiment = [
        {"sentiment": sentiment, "total": total}
        for sentiment, total in sentiment_results
    ]
    sentiment_dict = {row.sentiment: row.total for row in sentiment_results}
    df_sentiment = pd.DataFrame(json_sentiment, columns=['sentiment', 'total'])
    md_sentiment_data = df_sentiment.to_markdown(index=False)

    response_sentiment = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system",
             "content": "Kamu adalah analis intelijen yang memberikan insight berdasarkan data yang diberikan. bahasa yang kamu gunakan adalah bahasa yang formal dan to the point"},
            {"role": "user",
             "content": f"Berikan deskripsi yang menjelaskan dari data berikut:\n{md_sentiment_data}. buat dalam bentuk 1 paragraf text"}
        ],
        temperature=0.5
    )

    plt.figure(figsize=(6, 6))
    plt.pie(
        df_sentiment["total"],
        labels=df_sentiment["sentiment"],
        autopct="%1.1f%%",
        startangle=90
    )
    plt.title("Sentiment Distribution")
    plt.axis("equal")
    plt.tight_layout()

    sentiment_img_stream = BytesIO()
    plt.savefig(sentiment_img_stream, format='png')
    sentiment_img_stream.seek(0)

    ## =========== Analisa Sentiment per Kategori =============
    sentiment_category_query = db.query(ReportUser.category, ReportUser.sentiment, func.count(ReportUser.id))
    sentiment_category_results = sentiment_category_query.group_by(ReportUser.sentiment, ReportUser.category).all()
    json_sentiment_category = [
        {"category": category, "sentiment": sentiment, "total": total}
        for category, sentiment, total in sentiment_category_results
    ]
    df_sentiment_category = pd.DataFrame(json_sentiment_category, columns=['category', 'sentiment', 'total'])
    md_sentiment_category_data = df_sentiment_category.to_markdown(index=False)

    response_sentiment_category = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system",
             "content": "Kamu adalah analis intelijen yang memberikan insight berdasarkan data yang diberikan. bahasa yang kamu gunakan adalah bahasa yang formal dan to the point"},
            {"role": "user",
             "content": f"Berikan deskripsi yang menjelaskan dari data berikut:\n{md_sentiment_category_data}. buat dalam bentuk 1 paragraf text"}
        ],
        temperature=0.5
    )

    pivot_df = df_sentiment_category.pivot(
        index="category", columns="sentiment", values="total"
    ).fillna(0)

    pivot_df.plot(
        kind="bar",
        figsize=(8, 6)
    )

    plt.title("Sentiment Count by Category")
    plt.xlabel("Category")
    plt.ylabel("Count")
    plt.xticks(rotation=45, ha="right")
    plt.legend(title="Sentiment")
    plt.tight_layout()

    sentiment_category_img_stream = BytesIO()
    plt.savefig(sentiment_category_img_stream, format='png')
    sentiment_category_img_stream.seek(0)

    ## =========== Render =================
    doc = DocxTemplate("./template/template-report.docx")
    context = {
        'title': title if title else 'Laporan',
        'lokasi': location_name_str,
        'generated_date_start': start_date,
        'generated_date_end': end_date,
        'total_report': total,
        'report_this_month': total_report_with_period,
        'chart_tren_laporan': InlineImage(doc, trend_img_stream, width=Mm(160)),
        'deskripsi_tren_laporan': response_trend.choices[0].message.content,
        'total_laporan_permasalahan': category_dict['Laporan Permasalahan'],
        'total_laporan_informasi': category_dict['Laporan Informasi'],
        'total_laporan_progress': category_dict['Laporan Progress'],
        'deskripsi_kategori_laporan': response_category.choices[0].message.content,
        'rangkuman_laporan_perlokasi': response_report_per_location.choices[0].message.content,
        'rangkuman_laporan': response_summary.choices[0].message.content,
        'chart_top_kontributor': InlineImage(doc, contributor_img_stream, width=Mm(160)),
        'deskripsi_top_kontributor': response_contributor.choices[0].message.content,
        'chart_tren_kontributor': InlineImage(doc, trend_contributor_img_stream, width=Mm(160)),
        'deskripsi_tren_kontributor': response_trend_contributor.choices[0].message.content,
        'total_sentiment_positive': sentiment_dict['Positif'],
        'total_sentiment_neutral': sentiment_dict['Netral'],
        'total_sentiment_negative': sentiment_dict['Negatif'],
        'chart_sentiment': InlineImage(doc, sentiment_img_stream, width=Mm(160)),
        'deskripsi_sentiment': response_sentiment.choices[0].message.content,
        'chart_sentiment_category': InlineImage(doc, sentiment_category_img_stream, width=Mm(160)),
        'deskripsi_sentiment_category': response_sentiment_category.choices[0].message.content
    }
    doc.render(context)
    filename = f'report-{location_id}-{start_date}-{end_date}.docx'
    doc.save(f"./report/{filename}")
    stmt = (
        insert(ReportMetadata)
        .values(
            title=filename,
            location_id=location_id,
            url=f"https://bgn-be.anakanjeng.site/{filename}",
        )
        .returning(ReportMetadata.id)
    )

    result = db.execute(stmt)
    inserted_id = result.scalar()
    db.commit()
    return {
        'id': inserted_id,
        'title': filename
    }


def get_download_report(db: Session, id: int, url: str):
    result = (
        db.query(ReportMetadata)
        .filter(ReportMetadata.id == id)
        .first()
    )

    if not result:
        return {}

    json_result = {c.name: getattr(result, c.name) for c in result.__table__.columns}
    url_data = json_result.get('url')

    if url_data != url:
        return {}

    filename = json_result.get('title')
    return FileResponse(
        path=f'./report/{filename}',
        filename=filename,
        media_type='application/octet-stream'
    )


def get_all_document(db: Session, page: int = 1, limit: int = 10):
    offset = (page - 1) * limit

    # Query data
    results = (
        db.query(ReportMetadata)
        .order_by(desc(ReportMetadata.generated_at))
        .limit(limit)
        .offset(offset)
        .all()
    )

    # Hitung total
    total = db.query(func.count(ReportMetadata.id)).scalar()

    return {
        "page": page,
        "limit": limit,
        "total": total,
        "total_page": (total + limit - 1) // limit,  # pembulatan ke atas
        "data": [row.__dict__ for row in results]
    }

def get_report_statistics(db: Session):
    total = db.query(ReportMetadata).count()
    now = datetime.now()
    month_total = db.query(ReportMetadata).filter(
        ReportMetadata.generated_at >= datetime(now.year, now.month, 1),
        ReportMetadata.generated_at < datetime(now.year, now.month + 1 if now.month < 12 else 1, 1) if now.month < 12 else datetime(now.year + 1, 1, 1)
    ).count()
    return {"total": total, "month_total": month_total}