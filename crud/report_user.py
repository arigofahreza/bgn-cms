from sqlalchemy import and_, cast, Date, literal_column
from sqlalchemy.orm import Session

from utils.generator import filter_query_location
from models.report_user import ReportUser
from models.users import User
from datetime import datetime


def get_report_user_statistics(db: Session,
                               kd_propinsi: str | None = None,
                               kd_kabupaten: str | None = None,
                               kd_kecamatan: str | None = None,
                               kd_kelurahan: str | None = None
                               ):
    query_total = (db.query(ReportUser)
                   .join(User, User.phone == ReportUser.created_by_phone))
    now = datetime.now()
    query_month_total = (db.query(ReportUser)
                         .filter(
        ReportUser.when >= datetime(now.year, now.month, 1),
        ReportUser.when < datetime(now.year, now.month + 1 if now.month < 12 else 1, 1) if now.month < 12 else datetime(
            now.year + 1, 1, 1)
    ).join(User, User.phone == ReportUser.created_by_phone))

    if kd_propinsi and not kd_kabupaten and not kd_kecamatan and not kd_kelurahan:
        kd_propinsi_col = literal_column("(string_to_array(users.location_id, '.'))[1]")
        query_total = (query_total.filter(kd_propinsi_col == kd_propinsi))
        query_month_total = (query_month_total.filter(kd_propinsi_col == kd_propinsi))
    elif kd_propinsi and kd_kabupaten and not kd_kecamatan and not kd_kelurahan:
        kd_propinsi_col = literal_column("(string_to_array(users.location_id, '.'))[1]")
        kd_kabupaten_col = literal_column("(string_to_array(users.location_id, '.'))[2]")
        query_total = (query_total.filter(kd_propinsi_col == kd_propinsi,
                                          kd_kabupaten_col == kd_kabupaten))
        query_month_total = (query_month_total.filter(kd_propinsi_col == kd_propinsi,
                                                      kd_kabupaten_col == kd_kabupaten))
    elif kd_propinsi and kd_kabupaten and kd_kecamatan and not kd_kelurahan:
        kd_propinsi_col = literal_column("(string_to_array(users.location_id, '.'))[1]")
        kd_kabupaten_col = literal_column("(string_to_array(users.location_id, '.'))[2]")
        kd_kecamatan_col = literal_column("(string_to_array(users.location_id, '.'))[3]")
        query_total = (query_total.filter(kd_propinsi_col == kd_propinsi,
                                          kd_kabupaten_col == kd_kabupaten,
                                          kd_kecamatan_col == kd_kecamatan))
        query_month_total = (query_month_total.filter(kd_propinsi_col == kd_propinsi,
                                                      kd_kabupaten_col == kd_kabupaten,
                                                      kd_kecamatan_col == kd_kecamatan))
    elif kd_propinsi and kd_kabupaten and kd_kecamatan and kd_kelurahan:
        kd_propinsi_col = literal_column("(string_to_array(users.location_id, '.'))[1]")
        kd_kabupaten_col = literal_column("(string_to_array(users.location_id, '.'))[2]")
        kd_kecamatan_col = literal_column("(string_to_array(users.location_id, '.'))[3]")
        kd_kelurahan_col = literal_column("(string_to_array(users.location_id, '.'))[4]")
        query_total = (query_total.filter(kd_propinsi_col == kd_propinsi,
                                          kd_kabupaten_col == kd_kabupaten,
                                          kd_kecamatan_col == kd_kecamatan,
                                          kd_kelurahan_col == kd_kelurahan))
        query_month_total = (query_month_total.filter(kd_propinsi_col == kd_propinsi,
                                                      kd_kabupaten_col == kd_kabupaten,
                                                      kd_kecamatan_col == kd_kecamatan,
                                                      kd_kelurahan_col == kd_kelurahan))
    total = query_total.count()
    month_total = query_month_total.count()

    return {"total": total, "month_total": month_total}


def get_total_by_category(db: Session,
                          start_date: str = None,
                          end_date: str = None,
                          kd_propinsi: str | None = None,
                          kd_kabupaten: str | None = None,
                          kd_kecamatan: str | None = None,
                          kd_kelurahan: str | None = None
                          ):
    from sqlalchemy import func
    query = (db.query(ReportUser.category, func.count(ReportUser.id))
             .join(User, User.phone == ReportUser.created_by_phone))
    if start_date and end_date:
        query = query.filter(cast(ReportUser.when, Date) >= datetime.strptime(start_date, '%Y-%m-%d'),
                             cast(ReportUser.when, Date) <= datetime.strptime(end_date, '%Y-%m-%d'))
    if kd_propinsi and not kd_kabupaten and not kd_kecamatan and not kd_kelurahan:
        kd_propinsi_col = literal_column("(string_to_array(users.location_id, '.'))[1]")
        query = (query.filter(kd_propinsi_col == kd_propinsi)
                 .group_by(kd_propinsi_col, ReportUser.category))
    elif kd_propinsi and kd_kabupaten and not kd_kecamatan and not kd_kelurahan:
        kd_propinsi_col = literal_column("(string_to_array(users.location_id, '.'))[1]")
        kd_kabupaten_col = literal_column("(string_to_array(users.location_id, '.'))[2]")
        query = (query.filter(kd_propinsi_col == kd_propinsi,
                              kd_kabupaten_col == kd_kabupaten)
                 .group_by(kd_propinsi_col, kd_kabupaten_col, ReportUser.category))
    elif kd_propinsi and kd_kabupaten and kd_kecamatan and not kd_kelurahan:
        kd_propinsi_col = literal_column("(string_to_array(users.location_id, '.'))[1]")
        kd_kabupaten_col = literal_column("(string_to_array(users.location_id, '.'))[2]")
        kd_kecamatan_col = literal_column("(string_to_array(users.location_id, '.'))[3]")
        query = (query.filter(kd_propinsi_col == kd_propinsi,
                              kd_kabupaten_col == kd_kabupaten,
                              kd_kecamatan_col == kd_kecamatan)
                 .group_by(kd_propinsi_col, kd_kabupaten_col, kd_kecamatan_col, ReportUser.category))
    elif kd_propinsi and kd_kabupaten and kd_kecamatan and kd_kelurahan:
        kd_propinsi_col = literal_column("(string_to_array(users.location_id, '.'))[1]")
        kd_kabupaten_col = literal_column("(string_to_array(users.location_id, '.'))[2]")
        kd_kecamatan_col = literal_column("(string_to_array(users.location_id, '.'))[3]")
        kd_kelurahan_col = literal_column("(string_to_array(users.location_id, '.'))[4]")
        query = (query.filter(kd_propinsi_col == kd_propinsi,
                              kd_kabupaten_col == kd_kabupaten,
                              kd_kecamatan_col == kd_kecamatan,
                              kd_kelurahan_col == kd_kelurahan)
                 .group_by(kd_propinsi_col, kd_kabupaten_col, kd_kecamatan_col, kd_kelurahan_col, ReportUser.category))
    else:
        query = (query.group_by(ReportUser.category))
    results = query.all()
    return [{"category": cat, "total": total} for cat, total in results]


def get_total_by_created_by(db: Session,
                            category: str = None,
                            start_date: str = None,
                            end_date: str = None,
                            kd_propinsi: str | None = None,
                            kd_kabupaten: str | None = None,
                            kd_kecamatan: str | None = None,
                            kd_kelurahan: str | None = None):
    from sqlalchemy import func, desc
    query = (db.query(User.name, ReportUser.created_by_phone.label('cb'), func.count(ReportUser.id).label("total"))
             .join(User, User.phone == ReportUser.created_by_phone))
    if category:
        query = query.filter(ReportUser.category == category)
    if start_date and end_date:
        query = query.filter(cast(ReportUser.when, Date) >= datetime.strptime(start_date, '%Y-%m-%d'),
                             cast(ReportUser.when, Date) <= datetime.strptime(end_date, '%Y-%m-%d'))
    if kd_propinsi and not kd_kabupaten and not kd_kecamatan and not kd_kelurahan:
        kd_propinsi_col = literal_column("(string_to_array(users.location_id, '.'))[1]")
        query = (query.filter(kd_propinsi_col == kd_propinsi)
                 .group_by(kd_propinsi_col, User.name, ReportUser.created_by_phone))
    elif kd_propinsi and kd_kabupaten and not kd_kecamatan and not kd_kelurahan:
        kd_propinsi_col = literal_column("(string_to_array(users.location_id, '.'))[1]")
        kd_kabupaten_col = literal_column("(string_to_array(users.location_id, '.'))[2]")
        query = (query.filter(kd_propinsi_col == kd_propinsi,
                              kd_kabupaten_col == kd_kabupaten)
                 .group_by(kd_propinsi_col, kd_kabupaten_col, User.name, ReportUser.created_by_phone))
    elif kd_propinsi and kd_kabupaten and kd_kecamatan and not kd_kelurahan:
        kd_propinsi_col = literal_column("(string_to_array(users.location_id, '.'))[1]")
        kd_kabupaten_col = literal_column("(string_to_array(users.location_id, '.'))[2]")
        kd_kecamatan_col = literal_column("(string_to_array(users.location_id, '.'))[3]")
        query = (query.filter(kd_propinsi_col == kd_propinsi,
                              kd_kabupaten_col == kd_kabupaten,
                              kd_kecamatan_col == kd_kecamatan)
                 .group_by(kd_propinsi_col, kd_kabupaten_col, kd_kecamatan_col, User.name, ReportUser.created_by_phone))
    elif kd_propinsi and kd_kabupaten and kd_kecamatan and kd_kelurahan:
        kd_propinsi_col = literal_column("(string_to_array(users.location_id, '.'))[1]")
        kd_kabupaten_col = literal_column("(string_to_array(users.location_id, '.'))[2]")
        kd_kecamatan_col = literal_column("(string_to_array(users.location_id, '.'))[3]")
        kd_kelurahan_col = literal_column("(string_to_array(users.location_id, '.'))[4]")
        query = (query.filter(kd_propinsi_col == kd_propinsi,
                              kd_kabupaten_col == kd_kabupaten,
                              kd_kecamatan_col == kd_kecamatan,
                              kd_kelurahan_col == kd_kelurahan)
                 .group_by(kd_propinsi_col, kd_kabupaten_col, kd_kecamatan_col, kd_kelurahan_col, User.name,
                           ReportUser.created_by_phone))
    else:
        query = query.group_by(User.name, ReportUser.created_by_phone)
    query = query.order_by(desc("total")).limit(5)
    results = (
        query
        .all()
    )
    return [{"name": name, "created_by": cb, "total": total} for name, cb, total in results]


def get_trend_contributor(db: Session,
                          category: str = None,
                          start_date: str = None,
                          end_date: str = None):
    from sqlalchemy import func, cast, Date, desc
    from datetime import date, timedelta

    today = date.today()
    days = [(today - timedelta(days=i)) for i in range(7, -1, -1)]
    if start_date and end_date:
        today = datetime.strptime(end_date, '%Y-%m-%d')
        start = datetime.strptime(start_date, "%Y-%m-%d")
        days = [(start + timedelta(days=i)).date() for i in range((today - start).days + 1)]
    # Get top 5 contributors
    top_query = db.query(User.name, ReportUser.created_by_phone, func.count(ReportUser.id).label("total"))
    if category:
        top_query = top_query.filter(ReportUser.category == category)
    if start_date and end_date:
        top_query = top_query.filter(cast(ReportUser.when, Date) >= datetime.strptime(start_date, '%Y-%m-%d'),
                                     cast(ReportUser.when, Date) <= datetime.strptime(end_date, '%Y-%m-%d'))
    else:
        top_query = top_query.filter(cast(ReportUser.when, Date) >= today - timedelta(days=7),
                                     cast(ReportUser.when, Date) <= today)
    top_query = (top_query.join(
        User,
        (User.phone == ReportUser.created_by_phone))).order_by(desc("total"))

    top_query = top_query.group_by(User.name,
                                   ReportUser.created_by_phone)
    top_contributors = [cb for cb, _, _ in top_query.limit(5).all()]
    # Get daily totals for each contributor
    trend_data = {d: {cb: 0 for cb in top_contributors} for d in days}
    query = (db.query(
        cast(ReportUser.when, Date).label("date"),
        User.name,
        func.count(ReportUser.id).label("total")
    ).join(
        User,
        (User.phone == ReportUser.created_by_phone)
    ).order_by(ReportUser.when))
    if category:
        query = query.filter(ReportUser.category == category)
    if start_date and end_date:
        query = query.filter(cast(ReportUser.when, Date) >= datetime.strptime(start_date, '%Y-%m-%d'),
                             cast(ReportUser.when, Date) <= datetime.strptime(end_date, '%Y-%m-%d'))
    else:
        query = query.filter(User.name.in_(top_contributors),
                             cast(ReportUser.when, Date) >= today - timedelta(days=7),
                             cast(ReportUser.when, Date) <= today)
    query = query.group_by(cast(ReportUser.when, Date), User.name)
    results = query.all()
    for d, cb, total in results:
        if cb in top_contributors and d in trend_data:
            trend_data[d][cb] = total
    response = []
    for d in days:
        data = [{"name": cb, "total": trend_data[d][cb]} for cb in top_contributors]
        response.append({"date": d.isoformat(), "data": data})
    return response


def get_total_by_date_last_14_days(db: Session,
                                   category: str = None,
                                   start_date: str = None,
                                   end_date: str = None,
                                   kd_propinsi: str | None = None,
                                   kd_kabupaten: str | None = None,
                                   kd_kecamatan: str | None = None,
                                   kd_kelurahan: str | None = None
                                   ):
    from sqlalchemy import func, cast, Date
    from datetime import date, timedelta
    today = date.today()
    days = [(today - timedelta(days=i)) for i in range(14, -1, -1)]
    if start_date and end_date:
        today = datetime.strptime(end_date, '%Y-%m-%d')
        start = datetime.strptime(start_date, "%Y-%m-%d")
        days = [(start + timedelta(days=i)).date() for i in range((today - start).days + 1)]
    query = db.query(cast(ReportUser.when, Date), func.count(ReportUser.id))
    query = query.join(
        User,
        (User.phone == ReportUser.created_by_phone))
    if start_date and end_date:
        query = query.filter(cast(ReportUser.when, Date) >= datetime.strptime(start_date, '%Y-%m-%d'),
                             cast(ReportUser.when, Date) <= datetime.strptime(end_date, '%Y-%m-%d'))
    else:
        query = query.filter(
            cast(ReportUser.when, Date) >= today - timedelta(days=14),
            cast(ReportUser.when, Date) <= today
        )
    if category:
        query = query.filter(ReportUser.category == category)
    if kd_propinsi and not kd_kabupaten and not kd_kecamatan and not kd_kelurahan:
        kd_propinsi_col = literal_column("(string_to_array(users.location_id, '.'))[1]")
        query = (query.filter(kd_propinsi_col == kd_propinsi)
                 .group_by(kd_propinsi_col, cast(ReportUser.when, Date)))
    elif kd_propinsi and kd_kabupaten and not kd_kecamatan and not kd_kelurahan:
        kd_propinsi_col = literal_column("(string_to_array(users.location_id, '.'))[1]")
        kd_kabupaten_col = literal_column("(string_to_array(users.location_id, '.'))[2]")
        query = (query.filter(kd_propinsi_col == kd_propinsi,
                              kd_kabupaten_col == kd_kabupaten)
                 .group_by(kd_propinsi_col, kd_kabupaten_col, cast(ReportUser.when, Date)))
    elif kd_propinsi and kd_kabupaten and kd_kecamatan and not kd_kelurahan:
        kd_propinsi_col = literal_column("(string_to_array(users.location_id, '.'))[1]")
        kd_kabupaten_col = literal_column("(string_to_array(users.location_id, '.'))[2]")
        kd_kecamatan_col = literal_column("(string_to_array(users.location_id, '.'))[3]")
        query = (query.filter(kd_propinsi_col == kd_propinsi,
                              kd_kabupaten_col == kd_kabupaten,
                              kd_kecamatan_col == kd_kecamatan)
                 .group_by(kd_propinsi_col, kd_kabupaten_col, kd_kecamatan_col, cast(ReportUser.when, Date)))
    elif kd_propinsi and kd_kabupaten and kd_kecamatan and kd_kelurahan:
        kd_propinsi_col = literal_column("(string_to_array(users.location_id, '.'))[1]")
        kd_kabupaten_col = literal_column("(string_to_array(users.location_id, '.'))[2]")
        kd_kecamatan_col = literal_column("(string_to_array(users.location_id, '.'))[3]")
        kd_kelurahan_col = literal_column("(string_to_array(users.location_id, '.'))[4]")
        query = (query.filter(kd_propinsi_col == kd_propinsi,
                              kd_kabupaten_col == kd_kabupaten,
                              kd_kecamatan_col == kd_kecamatan,
                              kd_kelurahan_col == kd_kelurahan)
                 .group_by(kd_propinsi_col, kd_kabupaten_col, kd_kecamatan_col, kd_kelurahan_col,
                           cast(ReportUser.when, Date)))
    else:
        query = query.group_by(cast(ReportUser.when, Date))
    results = query.all()
    result_dict = {d: 0 for d in days}
    for d, total in results:
        result_dict[d] = total
    return [{"date": d.isoformat(), "total": result_dict[d]} for d in days]


def get_heatmap(db: Session, category: str = None,
                kd_propinsi: str | None = None,
                kd_kabupaten: str | None = None,
                kd_kecamatan: str | None = None,
                kd_kelurahan: str | None = None
                ):
    from collections import defaultdict
    from datetime import datetime, timedelta
    from models.report_user import ReportUser
    today = datetime.today()
    start_month = today.replace(day=1)
    six_months_ago = (start_month - timedelta(days=1)).replace(day=1) - timedelta(days=30 * 5)
    query = db.query(ReportUser.when)
    query = query.join(
        User,
        (User.phone == ReportUser.created_by_phone))
    if category:
        query = query.filter(ReportUser.category == category)
    query = query.filter(ReportUser.when >= six_months_ago)
    if kd_propinsi and not kd_kabupaten and not kd_kecamatan and not kd_kelurahan:
        kd_propinsi_col = literal_column("(string_to_array(users.location_id, '.'))[1]")
        query = (query.filter(kd_propinsi_col == kd_propinsi))
    elif kd_propinsi and kd_kabupaten and not kd_kecamatan and not kd_kelurahan:
        kd_propinsi_col = literal_column("(string_to_array(users.location_id, '.'))[1]")
        kd_kabupaten_col = literal_column("(string_to_array(users.location_id, '.'))[2]")
        query = (query.filter(kd_propinsi_col == kd_propinsi,
                              kd_kabupaten_col == kd_kabupaten))
    elif kd_propinsi and kd_kabupaten and kd_kecamatan and not kd_kelurahan:
        kd_propinsi_col = literal_column("(string_to_array(users.location_id, '.'))[1]")
        kd_kabupaten_col = literal_column("(string_to_array(users.location_id, '.'))[2]")
        kd_kecamatan_col = literal_column("(string_to_array(users.location_id, '.'))[3]")
        query = (query.filter(kd_propinsi_col == kd_propinsi,
                              kd_kabupaten_col == kd_kabupaten,
                              kd_kecamatan_col == kd_kecamatan))
    elif kd_propinsi and kd_kabupaten and kd_kecamatan and kd_kelurahan:
        kd_propinsi_col = literal_column("(string_to_array(users.location_id, '.'))[1]")
        kd_kabupaten_col = literal_column("(string_to_array(users.location_id, '.'))[2]")
        kd_kecamatan_col = literal_column("(string_to_array(users.location_id, '.'))[3]")
        kd_kelurahan_col = literal_column("(string_to_array(users.location_id, '.'))[4]")
        query = (query.filter(kd_propinsi_col == kd_propinsi,
                              kd_kabupaten_col == kd_kabupaten,
                              kd_kecamatan_col == kd_kecamatan,
                              kd_kelurahan_col == kd_kelurahan))
    data = query.all()
    heatmap = defaultdict(int)
    for row in data:
        date_str = row.when.strftime("%Y-%m-%d")
        heatmap[date_str] += 1

    result = []
    current = six_months_ago
    while current <= today:
        date_str = current.strftime("%Y-%m-%d")
        result.append({
            "date": date_str,
            "total": heatmap.get(date_str, 0)
        })
        current += timedelta(days=1)

    return result


def get_wordcloud(db: Session,
                  category: str = None,
                  kd_propinsi: str | None = None,
                  kd_kabupaten: str | None = None,
                  kd_kecamatan: str | None = None,
                  kd_kelurahan: str | None = None
                  ):
    from collections import Counter
    from models.report_user import ReportUser
    query = db.query(ReportUser.summary)
    query = query.join(
        User,
        (User.phone == ReportUser.created_by_phone))
    if category:
        query = query.filter(ReportUser.category == category)
    if kd_propinsi and not kd_kabupaten and not kd_kecamatan and not kd_kelurahan:
        kd_propinsi_col = literal_column("(string_to_array(users.location_id, '.'))[1]")
        query = (query.filter(kd_propinsi_col == kd_propinsi))
    elif kd_propinsi and kd_kabupaten and not kd_kecamatan and not kd_kelurahan:
        kd_propinsi_col = literal_column("(string_to_array(users.location_id, '.'))[1]")
        kd_kabupaten_col = literal_column("(string_to_array(users.location_id, '.'))[2]")
        query = (query.filter(kd_propinsi_col == kd_propinsi,
                              kd_kabupaten_col == kd_kabupaten))
    elif kd_propinsi and kd_kabupaten and kd_kecamatan and not kd_kelurahan:
        kd_propinsi_col = literal_column("(string_to_array(users.location_id, '.'))[1]")
        kd_kabupaten_col = literal_column("(string_to_array(users.location_id, '.'))[2]")
        kd_kecamatan_col = literal_column("(string_to_array(users.location_id, '.'))[3]")
        query = (query.filter(kd_propinsi_col == kd_propinsi,
                              kd_kabupaten_col == kd_kabupaten,
                              kd_kecamatan_col == kd_kecamatan))
    elif kd_propinsi and kd_kabupaten and kd_kecamatan and kd_kelurahan:
        kd_propinsi_col = literal_column("(string_to_array(users.location_id, '.'))[1]")
        kd_kabupaten_col = literal_column("(string_to_array(users.location_id, '.'))[2]")
        kd_kecamatan_col = literal_column("(string_to_array(users.location_id, '.'))[3]")
        kd_kelurahan_col = literal_column("(string_to_array(users.location_id, '.'))[4]")
        query = (query.filter(kd_propinsi_col == kd_propinsi,
                              kd_kabupaten_col == kd_kabupaten,
                              kd_kecamatan_col == kd_kecamatan,
                              kd_kelurahan_col == kd_kelurahan))
    data = query.all()
    words = []
    for (summary,) in data:
        if summary:
            words.extend(summary.lower().split())
    counter = Counter(words)
    # Return as list of dicts: [{"word": w, "count": c}, ...]
    return [{"word": w, "count": c} for w, c in counter.most_common()]


def get_total_by_sentiment(db: Session,
                           category: str = None,
                           start_date: str = None,
                           end_date: str = None,
                           kd_propinsi: str | None = None,
                           kd_kabupaten: str | None = None,
                           kd_kecamatan: str | None = None,
                           kd_kelurahan: str | None = None
                           ):
    from sqlalchemy import func
    query = db.query(ReportUser.sentiment, func.count(ReportUser.id))
    if start_date and end_date:
        query = query.filter(cast(ReportUser.when, Date) >= datetime.strptime(start_date, '%Y-%m-%d'),
                             cast(ReportUser.when, Date) <= datetime.strptime(end_date, '%Y-%m-%d'))
    query = query.join(
        User,
        (User.phone == ReportUser.created_by_phone))
    if category:
        query = query.filter(ReportUser.category == category)
    if kd_propinsi and not kd_kabupaten and not kd_kecamatan and not kd_kelurahan:
        kd_propinsi_col = literal_column("(string_to_array(users.location_id, '.'))[1]")
        query = (query.filter(kd_propinsi_col == kd_propinsi)
                 .group_by(kd_propinsi_col, ReportUser.sentiment))
    elif kd_propinsi and kd_kabupaten and not kd_kecamatan and not kd_kelurahan:
        kd_propinsi_col = literal_column("(string_to_array(users.location_id, '.'))[1]")
        kd_kabupaten_col = literal_column("(string_to_array(users.location_id, '.'))[2]")
        query = (query.filter(kd_propinsi_col == kd_propinsi,
                              kd_kabupaten_col == kd_kabupaten)
                 .group_by(kd_propinsi_col, kd_kabupaten_col, ReportUser.sentiment))
    elif kd_propinsi and kd_kabupaten and kd_kecamatan and not kd_kelurahan:
        kd_propinsi_col = literal_column("(string_to_array(users.location_id, '.'))[1]")
        kd_kabupaten_col = literal_column("(string_to_array(users.location_id, '.'))[2]")
        kd_kecamatan_col = literal_column("(string_to_array(users.location_id, '.'))[3]")
        query = (query.filter(kd_propinsi_col == kd_propinsi,
                              kd_kabupaten_col == kd_kabupaten,
                              kd_kecamatan_col == kd_kecamatan)
                 .group_by(kd_propinsi_col, kd_kabupaten_col, kd_kecamatan_col, ReportUser.sentiment))
    elif kd_propinsi and kd_kabupaten and kd_kecamatan and kd_kelurahan:
        kd_propinsi_col = literal_column("(string_to_array(users.location_id, '.'))[1]")
        kd_kabupaten_col = literal_column("(string_to_array(users.location_id, '.'))[2]")
        kd_kecamatan_col = literal_column("(string_to_array(users.location_id, '.'))[3]")
        kd_kelurahan_col = literal_column("(string_to_array(users.location_id, '.'))[4]")
        query = (query.filter(kd_propinsi_col == kd_propinsi,
                              kd_kabupaten_col == kd_kabupaten,
                              kd_kecamatan_col == kd_kecamatan,
                              kd_kelurahan_col == kd_kelurahan)
                 .group_by(kd_propinsi_col, kd_kabupaten_col, kd_kecamatan_col, kd_kelurahan_col, ReportUser.sentiment))
    else:
        query = query.group_by(ReportUser.sentiment)
    results = query.all()
    return [{"sentiment": s, "total": total} for s, total in results]


def get_desc_data(db: Session, category: str = None,
                  start_date: str = None,
                  end_date: str = None,
                  kd_propinsi: str | None = None,
                  kd_kabupaten: str | None = None,
                  kd_kecamatan: str | None = None,
                  kd_kelurahan: str | None = None
                  ):
    query = db.query(
        ReportUser.what,
        ReportUser.who,
        ReportUser.where,
        ReportUser.created_at
    )
    if category:
        query = query.filter(ReportUser.category == category)
    if start_date and end_date:
        query = query.filter(cast(ReportUser.when, Date) >= datetime.strptime(start_date, '%Y-%m-%d'),
                             cast(ReportUser.when, Date) <= datetime.strptime(end_date, '%Y-%m-%d'))
    if kd_propinsi and not kd_kabupaten and not kd_kecamatan and not kd_kelurahan:
        kd_propinsi_col = literal_column("(string_to_array(users.location_id, '.'))[1]")
        query = (query.filter(kd_propinsi_col == kd_propinsi))
    elif kd_propinsi and kd_kabupaten and not kd_kecamatan and not kd_kelurahan:
        kd_propinsi_col = literal_column("(string_to_array(users.location_id, '.'))[1]")
        kd_kabupaten_col = literal_column("(string_to_array(users.location_id, '.'))[2]")
        query = (query.filter(kd_propinsi_col == kd_propinsi,
                              kd_kabupaten_col == kd_kabupaten))
    elif kd_propinsi and kd_kabupaten and kd_kecamatan and not kd_kelurahan:
        kd_propinsi_col = literal_column("(string_to_array(users.location_id, '.'))[1]")
        kd_kabupaten_col = literal_column("(string_to_array(users.location_id, '.'))[2]")
        kd_kecamatan_col = literal_column("(string_to_array(users.location_id, '.'))[3]")
        query = (query.filter(kd_propinsi_col == kd_propinsi,
                              kd_kabupaten_col == kd_kabupaten,
                              kd_kecamatan_col == kd_kecamatan))
    elif kd_propinsi and kd_kabupaten and kd_kecamatan and kd_kelurahan:
        kd_propinsi_col = literal_column("(string_to_array(users.location_id, '.'))[1]")
        kd_kabupaten_col = literal_column("(string_to_array(users.location_id, '.'))[2]")
        kd_kecamatan_col = literal_column("(string_to_array(users.location_id, '.'))[3]")
        kd_kelurahan_col = literal_column("(string_to_array(users.location_id, '.'))[4]")
        query = (query.filter(kd_propinsi_col == kd_propinsi,
                              kd_kabupaten_col == kd_kabupaten,
                              kd_kecamatan_col == kd_kecamatan,
                              kd_kelurahan_col == kd_kelurahan))
    query = query.order_by(ReportUser.created_at.desc()).limit(15)
    results = query.all()
    return [
        {
            "what": r[0],
            "who": r[1],
            "where": r[2],
            "created_at": r[3].isoformat() if r[3] else None
        }
        for r in results
    ]


def get_sentiment_category(db: Session, category: str = None,
                           start_date: str = None,
                           end_date: str = None,
                           kd_propinsi: str | None = None,
                           kd_kabupaten: str | None = None,
                           kd_kecamatan: str | None = None,
                           kd_kelurahan: str | None = None
                           ):
    from sqlalchemy import func
    query = db.query(ReportUser.category, ReportUser.sentiment, func.count(ReportUser.id))
    query = query.join(
        User,
        (User.phone == ReportUser.created_by_phone))
    if start_date and end_date:
        query = query.filter(cast(ReportUser.when, Date) >= datetime.strptime(start_date, '%Y-%m-%d'),
                             cast(ReportUser.when, Date) <= datetime.strptime(end_date, '%Y-%m-%d'))
    if category:
        query = query.filter(ReportUser.category == category)
    if kd_propinsi and not kd_kabupaten and not kd_kecamatan and not kd_kelurahan:
        kd_propinsi_col = literal_column("(string_to_array(users.location_id, '.'))[1]")
        query = (query.filter(kd_propinsi_col == kd_propinsi)
                 .group_by(kd_propinsi_col, ReportUser.sentiment, ReportUser.category))
    elif kd_propinsi and kd_kabupaten and not kd_kecamatan and not kd_kelurahan:
        kd_propinsi_col = literal_column("(string_to_array(users.location_id, '.'))[1]")
        kd_kabupaten_col = literal_column("(string_to_array(users.location_id, '.'))[2]")
        query = (query.filter(kd_propinsi_col == kd_propinsi,
                              kd_kabupaten_col == kd_kabupaten)
                 .group_by(kd_propinsi_col, kd_kabupaten_col, ReportUser.sentiment, ReportUser.category))
    elif kd_propinsi and kd_kabupaten and kd_kecamatan and not kd_kelurahan:
        kd_propinsi_col = literal_column("(string_to_array(users.location_id, '.'))[1]")
        kd_kabupaten_col = literal_column("(string_to_array(users.location_id, '.'))[2]")
        kd_kecamatan_col = literal_column("(string_to_array(users.location_id, '.'))[3]")
        query = (query.filter(kd_propinsi_col == kd_propinsi,
                              kd_kabupaten_col == kd_kabupaten,
                              kd_kecamatan_col == kd_kecamatan)
                 .group_by(kd_propinsi_col, kd_kabupaten_col, kd_kecamatan_col, ReportUser.sentiment, ReportUser.category))
    elif kd_propinsi and kd_kabupaten and kd_kecamatan and kd_kelurahan:
        kd_propinsi_col = literal_column("(string_to_array(users.location_id, '.'))[1]")
        kd_kabupaten_col = literal_column("(string_to_array(users.location_id, '.'))[2]")
        kd_kecamatan_col = literal_column("(string_to_array(users.location_id, '.'))[3]")
        kd_kelurahan_col = literal_column("(string_to_array(users.location_id, '.'))[4]")
        query = (query.filter(kd_propinsi_col == kd_propinsi,
                              kd_kabupaten_col == kd_kabupaten,
                              kd_kecamatan_col == kd_kecamatan,
                              kd_kelurahan_col == kd_kelurahan)
                 .group_by(kd_propinsi_col, kd_kabupaten_col, kd_kecamatan_col, kd_kelurahan_col, ReportUser.sentiment, ReportUser.category))
    else:
        query = query.group_by(ReportUser.sentiment, ReportUser.category)
    results = query.all()
    return [{"category": category, "sentiment": s, "total": total} for category, s, total in results]
