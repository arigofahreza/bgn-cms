
from sqlalchemy.orm import Session
from models.report_user import ReportUser
from datetime import datetime

def get_report_user_statistics(db: Session):
    total = db.query(ReportUser).count()
    now = datetime.now()
    month_total = db.query(ReportUser).filter(
        ReportUser.when >= datetime(now.year, now.month, 1),
        ReportUser.when < datetime(now.year, now.month + 1 if now.month < 12 else 1, 1) if now.month < 12 else datetime(now.year + 1, 1, 1)
    ).count()
    return {"total": total, "month_total": month_total}

def get_total_by_category(db: Session):
    from sqlalchemy import func
    results = db.query(ReportUser.category, func.count(ReportUser.id)).group_by(ReportUser.category).all()
    return [{"category": cat, "total": total} for cat, total in results]

def get_total_by_created_by(db: Session, category: str = None):
    from sqlalchemy import func, desc
    query = db.query(ReportUser.when.label('cb'), func.count(ReportUser.id).label("total"))
    if category:
        query = query.filter(ReportUser.category == category)
    results = (
        query.group_by(ReportUser.when)
        .order_by(desc("total"))
        .limit(5)
        .all()
    )
    return [{"created_by": cb, "total": total} for cb, total in results]

def get_trend_contributor(db: Session, category: str = None):
    from sqlalchemy import func, cast, Date, desc
    from datetime import date, timedelta
    today = date.today()
    days = [(today - timedelta(days=i)) for i in range(7, -1, -1)]
    # Get top 5 contributors
    top_query = db.query(ReportUser.created_by, func.count(ReportUser.id).label("total"))
    if category:
        top_query = top_query.filter(ReportUser.category == category)
    top_contributors = [cb for cb, _ in top_query.group_by(ReportUser.created_by).order_by(desc("total")).limit(5).all()]
    # Get daily totals for each contributor
    trend_data = {d: {cb: 0 for cb in top_contributors} for d in days}
    query = db.query(
        cast(ReportUser.created_at, Date).label("date"),
        ReportUser.created_by,
        func.count(ReportUser.id).label("total")
    ).filter(
        cast(ReportUser.created_at, Date) >= today - timedelta(days=7),
        cast(ReportUser.created_at, Date) <= today
    )
    if category:
        query = query.filter(ReportUser.category == category)
    query = query.filter(ReportUser.created_by.in_(top_contributors))
    results = query.group_by(cast(ReportUser.created_at, Date), ReportUser.created_by).all()
    for d, cb, total in results:
        if cb in top_contributors and d in trend_data:
            trend_data[d][cb] = total
    response = []
    for d in days:
        data = [{"name": cb, "total": trend_data[d][cb]} for cb in top_contributors]
        response.append({"date": d.isoformat(), "data": data})
    return response

def get_total_by_date_last_14_days(db: Session, category: str = None):
    from sqlalchemy import func, cast, Date
    from datetime import date, timedelta
    today = date.today()
    days = [(today - timedelta(days=i)) for i in range(14, -1, -1)]
    query = db.query(cast(ReportUser.when, Date), func.count(ReportUser.id))
    query = query.filter(
        cast(ReportUser.when, Date) >= today - timedelta(days=14),
        cast(ReportUser.when, Date) <= today
    )
    if category:
        query = query.filter(ReportUser.category == category)
    results = query.group_by(cast(ReportUser.when, Date)).all()
    result_dict = {d: 0 for d in days}
    for d, total in results:
        result_dict[d] = total
    return [{"date": d.isoformat(), "total": result_dict[d]} for d in days]

def get_heatmap(db: Session, category: str = None):
    from collections import defaultdict
    from datetime import datetime, timedelta
    from models.report_user import ReportUser
    today = datetime.today()
    start_month = today.replace(day=1)
    six_months_ago = (start_month - timedelta(days=1)).replace(day=1) - timedelta(days=30*5)
    query = db.query(ReportUser.when)
    if category:
        query = query.filter(ReportUser.category == category)
    query = query.filter(ReportUser.when >= six_months_ago)
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

def get_wordcloud(db: Session, category: str = None):
    from collections import Counter
    from models.report_user import ReportUser
    query = db.query(ReportUser.summary)
    if category:
        query = query.filter(ReportUser.category == category)
    data = query.all()
    words = []
    for (summary,) in data:
        if summary:
            words.extend(summary.lower().split())
    counter = Counter(words)
    # Return as list of dicts: [{"word": w, "count": c}, ...]
    return [{"word": w, "count": c} for w, c in counter.most_common()]

def get_total_by_sentiment(db: Session, category: str = None):
    from sqlalchemy import func
    query = db.query(ReportUser.sentiment, func.count(ReportUser.id))
    if category:
        query = query.filter(ReportUser.category == category)
    results = query.group_by(ReportUser.sentiment).all()
    return [{"sentiment": s, "total": total} for s, total in results]

def get_desc_data(db: Session, category: str = None):
    query = db.query(
        ReportUser.what,
        ReportUser.who,
        ReportUser.where,
        ReportUser.created_at
    )
    if category:
        query = query.filter(ReportUser.category == category)
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