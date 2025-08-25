from sqlalchemy import and_, cast, Date, literal_column, text, func, or_
from sqlalchemy.orm import Session

from utils.generator import filter_query_location
from models.report_user import ReportUser
from models.users import User
from datetime import datetime


def update_progress(db: Session, chat_id: str, progress: int):
    query = (db.query(ReportUser))
    query = query.filter(
        or_(
            ReportUser.parent_id == chat_id,
            ReportUser.chat_id == chat_id
        )
    )
    query.update(
        {
            ReportUser.progress: progress
        },
        synchronize_session=False
    )
    db.commit()
    return {
        "status": "success"
    }


def update_status(db: Session, chat_id: str, status: str):
    query = (db.query(ReportUser))
    query = query.filter(
        or_(
            ReportUser.parent_id == chat_id,
            ReportUser.chat_id == chat_id
        )
    )
    query.update(
        {
            ReportUser.status: status
        },
        synchronize_session=False
    )
    db.commit()
    return {
        "status": "success"
    }


def list_chats(db: Session, chat_id: str):
    query = (db.query(ReportUser.summmary, ReportUser.created_at, User.name, ReportUser.parent_id, ReportUser.chat_id,
                      ReportUser.progress, ReportUser.status, ReportUser.is_verified)
             .join(User, User.phone == ReportUser.created_by_phone))
    query = query.filter(
        or_(
            ReportUser.parent_id == chat_id,
            ReportUser.chat_id == chat_id
        )
    )
    results = query.order_by(ReportUser.created_at).all()
    return [{"summary": summary, "created_at": created_at, "name": name, "parent_id": parent_id, "chat_id": chat_id,
             "progress": progress, "status": status, "is_verified": is_verified} for
            summary, created_at, name, parent_id, chat_id, progress, status, is_verified in results]


def list_notes():
    pass


def create_notes():
    pass


def delete_notes():
    pass


def update_notes():
    pass
