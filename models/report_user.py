from sqlalchemy import Column, Integer, Text, Date, String, TIMESTAMP, Boolean
from database import Base


class ReportUser(Base):
    __tablename__ = "report_user"

    id = Column(Integer, primary_key=True, index=True)
    what = Column(Text)
    who = Column(Text)
    when = Column(Date)
    where = Column(Text)
    why = Column(Text)
    how = Column(Text)
    category = Column(String(100))
    summary = Column(Text)
    created_at = Column(TIMESTAMP)
    created_by = Column(String(100))
    created_by_phone = Column(String(20))
    sentiment = Column(String)
    progress = Column(Integer)
    status = Column(Text)
    parent_id = Column(Text)
    chat_id = Column(Text)
    is_verified = Column(Boolean)
