from sqlalchemy import Column, Integer, String, Text, DateTime, func
from database import Base

class ReportMetadata(Base):
    __tablename__ = "report_metadata"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    title = Column(Text, nullable=False)
    category = Column(String(100))
    url = Column(Text)
    generated_at = Column(DateTime, default=func.now())