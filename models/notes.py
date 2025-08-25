from sqlalchemy import Column, Integer, String, Text, TIMESTAMP, func
from database import Base

class Notes(Base):
    __tablename__ = "notes"

    id = Column(Integer, primary_key=True, index=True)
    note = Column(Text, nullable=False)
    chat_id = Column(Text, nullable=True)
    created_by = Column(Integer, nullable=True)
    created_at = Column(TIMESTAMP, server_default=func.now())
    updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now())
