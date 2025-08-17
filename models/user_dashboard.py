from sqlalchemy import Column, Integer, String, Text, TIMESTAMP, func
from database import Base

class UserDasboard(Base):
    __tablename__ = "users_dashboard"

    id = Column(Integer, primary_key=True, index=True)
    nama = Column(Text, nullable=False)
    alamat = Column(Text, nullable=True)
    no_telepon = Column(Text, nullable=True)
    email = Column(Text, nullable=True)
    password = Column(Text, nullable=True)
    location_id = Column(Text, nullable=True)
    created_at = Column(TIMESTAMP, server_default=func.now())
    updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now())
