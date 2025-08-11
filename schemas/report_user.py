from pydantic import BaseModel
from typing import Optional
from datetime import date, datetime

class ReportUserBase(BaseModel):
    what: Optional[str]
    who: Optional[str]
    when: Optional[date]
    where: Optional[str]
    why: Optional[str]
    how: Optional[str]
    category: Optional[str]
    summary: Optional[str]
    created_at: Optional[datetime]
    created_by: Optional[str]
    created_by_phone: Optional[str]
    sentiment: Optional[str]

class ReportUserCreate(ReportUserBase):
    pass

class ReportUser(ReportUserBase):
    id: int

    class Config:
        orm_mode = True
