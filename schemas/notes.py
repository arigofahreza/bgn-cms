from pydantic import BaseModel
from typing import Optional
from datetime import datetime


class NoteBase(BaseModel):
    note: str
    chat_id: str
    created_by: int


class NoteCreate(NoteBase):
    pass


class NoteUpdate(NoteBase):
    pass


class NoteOut(NoteBase):
    id: int
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True  # for SQLAlchemy ORM
