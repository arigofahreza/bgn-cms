from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
import database
from schemas import notes
from crud import modals as crud

router = APIRouter(prefix="/modals", tags=["Modals"])


@router.get("/list-notes")
def list_notes(chat_id: str = Query(), db: Session = Depends(database.get_db)):
    return crud.list_notes(db, chat_id)


@router.get("/list-chats")
def list_chats(chat_id: str = Query(), db: Session = Depends(database.get_db)):
    return crud.list_chats(db, chat_id)


@router.put("/update-progress")
def update_progress(progress: int = Query(), chat_id: str = Query(), db: Session = Depends(database.get_db)):
    return crud.update_progress(db, chat_id, progress)


@router.put("/update-status")
def update_status(status: str = Query(), chat_id: str = Query(), db: Session = Depends(database.get_db)):
    return crud.update_status(db, chat_id, status)


@router.post("/create", response_model=notes.NoteOut)
def create_note(note: notes.NoteCreate, db: Session = Depends(database.get_db)):
    return crud.create_notes(db, note)


@router.put("/update", response_model=notes.NoteOut)
def update_note(note: notes.NoteUpdate, db: Session = Depends(database.get_db), note_id: int = Query()):
    updated = crud.update_notes(db, note_id, note)
    if not updated:
        raise HTTPException(status_code=404, detail="Note not found")
    return updated


@router.delete("/delete", response_model=notes.NoteOut)
def delete_note(db: Session = Depends(database.get_db), note_id: int = Query()):
    deleted = crud.delete_notes(db, note_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Note not found")
    return deleted
