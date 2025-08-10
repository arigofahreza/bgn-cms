from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
import database
from schemas import users
from crud import users as crud

router = APIRouter(prefix="/users", tags=["Users"])

@router.get("/list")
def list_users(page: int = 1, limit: int = 100, db: Session = Depends(database.get_db)):
    return crud.get_users(db, page, limit)

@router.get("/select/", response_model=users.UserOut)
def read_user(db: Session = Depends(database.get_db), user_id: int = Query()):
    user = crud.get_user(db, user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return user

@router.post("/create/", response_model=users.UserOut)
def create_user(user: users.UserCreate, db: Session = Depends(database.get_db)):
    return crud.create_user(db, user)

@router.put("/update/", response_model=users.UserOut)
def update_user(user: users.UserUpdate, db: Session = Depends(database.get_db), user_id: int = Query()):
    updated = crud.update_user(db, user_id, user)
    if not updated:
        raise HTTPException(status_code=404, detail="User not found")
    return updated

@router.delete("/delete/", response_model=users.UserOut)
def delete_user(db: Session = Depends(database.get_db), user_id: int = Query()):
    deleted = crud.delete_user(db, user_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="User not found")
    return deleted