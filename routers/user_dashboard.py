from fastapi import APIRouter, HTTPException, Depends
from sqlalchemy.orm import Session

import database
from schemas.user_dashboard import UserLogin
from crud.user_dasboard import get_user_by_credentials

router = APIRouter(prefix='/auth', tags=['Auth'])

@router.post("/login")
def login(user: UserLogin, db: Session = Depends(database.get_db)):
    db_user = get_user_by_credentials(db, user.username, user.password)
    if not db_user:
        raise HTTPException(status_code=401, detail="Invalid credentials")
    return db_user
