from sqlalchemy.orm import Session

from models import user_dashboard as u_models


def get_user_by_credentials(db: Session, username: str, password: str):
    result = (db.query(u_models.UserDasboard).filter(u_models.UserDasboard.nama == username, u_models.UserDasboard.password == password).first())
    return result