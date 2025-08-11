from sqlalchemy import desc, func, literal, and_
from sqlalchemy.orm import Session
from models import users as u_models
from models import locations as l_models
import database
from schemas import users as schemas


def get_users(db: Session, page: int = 0, limit: int = 100):
    offset = (page - 1) * limit
    total = db.query(func.count(u_models.User.id)).scalar()
    results = (
        db.query(u_models.User)
        .order_by(desc(u_models.User.created_at))
        .offset(offset)
        .limit(limit)
        .all()
    )

    json_results = [row.__dict__ for row in results]
    for result in json_results:
        if result.get('location_id'):
            split_location_id = result.get('location_id').split('.')
            if len(split_location_id) == 1:
                location_result = (db.query(
                    l_models.Propinsi.nm_propinsi,
                )
                                   .filter(
                    and_(
                        l_models.Propinsi.kd_propinsi == split_location_id[0]
                    )
                )
                                   .first())
                if location_result:
                    result['nm_propinsi'] = location_result.nm_propinsi
            elif len(split_location_id) == 2:
                location_result = (
                    db.query(
                        l_models.Kabupaten.nm_kabupaten,
                        l_models.Propinsi.nm_propinsi,
                    )
                    .join(
                        l_models.Propinsi,
                        and_(
                            l_models.Kabupaten.kd_propinsi == l_models.Propinsi.kd_propinsi
                        )
                    )
                    .filter(
                        and_(
                            l_models.Kabupaten.kd_kabupaten == split_location_id[1],
                            l_models.Kabupaten.kd_propinsi == split_location_id[0]
                        )
                    )
                    .first()
                )
                if location_result:
                    result['nm_propinsi'] = location_result.nm_propinsi
                    result['nm_kabupaten'] = location_result.nm_kabupaten
            elif len(split_location_id) == 3:
                location_result = (
                    db.query(
                        l_models.Kecamatan.nm_kecamatan,
                        l_models.Kabupaten.nm_kabupaten,
                        l_models.Propinsi.nm_propinsi,
                    )
                    .join(
                        l_models.Kabupaten,
                        and_(
                            l_models.Kecamatan.kd_kabupaten == l_models.Kabupaten.kd_kabupaten,
                            l_models.Kecamatan.kd_propinsi == l_models.Kabupaten.kd_propinsi
                        )
                    )
                    .join(
                        l_models.Propinsi,
                        and_(
                            l_models.Kecamatan.kd_propinsi == l_models.Propinsi.kd_propinsi
                        )
                    )
                    .filter(
                        and_(
                            l_models.Kecamatan.kd_kecamatan == split_location_id[2],
                            l_models.Kecamatan.kd_kabupaten == split_location_id[1],
                            l_models.Kecamatan.kd_propinsi == split_location_id[0]
                        )
                    )
                    .first()
                )
                if location_result:
                    result['nm_propinsi'] = location_result.nm_propinsi
                    result['nm_kabupaten'] = location_result.nm_kabupaten
                    result['nm_kecamatan'] = location_result.nm_kecamatan
            elif len(split_location_id) == 4:
                location_result = (
                    db.query(
                        l_models.Kelurahan.nm_kelurahan,
                        l_models.Kecamatan.nm_kecamatan,
                        l_models.Kabupaten.nm_kabupaten,
                        l_models.Propinsi.nm_propinsi,
                    )
                    .join(
                        l_models.Kecamatan,
                        and_(
                            l_models.Kelurahan.kd_kecamatan == l_models.Kecamatan.kd_kecamatan,
                            l_models.Kelurahan.kd_kabupaten == l_models.Kecamatan.kd_kabupaten,
                            l_models.Kelurahan.kd_propinsi == l_models.Kecamatan.kd_propinsi
                        )
                    )
                    .join(
                        l_models.Kabupaten,
                        and_(
                            l_models.Kelurahan.kd_kabupaten == l_models.Kabupaten.kd_kabupaten,
                            l_models.Kelurahan.kd_propinsi == l_models.Kabupaten.kd_propinsi
                        )
                    )
                    .join(
                        l_models.Propinsi,
                        and_(
                            l_models.Kelurahan.kd_propinsi == l_models.Propinsi.kd_propinsi
                        )
                    )
                    .filter(
                        and_(
                            l_models.Kelurahan.kd_kelurahan == split_location_id[3],
                            l_models.Kelurahan.kd_kecamatan == split_location_id[2],
                            l_models.Kelurahan.kd_kabupaten == split_location_id[1],
                            l_models.Kelurahan.kd_propinsi == split_location_id[0]
                        )
                    )
                    .first()
                )
                if location_result:
                    result['nm_propinsi'] = location_result.nm_propinsi
                    result['nm_kabupaten'] = location_result.nm_kabupaten
                    result['nm_kecamatan'] = location_result.nm_kecamatan
                    result['nm_kabupaten'] = location_result.nm_kabupaten
    return {
        "page": page,
        "limit": limit,
        "total": total,
        "total_page": (total + limit - 1) // limit,
        "data": json_results
    }


def get_user(db: Session, user_id: int):
    result = (db.query(u_models.User).filter(u_models.User.id == user_id).first())
    json_result = result.__dict__
    json_result.pop('_sa_instance_state')
    if json_result.get('location_id'):
        split_location_id = json_result.get('location_id').split('.')
        if len(split_location_id) == 1:
            location_result = (db.query(
                l_models.Propinsi.nm_propinsi,
            )
            .filter(
                and_(
                    l_models.Propinsi.kd_propinsi == split_location_id[0]
                )
            )
            .first())
            if location_result:
                json_result['nm_propinsi'] = location_result.nm_propinsi
        elif len(split_location_id) == 2:
            location_result = (
                db.query(
                    l_models.Kabupaten.nm_kabupaten,
                    l_models.Propinsi.nm_propinsi,
                )
                .join(
                    l_models.Propinsi,
                    and_(
                        l_models.Kabupaten.kd_propinsi == l_models.Propinsi.kd_propinsi
                    )
                )
                .filter(
                    and_(
                        l_models.Kabupaten.kd_kabupaten == split_location_id[1],
                        l_models.Kabupaten.kd_propinsi == split_location_id[0]
                    )
                )
                .first()
            )
            if location_result:
                json_result['nm_propinsi'] = location_result.nm_propinsi
                json_result['nm_kabupaten'] = location_result.nm_kabupaten
        elif len(split_location_id) == 3:
            location_result = (
                db.query(
                    l_models.Kecamatan.nm_kecamatan,
                    l_models.Kabupaten.nm_kabupaten,
                    l_models.Propinsi.nm_propinsi,
                )
                .join(
                    l_models.Kabupaten,
                    and_(
                        l_models.Kecamatan.kd_kabupaten == l_models.Kabupaten.kd_kabupaten,
                        l_models.Kecamatan.kd_propinsi == l_models.Kabupaten.kd_propinsi
                    )
                )
                .join(
                    l_models.Propinsi,
                    and_(
                        l_models.Kecamatan.kd_propinsi == l_models.Propinsi.kd_propinsi
                    )
                )
                .filter(
                    and_(
                        l_models.Kecamatan.kd_kecamatan == split_location_id[2],
                        l_models.Kecamatan.kd_kabupaten == split_location_id[1],
                        l_models.Kecamatan.kd_propinsi == split_location_id[0]
                    )
                )
                .first()
            )
            if location_result:
                json_result['nm_propinsi'] = location_result.nm_propinsi
                json_result['nm_kabupaten'] = location_result.nm_kabupaten
                json_result['nm_kecamatan'] = location_result.nm_kecamatan
        elif len(split_location_id) == 4:
            location_result = (
                db.query(
                    l_models.Kelurahan.nm_kelurahan,
                    l_models.Kecamatan.nm_kecamatan,
                    l_models.Kabupaten.nm_kabupaten,
                    l_models.Propinsi.nm_propinsi,
                )
                .join(
                    l_models.Kecamatan,
                    and_(
                        l_models.Kelurahan.kd_kecamatan == l_models.Kecamatan.kd_kecamatan,
                        l_models.Kelurahan.kd_kabupaten == l_models.Kecamatan.kd_kabupaten,
                        l_models.Kelurahan.kd_propinsi == l_models.Kecamatan.kd_propinsi
                    )
                )
                .join(
                    l_models.Kabupaten,
                    and_(
                        l_models.Kelurahan.kd_kabupaten == l_models.Kabupaten.kd_kabupaten,
                        l_models.Kelurahan.kd_propinsi == l_models.Kabupaten.kd_propinsi
                    )
                )
                .join(
                    l_models.Propinsi,
                    and_(
                        l_models.Kelurahan.kd_propinsi == l_models.Propinsi.kd_propinsi
                    )
                )
                .filter(
                    and_(
                        l_models.Kelurahan.kd_kelurahan == split_location_id[3],
                        l_models.Kelurahan.kd_kecamatan == split_location_id[2],
                        l_models.Kelurahan.kd_kabupaten == split_location_id[1],
                        l_models.Kelurahan.kd_propinsi == split_location_id[0]
                    )
                )
                .first()
            )
            if location_result:
                json_result['nm_propinsi'] = location_result.nm_propinsi
                json_result['nm_kabupaten'] = location_result.nm_kabupaten
                json_result['nm_kecamatan'] = location_result.nm_kecamatan
                json_result['nm_kabupaten'] = location_result.nm_kabupaten
    return json_result


def create_user(db: Session, user: schemas.UserCreate):
    db_user = u_models.User(**user.dict())
    db.add(db_user)
    db.commit()
    db.refresh(db_user)
    return db_user


def update_user(db: Session, user_id: int, user: schemas.UserUpdate):
    db_user = db.query(u_models.User).filter(u_models.User.id == user_id).first()
    if not db_user:
        return None
    for key, value in user.dict(exclude_unset=True).items():
        setattr(db_user, key, value)
    db.commit()
    db.refresh(db_user)
    return db_user


def delete_user(db: Session, user_id: int):
    db_user = db.query(u_models.User).filter(u_models.User.id == user_id).first()
    if not db_user:
        return None
    db.delete(db_user)
    db.commit()
    return db_user
