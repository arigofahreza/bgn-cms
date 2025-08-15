from shapely.geometry.geo import shape
from sqlalchemy import and_, literal, func
from sqlalchemy.orm import Session

from utils.helpers import create_propinsi, get_zoom_and_centroid, create_kabupaten, create_kecamatan, create_kelurahan
from models import locations as models
import database
from schemas import locations as schemas


def get_locations(db: Session,
                  kd_propinsi: str | None = None,
                  kd_kabupaten: str | None = None,
                  kd_kecamatan: str | None = None,
):
    if kd_propinsi and kd_kabupaten and kd_kecamatan:
        return (
            db.query(
                models.Kelurahan.kd_kelurahan,
                models.Kelurahan.nm_kelurahan,
                models.Kelurahan.kd_kecamatan,
                models.Kecamatan.nm_kecamatan,
                models.Kelurahan.kd_kabupaten,
                models.Kabupaten.nm_kabupaten,
                models.Kelurahan.kd_propinsi,
                models.Propinsi.nm_propinsi,
                literal("kelurahan").label("category")
            )
            .join(
                models.Kecamatan,
                and_(
                    models.Kelurahan.kd_kecamatan == models.Kecamatan.kd_kecamatan,
                    models.Kelurahan.kd_kabupaten == models.Kecamatan.kd_kabupaten,
                    models.Kelurahan.kd_propinsi == models.Kecamatan.kd_propinsi
                )
            )
            .join(
                models.Kabupaten,
                and_(
                    models.Kelurahan.kd_kabupaten == models.Kabupaten.kd_kabupaten,
                    models.Kelurahan.kd_propinsi == models.Kabupaten.kd_propinsi
                )
            )
            .join(
                models.Propinsi,
                and_(
                    models.Kelurahan.kd_propinsi == models.Propinsi.kd_propinsi
                )
            )
            .filter(
                and_(
                    models.Kelurahan.kd_kecamatan == kd_kecamatan,
                    models.Kelurahan.kd_kabupaten == kd_kabupaten,
                    models.Kelurahan.kd_propinsi == kd_propinsi
                )
            )
            .all()
        )
    elif kd_propinsi and kd_kabupaten and not kd_kecamatan:
        return (
            db.query(
                models.Kecamatan.kd_kecamatan,
                models.Kecamatan.nm_kecamatan,
                models.Kecamatan.kd_kabupaten,
                models.Kabupaten.nm_kabupaten,
                models.Kecamatan.kd_propinsi,
                models.Propinsi.nm_propinsi,
                literal("kecamatan").label("category")
            )
            .join(
                models.Kabupaten,
                and_(
                    models.Kecamatan.kd_kabupaten == models.Kabupaten.kd_kabupaten,
                    models.Kecamatan.kd_propinsi == models.Kabupaten.kd_propinsi
                )
            )
            .join(
                models.Propinsi,
                and_(
                    models.Kecamatan.kd_propinsi == models.Propinsi.kd_propinsi
                )
            )
            .filter(
                and_(
                    models.Kecamatan.kd_kabupaten == kd_kabupaten,
                    models.Kecamatan.kd_propinsi == kd_propinsi
                )
            )
            .all()
        )
    elif kd_propinsi and not kd_kabupaten and not kd_kecamatan:
        return (
            db.query(
                models.Kabupaten.kd_kabupaten,
                models.Kabupaten.nm_kabupaten,
                models.Kabupaten.kd_propinsi,
                models.Propinsi.nm_propinsi,
                literal("kabupaten").label("category")
            )
            .join(
                models.Propinsi,
                and_(
                    models.Kabupaten.kd_propinsi == models.Propinsi.kd_propinsi
                )
            )
            .filter(
                and_(
                    models.Kabupaten.kd_propinsi == kd_propinsi
                )
            )
            .all()
        )
    elif not kd_propinsi and not kd_kabupaten and not kd_kecamatan:
        return (
            db.query(
                models.Propinsi.kd_propinsi,
                models.Propinsi.nm_propinsi,
                literal("propinsi").label("category")
            )
            .all()
        )


def get_location(db: Session,
                 kd_propinsi: str | None = None,
                 kd_kabupaten: str | None = None,
                 kd_kecamatan: str | None = None,
                 kd_kelurahan: str | None = None
                 ):
    if kd_propinsi and kd_kabupaten and kd_kecamatan and kd_kelurahan:
        return (
            db.query(
                models.Kelurahan.kd_kelurahan,
                models.Kelurahan.nm_kelurahan,
                models.Kelurahan.kd_kecamatan,
                models.Kecamatan.nm_kecamatan,
                models.Kelurahan.kd_kabupaten,
                models.Kabupaten.nm_kabupaten,
                models.Kelurahan.kd_propinsi,
                models.Propinsi.nm_propinsi,
                literal("kelurahan").label("category")
            )
            .join(
                models.Kecamatan,
                and_(
                    models.Kelurahan.kd_kecamatan == models.Kecamatan.kd_kecamatan,
                    models.Kelurahan.kd_kabupaten == models.Kecamatan.kd_kabupaten,
                    models.Kelurahan.kd_propinsi == models.Kecamatan.kd_propinsi
                )
            )
            .join(
                models.Kabupaten,
                and_(
                    models.Kelurahan.kd_kabupaten == models.Kabupaten.kd_kabupaten,
                    models.Kelurahan.kd_propinsi == models.Kabupaten.kd_propinsi
                )
            )
            .join(
                models.Propinsi,
                and_(
                    models.Kelurahan.kd_propinsi == models.Propinsi.kd_propinsi
                )
            )
            .filter(
                and_(
                    models.Kelurahan.kd_kelurahan == kd_kelurahan,
                    models.Kelurahan.kd_kecamatan == kd_kecamatan,
                    models.Kelurahan.kd_kabupaten == kd_kabupaten,
                    models.Kelurahan.kd_propinsi == kd_propinsi
                )
            )
            .first()
        )
    elif kd_propinsi and kd_kabupaten and kd_kecamatan and not kd_kelurahan:
        return (
            db.query(
                models.Kecamatan.kd_kecamatan,
                models.Kecamatan.nm_kecamatan,
                models.Kecamatan.kd_kabupaten,
                models.Kabupaten.nm_kabupaten,
                models.Kecamatan.kd_propinsi,
                models.Propinsi.nm_propinsi,
                literal("kecamatan").label("category")
            )
            .join(
                models.Kabupaten,
                and_(
                    models.Kecamatan.kd_kabupaten == models.Kabupaten.kd_kabupaten,
                    models.Kecamatan.kd_propinsi == models.Kabupaten.kd_propinsi
                )
            )
            .join(
                models.Propinsi,
                and_(
                    models.Kecamatan.kd_propinsi == models.Propinsi.kd_propinsi
                )
            )
            .filter(
                and_(
                    models.Kecamatan.kd_kecamatan == kd_kecamatan,
                    models.Kecamatan.kd_kabupaten == kd_kabupaten,
                    models.Kecamatan.kd_propinsi == kd_propinsi
                )
            )
            .first()
        )
    elif kd_propinsi and kd_kabupaten and not kd_kecamatan and not kd_kelurahan:
        return (
            db.query(
                models.Kabupaten.kd_kabupaten,
                models.Kabupaten.nm_kabupaten,
                models.Kabupaten.kd_propinsi,
                models.Propinsi.nm_propinsi,
                literal("kabupaten").label("category")
            )
            .join(
                models.Propinsi,
                and_(
                    models.Kabupaten.kd_propinsi == models.Propinsi.kd_propinsi
                )
            )
            .filter(
                and_(
                    models.Kabupaten.kd_kabupaten == kd_kabupaten,
                    models.Kabupaten.kd_propinsi == kd_propinsi
                )
            )
            .first()
        )
    elif kd_propinsi and not kd_kabupaten and not kd_kecamatan and not kd_kelurahan:
        return (
            db.query(
                models.Propinsi.kd_propinsi,
                models.Propinsi.nm_propinsi,
                literal("propinsi").label("category")
            )
            .filter(
                and_(
                    models.Propinsi.kd_propinsi == kd_propinsi
                )
            )
            .first()
        )


def create_location(db: Session,
                    param: schemas.LocationCreate
):
    if param.kd_propinsi and param.kd_kabupaten and param.kd_kecamatan and param.kd_kelurahan:
        props = param.dict()
        geo = props.pop('geom')
        feature = geo["features"][0]
        g = shape(feature["geometry"])
        metrics = get_zoom_and_centroid(g, map_width_px=1024)
        db_kelurahan = create_kelurahan(props, g, metrics)
        db.add(db_kelurahan)
        db.commit()
        db.refresh(db_kelurahan)
        return db_kelurahan
    elif param.kd_propinsi and param.kd_kabupaten and param.kd_kecamatan and not param.kd_kelurahan:
        props = param.dict()
        geo = props.pop('geom')
        feature = geo["features"][0]
        g = shape(feature["geometry"])
        metrics = get_zoom_and_centroid(g, map_width_px=1024)
        db_kecamatan = create_kecamatan(props, g, metrics)
        db.add(db_kecamatan)
        db.commit()
        db.refresh(db_kecamatan)
        return db_kecamatan
    elif param.kd_propinsi and param.kd_kabupaten and not param.kd_kecamatan and not param.kd_kelurahan:
        props = param.dict()
        geo = props.pop('geom')
        feature = geo["features"][0]
        g = shape(feature["geometry"])
        metrics = get_zoom_and_centroid(g, map_width_px=1024)
        db_kabupaten = create_kabupaten(props, g, metrics)
        db.add(db_kabupaten)
        db.commit()
        db.refresh(db_kabupaten)
        return db_kabupaten
    elif param.kd_propinsi and not param.kd_kabupaten and not param.kd_kecamatan and not param.kd_kelurahan:
        props = param.dict()
        geo = props.pop('geom')
        feature = geo["features"][0]
        g = shape(feature["geometry"])
        metrics = get_zoom_and_centroid(g, map_width_px=1024)
        db_propinsi = create_propinsi(props, g, metrics)
        db.add(db_propinsi)
        db.commit()
        db.refresh(db_propinsi)
        return db_propinsi

def delete_location(db: Session,
                    kd_propinsi: str | None = None,
                    kd_kabupaten: str | None = None,
                    kd_kecamatan: str | None = None,
                    kd_kelurahan: str | None = None
):
    if kd_propinsi and kd_kabupaten and kd_kecamatan and kd_kelurahan:
        db_location = db.query(models.Kelurahan).filter(
                and_(
                    models.Kelurahan.kd_kelurahan == kd_kelurahan,
                    models.Kelurahan.kd_kecamatan == kd_kecamatan,
                    models.Kelurahan.kd_kabupaten == kd_kabupaten,
                    models.Kelurahan.kd_propinsi == kd_propinsi
                )
            ).first()
        if not db_location:
            return None
        db.delete(db_location)
        db.commit()
        return db_location
    elif kd_propinsi and kd_kabupaten and kd_kecamatan and not kd_kelurahan:
        db_location = db.query(models.Kecamatan).filter(
            and_(
                models.Kecamatan.kd_kecamatan == kd_kecamatan,
                models.Kecamatan.kd_kabupaten == kd_kabupaten,
                models.Kecamatan.kd_propinsi == kd_propinsi
            )
        ).first()
        if not db_location:
            return None
        db.delete(db_location)
        db.commit()
        return db_location
    elif kd_propinsi and kd_kabupaten and not kd_kecamatan and not kd_kelurahan:
        db_location = db.query(models.Kabupaten).filter(
            and_(
                models.Kabupaten.kd_kabupaten == kd_kabupaten,
                models.Kabupaten.kd_propinsi == kd_propinsi
            )
        ).first()
        if not db_location:
            return None
        db.delete(db_location)
        db.commit()
        return db_location
    elif kd_propinsi and not kd_kabupaten and not kd_kecamatan and not kd_kelurahan:
        db_location = db.query(models.Propinsi).filter(
            and_(
                models.Propinsi.kd_propinsi == kd_propinsi
            )
        ).first()
        if not db_location:
            return None
        db.delete(db_location)
        db.commit()
        return db_location