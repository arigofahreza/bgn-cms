from shapely import wkb, force_2d
from shapely.geometry.geo import shape, mapping
from sqlalchemy import and_, literal, func, text
from sqlalchemy.orm import Session
from models import locations as l_models


def get_geometry(db: Session,
                 kd_propinsi: str | None = None,
                 kd_kabupaten: str | None = None,
                 kd_kecamatan: str | None = None,
                 kd_kelurahan: str | None = None
                 ):
    if not kd_propinsi and not kd_kabupaten and not kd_kecamatan and not kd_kelurahan:
        rows = db.execute(text("SELECT kd_propinsi, nm_propinsi, geom FROM propinsi")).fetchall()
        features = []
        for kd_propinsi, nm_propinsi, geom_hex in rows:
            shape = wkb.loads(bytes.fromhex(geom_hex.hex() if hasattr(geom_hex, "hex") else geom_hex))
            if shape.has_z:
                shape = force_2d(shape)
            feature = {
                'geometry': mapping(shape),
                'properties': {
                    "kd_propinsi": kd_propinsi,
                    "nm_propinsi": nm_propinsi
                }
            }
            features.append(feature)
        return {
            "type": "FeatureCollection",
            "features": features
        }
    elif kd_propinsi and not kd_kabupaten and not kd_kecamatan and not kd_kelurahan:
        rows = db.execute(
            text(f"select kd_propinsi, kd_kabupaten, nm_kabupaten, geom from kabupaten where kd_propinsi = '{kd_propinsi}'"))
        features = []
        for kd_propinsi, kd_kabupaten, nm_kabupaten, geom_hex in rows:
            shape = wkb.loads(bytes.fromhex(geom_hex.hex() if hasattr(geom_hex, "hex") else geom_hex))
            if shape.has_z:
                shape = force_2d(shape)
            feature = {
                'geometry': mapping(shape),
                'properties': {
                    "kd_propinsi": kd_propinsi,
                    "kd_kabupaten": kd_kabupaten,
                    "nm_kabupaten": nm_kabupaten
                }
            }
            features.append(feature)
        return {
            "type": "FeatureCollection",
            "features": features
        }
    elif kd_propinsi and kd_kabupaten and not kd_kecamatan and not kd_kelurahan:
        rows = db.execute(
            text(f"select kd_propinsi, kd_kabupaten, kd_kecamatan, nm_kecamatan, geom from kecamatan where kd_propinsi = '{kd_propinsi}' and kd_kabupaten = '{kd_kabupaten}'"))
        features = []
        for kd_propinsi, kd_kabupaten, kd_kecamatan, nm_kecamatan, geom_hex in rows:
            shape = wkb.loads(bytes.fromhex(geom_hex.hex() if hasattr(geom_hex, "hex") else geom_hex))
            if shape.has_z:
                shape = force_2d(shape)
            feature = {
                'geometry': mapping(shape),
                'properties': {
                    "kd_propinsi": kd_propinsi,
                    "kd_kabupaten": kd_kabupaten,
                    'kd_kecamatan': kd_kecamatan,
                    "nm_kecamatan": nm_kecamatan
                }
            }
            features.append(feature)
        return {
            "type": "FeatureCollection",
            "features": features
        }
    elif kd_propinsi and kd_kabupaten and kd_kecamatan and not kd_kelurahan:
        rows = db.execute(
            text(f"select kd_propinsi, kd_kabupaten, kd_kecamatan, kd_kelurahan, nm_kelurahan, geom from kelurahan where kd_propinsi = '{kd_propinsi}' and kd_kabupaten = '{kd_kabupaten}' and kd_kecamatan = '{kd_kecamatan}'"))
        features = []
        for kd_propinsi, kd_kabupaten, kd_kecamatan, kd_kelurahan, nm_kelurahan, geom_hex in rows:
            shape = wkb.loads(bytes.fromhex(geom_hex.hex() if hasattr(geom_hex, "hex") else geom_hex))
            if shape.has_z:
                shape = force_2d(shape)
            feature = {
                'geometry': mapping(shape),
                'properties': {
                    "kd_propinsi": kd_propinsi,
                    "kd_kabupaten": kd_kabupaten,
                    'kd_kecamatan': kd_kecamatan,
                    'kd_kelurahan': kd_kelurahan,
                    "nm_kelurahan": nm_kelurahan
                }
            }
            features.append(feature)
        return {
            "type": "FeatureCollection",
            "features": features
        }
    elif kd_propinsi and kd_kabupaten and kd_kecamatan and kd_kelurahan:
        rows = db.execute(
            text(f"select kd_propinsi, kd_kabupaten, kd_kecamatan, kd_kelurahan, nm_kelurahan, geom from kelurahan where kd_propinsi = '{kd_propinsi}' and kd_kabupaten = '{kd_kabupaten}' and kd_kecamatan = '{kd_kecamatan}' and kd_kelurahan = '{kd_kelurahan}'"))
        features = []
        for kd_propinsi, kd_kabupaten, kd_kecamatan, kd_kelurahan, nm_kelurahan, geom_hex in rows:
            shape = wkb.loads(bytes.fromhex(geom_hex.hex() if hasattr(geom_hex, "hex") else geom_hex))
            if shape.has_z:
                shape = force_2d(shape)
            feature = {
                'geometry': mapping(shape),
                'properties': {
                    "kd_propinsi": kd_propinsi,
                    "kd_kabupaten": kd_kabupaten,
                    'kd_kecamatan': kd_kecamatan,
                    'kd_kelurahan': kd_kelurahan,
                    "nm_kelurahan": nm_kelurahan
                }
            }
            features.append(feature)
        return {
            "type": "FeatureCollection",
            "features": features
        }

def get_centroid(db: Session,
                 kd_propinsi: str | None = None,
                 kd_kabupaten: str | None = None,
                 kd_kecamatan: str | None = None,
                 kd_kelurahan: str | None = None
                 ):
    if not kd_propinsi and not kd_kabupaten and not kd_kecamatan and not kd_kelurahan:
        sql = text("""
            WITH location AS (
                SELECT kd_propinsi, nm_propinsi, centroid 
                FROM propinsi
            ), data AS (
                SELECT COUNT(ru.id) AS count, u.location_id 
                FROM report_user ru
                INNER JOIN users u 
                    ON u.phone = ru.created_by_phone
                GROUP BY u.location_id
            ), split_location AS (
                SELECT 
                    count, 
                    (string_to_array(location_id, '.'))[1] AS kd_propinsi
                FROM data
            ), calculate AS (
                SELECT 
                    SUM(count) AS total, 
                    kd_propinsi 
                FROM split_location
                GROUP BY kd_propinsi
            )
            SELECT 
                c.*, 
                l.nm_propinsi,
                l.centroid 
            FROM calculate c 
            INNER JOIN location l 
                ON c.kd_propinsi = l.kd_propinsi;
            """)
        rows = db.execute(sql).fetchall()
        features = []
        for total, kd_propinsi, nm_propinsi, centroid_hex in rows:
            shape = wkb.loads(bytes.fromhex(centroid_hex.hex() if hasattr(centroid_hex, "hex") else centroid_hex))
            if shape.has_z:
                shape = force_2d(shape)
            feature = {
                'geometry': mapping(shape),
                'properties': {
                    'total': total,
                    "kd_propinsi": kd_propinsi,
                    "nm_propinsi": nm_propinsi
                }
            }
            features.append(feature)
        return {
            "type": "FeatureCollection",
            "features": features
        }
    elif kd_propinsi and not kd_kabupaten and not kd_kecamatan and not kd_kelurahan:
        sql = text(f'''
            with location as (
            select kd_propinsi, kd_kabupaten, nm_kabupaten, centroid from kabupaten where kd_propinsi = '{kd_propinsi}'
            ), data as (
            select count(ru.id), u.location_id from report_user ru
            inner join users u on u.phone = ru.created_by_phone
            group by u.location_id
            ), split_location as (
            select count, (string_to_array(location_id, '.'))[1] AS kd_propinsi,
                (string_to_array(location_id, '.'))[2] AS kd_kabupaten
            from data
            ), calculate as (
            select sum(count) as total, kd_propinsi, kd_kabupaten 
            from split_location
            where kd_propinsi = '{kd_propinsi}'
            group by kd_propinsi, kd_kabupaten
            ) select c.*, l.nm_kabupaten, l.centroid from calculate c 
            inner join location l 
            on c.kd_propinsi = l.kd_propinsi 
            and c.kd_kabupaten = l.kd_kabupaten 
        ''')
        rows = db.execute(sql).fetchall()
        features = []
        for total, kd_propinsi, kd_kabupaten, nm_kabupaten, centroid_hex in rows:
            shape = wkb.loads(bytes.fromhex(centroid_hex.hex() if hasattr(centroid_hex, "hex") else centroid_hex))
            if shape.has_z:
                shape = force_2d(shape)
            feature = {
                'geometry': mapping(shape),
                'properties': {
                    'total': total,
                    "kd_propinsi": kd_propinsi,
                    'kd_kabupaten': kd_kabupaten,
                    "nm_kabupaten": nm_kabupaten
                }
            }
            features.append(feature)
        return {
            "type": "FeatureCollection",
            "features": features
        }
    elif kd_propinsi and kd_kabupaten and not kd_kecamatan and not kd_kelurahan:
        sql = text(f'''
            with location as (
            select kd_propinsi, kd_kabupaten, kd_kecamatan, nm_kecamatan, centroid from kecamatan where kd_propinsi = '{kd_propinsi}' and kd_kabupaten = '{kd_kabupaten}'
            ), data as (
            select count(ru.id), u.location_id from report_user ru
            inner join users u on u.phone = ru.created_by_phone
            group by u.location_id
            ), split_location as (
            select count, (string_to_array(location_id, '.'))[1] AS kd_propinsi,
                (string_to_array(location_id, '.'))[2] AS kd_kabupaten,
                (string_to_array(location_id, '.'))[3] AS kd_kecamatan
            from data
            ), calculate as (
            select sum(count) as total, kd_propinsi, kd_kabupaten, kd_kecamatan 
            from split_location
            where kd_propinsi = '{kd_propinsi}' and kd_kabupaten = '{kd_kabupaten}'
            group by kd_propinsi, kd_kabupaten, kd_kecamatan
            ) select c.*, l.nm_kecamatan, l.centroid from calculate c 
            inner join location l 
            on c.kd_propinsi = l.kd_propinsi 
            and c.kd_kabupaten = l.kd_kabupaten 
            and c.kd_kecamatan  = l.kd_kecamatan 
                ''')
        rows = db.execute(sql).fetchall()
        features = []
        for total, kd_propinsi, kd_kabupaten, kd_kecamatan, nm_kecamatan, centroid_hex in rows:
            shape = wkb.loads(bytes.fromhex(centroid_hex.hex() if hasattr(centroid_hex, "hex") else centroid_hex))
            if shape.has_z:
                shape = force_2d(shape)
            feature = {
                'geometry': mapping(shape),
                'properties': {
                    'total': total,
                    "kd_propinsi": kd_propinsi,
                    'kd_kabupaten': kd_kabupaten,
                    'kd_kecamatan': kd_kecamatan,
                    "nm_kecamatan": nm_kecamatan
                }
            }
            features.append(feature)
        return {
            "type": "FeatureCollection",
            "features": features
        }
    elif kd_propinsi and kd_kabupaten and kd_kecamatan and not kd_kelurahan:
        sql = text(f'''
            with location as (
        select kd_propinsi, kd_kabupaten, kd_kecamatan, kd_kelurahan, nm_kelurahan, centroid 
        from kelurahan where kd_propinsi = '{kd_propinsi}' and kd_kabupaten = '{kd_kabupaten}' and kd_kecamatan = '{kd_kecamatan}'
        ), data as (
        select count(ru.id), u.location_id from report_user ru
        inner join users u on u.phone = ru.created_by_phone
        group by u.location_id
        ), split_location as (
        select count, (string_to_array(location_id, '.'))[1] AS kd_propinsi,
            (string_to_array(location_id, '.'))[2] AS kd_kabupaten,
            (string_to_array(location_id, '.'))[3] AS kd_kecamatan,
            (string_to_array(location_id, '.'))[4] AS kd_kelurahan
        from data
        ), calculate as (
        select sum(count) as total, kd_propinsi, kd_kabupaten, kd_kecamatan, kd_kelurahan 
        from split_location
        where kd_propinsi = '{kd_propinsi}' and kd_kabupaten = '{kd_kabupaten}' and kd_kecamatan = '{kd_kecamatan}'
        group by kd_propinsi, kd_kabupaten, kd_kecamatan, kd_kelurahan
        ) select c.*, l.nm_kelurahan, l.centroid from calculate c 
        inner join location l 
        on c.kd_propinsi = l.kd_propinsi 
        and c.kd_kabupaten = l.kd_kabupaten 
        and c.kd_kecamatan  = l.kd_kecamatan 
        and c.kd_kelurahan = l.kd_kelurahan
        ''')
        rows = db.execute(sql).fetchall()
        features = []
        for total, kd_propinsi, kd_kabupaten, kd_kecamatan, kd_kelurahan, nm_kelurahan, centroid_hex in rows:
            shape = wkb.loads(bytes.fromhex(centroid_hex.hex() if hasattr(centroid_hex, "hex") else centroid_hex))
            if shape.has_z:
                shape = force_2d(shape)
            feature = {
                'geometry': mapping(shape),
                'properties': {
                    'total': total,
                    "kd_propinsi": kd_propinsi,
                    'kd_kabupaten': kd_kabupaten,
                    'kd_kecamatan': kd_kecamatan,
                    'kd_kelurahan': kd_kelurahan,
                    "nm_kelurahan": nm_kelurahan
                }
            }
            features.append(feature)
        return {
            "type": "FeatureCollection",
            "features": features
        }
    elif kd_propinsi and kd_kabupaten and kd_kecamatan and kd_kelurahan:
        sql = text(f'''
            with location as (
        select kd_propinsi, kd_kabupaten, kd_kecamatan, kd_kelurahan, nm_kelurahan, centroid 
        from kelurahan where kd_propinsi = '{kd_propinsi}' and kd_kabupaten = '{kd_kabupaten}' and kd_kecamatan = '{kd_kecamatan}' and kd_kelurahan = '{kd_kelurahan}'
        ), data as (
        select count(ru.id), u.location_id from report_user ru
        inner join users u on u.phone = ru.created_by_phone
        group by u.location_id
        ), split_location as (
        select count, (string_to_array(location_id, '.'))[1] AS kd_propinsi,
            (string_to_array(location_id, '.'))[2] AS kd_kabupaten,
            (string_to_array(location_id, '.'))[3] AS kd_kecamatan,
            (string_to_array(location_id, '.'))[4] AS kd_kelurahan
        from data
        ), calculate as (
        select sum(count) as total, kd_propinsi, kd_kabupaten, kd_kecamatan, kd_kelurahan 
        from split_location
        where kd_propinsi = '{kd_propinsi}' and kd_kabupaten = '{kd_kabupaten}' and kd_kecamatan = '{kd_kecamatan}' and kd_kelurahan = '{kd_kelurahan}'
        group by kd_propinsi, kd_kabupaten, kd_kecamatan, kd_kelurahan
        ) select c.*, l.nm_kelurahan, l.centroid from calculate c 
        inner join location l 
        on c.kd_propinsi = l.kd_propinsi 
        and c.kd_kabupaten = l.kd_kabupaten 
        and c.kd_kecamatan  = l.kd_kecamatan 
        and c.kd_kelurahan = l.kd_kelurahan
        ''')
        rows = db.execute(sql).fetchall()
        features = []
        for total, kd_propinsi, kd_kabupaten, kd_kecamatan, kd_kelurahan, nm_kelurahan, centroid_hex in rows:
            shape = wkb.loads(bytes.fromhex(centroid_hex.hex() if hasattr(centroid_hex, "hex") else centroid_hex))
            if shape.has_z:
                shape = force_2d(shape)
            feature = {
                'geometry': mapping(shape),
                'properties': {
                    'total': total,
                    "kd_propinsi": kd_propinsi,
                    'kd_kabupaten': kd_kabupaten,
                    'kd_kecamatan': kd_kecamatan,
                    'kd_kelurahan': kd_kelurahan,
                    "nm_kelurahan": nm_kelurahan
                }
            }
            features.append(feature)
        return {
            "type": "FeatureCollection",
            "features": features
        }

