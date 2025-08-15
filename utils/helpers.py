import math

from sqlalchemy import func
from models import locations
from shapely.geometry import shape, MultiPolygon, Polygon

def create_propinsi(props, geom_wkb, metrics):
    db_propinsi = locations.Propinsi(
        kd_propinsi=props.get("KODE_PROV"),
        nm_propinsi=props.get("PROVINSI"),
        geom=func.ST_SetSRID(func.ST_GeomFromWKB(geom_wkb), 4326),
        centroid=func.ST_SetSRID(
            func.ST_MakePoint(metrics["centroid_lon"], metrics["centroid_lat"]),
            4326
        ),
        zoom_width=metrics["zoom_width"],
        zoom_height=metrics["zoom_height"],
        best_zoom=metrics["best_zoom"]
    )
    return db_propinsi

def create_kabupaten(props, geom_wkb, metrics):
    db_kabupaten = locations.Kabupaten(
        kd_propinsi=props.get("kd_propinsi"),
        kd_kabupaten=props.get('kd_kabupaten'),
        nm_kabupaten=props.get("nm_kabupaten"),
        geom=func.ST_SetSRID(func.ST_GeomFromWKB(geom_wkb), 4326),
        centroid=func.ST_SetSRID(
            func.ST_MakePoint(metrics["centroid_lon"], metrics["centroid_lat"]),
            4326
        ),
        zoom_width=metrics["zoom_width"],
        zoom_height=metrics["zoom_height"],
        best_zoom=metrics["best_zoom"]
    )
    return db_kabupaten

def create_kecamatan(props, geom_wkb, metrics):
    db_kecamatan = locations.Kabupaten(
        kd_propinsi=props.get("kd_propinsi"),
        kd_kabupaten=props.get('kd_kabupaten'),
        kd_kecamatan=props.get('kd_kecamatan'),
        nm_kecamatan=props.get("nm_kecamatan"),
        geom=func.ST_SetSRID(func.ST_GeomFromWKB(geom_wkb), 4326),
        centroid=func.ST_SetSRID(
            func.ST_MakePoint(metrics["centroid_lon"], metrics["centroid_lat"]),
            4326
        ),
        zoom_width=metrics["zoom_width"],
        zoom_height=metrics["zoom_height"],
        best_zoom=metrics["best_zoom"]
    )
    return db_kecamatan

def create_kelurahan(props, geom_wkb, metrics):
    db_kelurahan = locations.Kabupaten(
        kd_propinsi=props.get("kd_propinsi"),
        kd_kabupaten=props.get('kd_kabupaten'),
        kd_kecamatan=props.get('kd_kecamatan'),
        kd_kelurahan=props.get('kd_kelurahan'),
        nm_kelurahan=props.get("nm_kelurahan"),
        geom=func.ST_SetSRID(func.ST_GeomFromWKB(geom_wkb), 4326),
        centroid=func.ST_SetSRID(
            func.ST_MakePoint(metrics["centroid_lon"], metrics["centroid_lat"]),
            4326
        ),
        zoom_width=metrics["zoom_width"],
        zoom_height=metrics["zoom_height"],
        best_zoom=metrics["best_zoom"]
    )
    return db_kelurahan

EARTH_CIRCUMFERENCE = 40075016.686  # meters (Web Mercator)
TILE_SIZE = 256  # pixels

def lon_to_m(lon):
    return lon * 20037508.34 / 180

def lat_to_m(lat):
    lat = max(min(lat, 89.9999), -89.9999)  # avoid infinity at poles
    rad = math.radians(lat)
    return math.log(math.tan(math.pi / 4 + rad / 2)) * 6378137.0

def get_zoom_and_centroid(geom, map_width_px=1024):
    # Normalize geometry
    if isinstance(geom, Polygon):
        geom = MultiPolygon([geom])

    # Get bounds in lon/lat
    minx, miny, maxx, maxy = geom.bounds

    # Convert to meters (Web Mercator)
    width_m = abs(lon_to_m(maxx) - lon_to_m(minx))
    height_m = abs(lat_to_m(maxy) - lat_to_m(miny))

    # Calculate zoom levels for width and height
    zoom_width = math.log2(EARTH_CIRCUMFERENCE / (width_m / map_width_px))
    zoom_height = math.log2(EARTH_CIRCUMFERENCE / (height_m / map_width_px))

    # Centroid
    centroid = geom.centroid
    lon, lat = centroid.x, centroid.y

    return {
        "centroid_lon": lon,
        "centroid_lat": lat,
        "zoom_width": round(zoom_width, 2),
        "zoom_height": round(zoom_height, 2),
        "best_zoom": round(min(zoom_width, zoom_height), 2)
    }