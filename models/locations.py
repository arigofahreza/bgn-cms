from sqlalchemy import Column, Integer, String, Numeric
from geoalchemy2 import Geometry
from database import Base

class Propinsi(Base):
    __tablename__ = "propinsi"

    id = Column(Integer, primary_key=True, index=True)
    kd_propinsi = Column(String(10), nullable=False)
    nm_propinsi = Column(String(100), nullable=False)
    # Generic Polygon/MultiPolygon
    geom = Column(Geometry(geometry_type="GEOMETRY", srid=4326), nullable=False)
    centroid = Column(Geometry(geometry_type="POINT", srid=4326), nullable=False)
    zoom_width = Column(Numeric(5, 2), nullable=True)
    zoom_height = Column(Numeric(5, 2), nullable=True)
    best_zoom = Column(Numeric(5, 2), nullable=True)

class Kabupaten(Base):
    __tablename__ = "kabupaten"

    id = Column(Integer, primary_key=True, index=True)
    kd_propinsi = Column(String(10), nullable=False)
    kd_kabupaten = Column(String(10), nullable=False)
    nm_kabupaten = Column(String(100), nullable=False)
    # Generic Polygon/MultiPolygon
    geom = Column(Geometry(geometry_type="GEOMETRY", srid=4326), nullable=False)
    centroid = Column(Geometry(geometry_type="POINT", srid=4326), nullable=False)
    zoom_width = Column(Numeric(5, 2), nullable=True)
    zoom_height = Column(Numeric(5, 2), nullable=True)
    best_zoom = Column(Numeric(5, 2), nullable=True)

class Kecamatan(Base):
    __tablename__ = "kecamatan"

    id = Column(Integer, primary_key=True, index=True)
    kd_propinsi = Column(String(10), nullable=False)
    kd_kabupaten = Column(String(10), nullable=False)
    kd_kecamatan = Column(String(10), nullable=False)
    nm_kecamatan = Column(String(100), nullable=False)
    # Generic Polygon/MultiPolygon
    geom = Column(Geometry(geometry_type="GEOMETRY", srid=4326), nullable=False)
    centroid = Column(Geometry(geometry_type="POINT", srid=4326), nullable=False)
    zoom_width = Column(Numeric(5, 2), nullable=True)
    zoom_height = Column(Numeric(5, 2), nullable=True)
    best_zoom = Column(Numeric(5, 2), nullable=True)

class Kelurahan(Base):
    __tablename__ = "kelurahan"

    id = Column(Integer, primary_key=True, index=True)
    kd_propinsi = Column(String(10), nullable=False)
    kd_kabupaten = Column(String(10), nullable=False)
    kd_kecamatan = Column(String(10), nullable=False)
    kd_kelurahan = Column(String(10), nullable=False)
    nm_kelurahan = Column(String(100), nullable=False)
    # Generic Polygon/MultiPolygon
    geom = Column(Geometry(geometry_type="GEOMETRY", srid=4326), nullable=False)
    centroid = Column(Geometry(geometry_type="POINT", srid=4326), nullable=False)
    zoom_width = Column(Numeric(5, 2), nullable=True)
    zoom_height = Column(Numeric(5, 2), nullable=True)
    best_zoom = Column(Numeric(5, 2), nullable=True)