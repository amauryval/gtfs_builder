from typing import Generator

from geospatial_lib import GeoSpatialLib
from sqlalchemy.orm import Session

from gtfs_builder.app.config import settings


def set_session() -> Session:
    sqlalchemy_database_url = settings.ADMIN_DB_URL
    session, _ = GeoSpatialLib().sqlalchemy_connection(
        db_url=sqlalchemy_database_url,
        scoped_session=True
    )
    return session


def get_session() -> Generator[Session, None, None]:
    with set_session() as session:
        yield session
        session.close()

