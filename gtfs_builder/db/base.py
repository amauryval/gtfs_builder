from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.declarative import as_declarative, declared_attr

from sqlalchemy.dialects.postgresql import TSRANGE

from sqlalchemy import Column, Integer, DateTime, String
from sqlalchemy.sql import func

import re
import datetime

from collections import namedtuple


# misc
re_table_name = '([A-Z])'
Pg_table = namedtuple('pg_table', ['schema', 'name', "rows_count"])


@as_declarative()
class Base:
    _session = None


    @declared_attr
    def __tablename__(cls):
        words = re.sub(re_table_name, ' \g<1>', cls.__name__).strip()
        return words.replace(' ', '_').lower()



class BaseMixin:
    _session = None

    @classmethod
    def set_session(cls, session):
        """
        :type session: scoped_session | Session
        """
        cls._session = session

    @classmethod
    def query(cls, *args):
        """
        :rtype: Query
        """
        return cls._session.query(*args)


class CommonQueries(BaseMixin):

    @classmethod
    def first(cls):
        return cls._session.query(cls).first()

    @classmethod
    def all(cls):
        return cls._session.query(cls).all()

    @classmethod
    def infos(cls):
        return Pg_table(
            schema=cls.__table__.schema,
            name=cls.__table__.name,
            rows_count=cls._session.query(cls).count()
        )