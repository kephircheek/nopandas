"""
SQLite connection wrapper.

SQLite is a C-language library that implements a small, fast,
self-contained, high-reliability, full-featured, SQL database engine.
SQLite is the most used database engine in the world.

Copyright 2020 Ilia Lazarev
Licensed under the Apache License, Version 2.0
"""

from . import (
    _Schema,
    _OriginalTable,
    _JoinedTable,
    _Attribute,
    _QDataFrame,
    _QSeries
)
from .tools import Table


__all__ = [
    'Schema',
    'QDataFrame',
]


class SQLite:
    """Parameters of 'sqlite' nopandas module."""

    ID = 'sqlite'
    MODULE = 'sqlite3'

    __slots__ = ()


class Schema(_Schema, SQLite):
    """
    Schema of SQLite DB.

    TODO: Description

    """

    __slots__ = ()

    @property
    def tables(self):
        """Return list of table names."""
        return self._master[
            (self._master['type'] == 'table')
            & ~(self._master['name'].isin(self.system_tables))
        ]['name']

    @property
    def system_tables(self):
        """
        Return list of system table names.

        Links:
            See https://www.techonthenet.com/sqlite/sys_tables/index.php
        """
        return ['sqlite_master', 'sqlite_sequence', 'sqlite_stat1']

    @property
    def _master(self):
        """
        Return system table 'sqlite_master'.

        Master listing of all database objects in the database and
        the SQL used to create each object.

        Columns:
        type - Type of database object such as table, index, trigger or view.
        name - Name of the database object.
        tbl_name - Table name that the database object is associated with.
        rootpage - Root page.
        sql - SQL used to create the database object.

        """
        columns = ["type", "name", "tbl_name", "rootpage", "sql"]
        query = "SELECT %s FROM sqlite_master;" % ', '.join(columns)
        return Table(*self.fetchall(query), columns=columns)

    @property
    def _sequence(self):
        """
        Return system table 'sqlite_sequence'.

        Lists the last sequence number used for the AUTOINCREMENT column
        in a table.

        The sqlite_sequence table will only be created once an AUTOINCREMENT
        column has been defined in the database and at least one sequence
        number value has been generated and used in the database.
        """
        raise NotImplementedError

    @property
    def _stat1(self):
        """
        Return system table 'sqlite_stat1'.

        This table is created by the ANALYZE command to store statistical
        information about the tables and indexes analyzed. This information
        will be later used by the query optimizer.
        """
        raise NotImplementedError


class OriginalTable(_OriginalTable, SQLite):
    """Base source of attributes."""

    __slots__ = ()


class JoinedTable(_JoinedTable, SQLite):

    __slots__ = ()


class Attribute(_Attribute, SQLite):
    """Attribute of database 'Source'."""

    __slots__ = ()


class QDataFrame(_QDataFrame, SQLite):
    """SQLite query for DataFrame."""

    __slots__ = ()


class QSeries(_QSeries, SQLite):
    """SQLite query for attribute."""

    __slots__ = ()
