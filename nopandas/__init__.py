"""
Wraps a database relations in Pandas DataFrame/Series.

Root module provides super classes and logic for creation of any RDBSM wrapper.
Real RDBMS inherits and defines '_Schema' class and also inherits
_OriginalTable, _Attribute, _QDataFrame, _QSeries. (see 'sqlite' module)

Copyright 2020 Ilia Lazarev
Licensed under the Apache License, Version 2.0
"""

from collections import OrderedDict
from itertools import chain, product
from contextlib import contextmanager

from .tools import Table


def _inheritor(supercls, label):
    """Return inheritor class of super class by label."""
    return next(cls for cls in supercls.__subclasses__()
                if label in [cls.ID, cls.MODULE])


class RdbmsMixin:
    """
    Keep common parameters of nopandas module.

    Names:
        ID (str) is a unique name of database, maybe equal to MODULE.
        MODULE (str) is module name of supported connection class.
            See 'connection.__class__.__module__'

    """

    ID = None
    MODULE = None

    __slots__ = ()


class _Schema(RdbmsMixin):
    """Schema of database."""

    __slots__ = ('_conn', '_alias_generator')

    @staticmethod
    def alias_generator():
        """Alias generator."""
        return chain.from_iterable(
            map(''.join, product('abcdefghijklmnopqrstuvwxyz', repeat=i+1))
            for i in range(26)
        )

    def __init__(self, conn):
        """
        Get schema of DB by connection.

        Args:
            conn (Connection): connection based on python DB-API.

        """
        self._conn = conn

    @contextmanager
    def cursor(self):
        """Get cursor."""
        cursor = self._conn.cursor()
        yield cursor
        cursor.close()

    def fetchall(self, query):
        """Return fetched rows for 'query'."""
        with self.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall()

    @property
    def conn(self):
        """Return connection object to DB."""
        return self._conn

    @property
    def tables(self):
        """Return list of table names."""
        raise NotImplementedError

    @property
    def system_tables(self):
        """Return list of system table names."""
        raise NotImplementedError

    def _cursor_description(self, query):
        """
        Return attributes info of the query by table. See 'cursor.description'.

        Columns: [TODO: add explanation]
            name
            type_code
            display_size
            internal_size
            precision
            scale
            null_ok

        """
        headers = ['name', 'type_code', 'display_size', 'internal_size',
                   'precision', 'scale', 'null_ok']
        with self.cursor() as cursor:
            return Table(*cursor.execute(query).description, columns=headers)

    def columns(self, table_name):
        """Return columns info of the table. See 'cursor.description'."""
        query = 'SELECT * FROM %s;' % table_name
        return self._cursor_description(query)

    def __getitem__(self, key):
        """Get table by name."""
        if isinstance(key, str):
            if key in self.tables or key in self.system_tables:
                ori_table = _inheritor(_OriginalTable, self.ID)(
                    name=key,
                    schema=self
                )
                return _inheritor(_QDataFrame, self.ID)(
                    source=(ori_table, None),
                    schema=self
                )

            raise ValueError('unknown table name: %r' % key)

        raise TypeError("unknown key type: %r" % key.__class__.__name__)

    def __str__(self):
        cols = [self.columns(tbl_name)['name'] for tbl_name in self.tables]
        return str(Table(*cols, columns=self.tables, transpose=True))


class Source:
    """Abstract class for sources ('FROM' statement)."""

    __slots__ = ()

    def attributes(self, alias=None):
        """Return list of available attributes."""
        raise NotImplementedError

    def __str__(self):
        """Return display name in 'FROM' statement."""
        raise NotImplementedError


class _OriginalTable(Source, RdbmsMixin):
    """
    Base source of attributes.

    Args:
        name (str): table name
        schema (Schema): schema object of database
        alias (str): alias of table

    Note:
        OriginTable always is

    """

    __slots__ = ('_name', '_schema')

    def __init__(self, name, schema):
        self._name = name
        self._schema = schema

    def __str__(self):
        """Return display name in 'FROM' statemet."""
        return self._name

    def attributes(self, alias=None):
        """Return original attribute objects of table."""
        cls = _inheritor(_Attribute, self.ID)
        return OrderedDict(
            (name, cls(name, source=(alias or self._name)))
            for name in self._schema.columns(self._name)['name']
        )


class _JoinedTable(Source, RdbmsMixin):

    __slots__ = ('_left', '_joins')

    def __init__(self, left, right, on, how=None):
        """
        Create joined source.

        Args:
            left (Source): target source.
            right (Source): object to merging.
            on (tuple of attributes): both attributes to join on.
            how ({‘left’, ‘right’, ‘outer’, ‘inner’}): default ‘inner’.

        """
        how = how or 'INNER'
        if isinstance(left, self.__class__):
            self._left = left._left
            self._joins = left._joins + [(right, how.upper(), on)]

        elif isinstance(left, self.__class__):
            raise NotImplementedError('right source should not be joined')

        else:
            self._left = left
            self._joins = [(right, how.upper(), on)]

    def attributes(self, alias=None):
        """Return full list of attribute objects of joined source."""
        return sum((source.attributes() for (source, _), _, _ in self._joins),
                   [])

    def __str__(self):

        def make(source, alias):
            return str(source) + ((" AS %s" % alias) if alias else "")

        expr = make(self._left[0], self._left[1])
        for source, how, (lattr, rattr) in self._joins:
            expr += " %s JOIN %s ON %s=%s" % (how, make(*source), lattr, rattr)
        return expr


class _Attribute(RdbmsMixin):
    """
    Attribute of database 'Source'.

    Pure/original attributes keeps name of column and data type.
    Complex attribute keeps expression tree with pure attributes.
    Expression tree keeps in Lisp style like:
        (function_object,
            first_arg_,
            ...,
            (other_fun_object,
                first_arg_for_other_fun,
                ...,
                last_arg_for_other_fun
            )
        )
    Available functions are part of class as static methods.
    Function returns display expression.
    """

    __slots__ = ('_expr', '_source')

    def __init__(self, expr, source=None):
        """
        Create expression for selection.

        Args:
            expr (str, tuple): display expression or expression tree.
            source (str): name or alias of table.

        """
        self._expr = expr
        self._source = source

    def __hash__(self):
        return hash(self._expr) ^ hash(self._source)

    def compile(self):
        """Compile expression tree."""
        fun, args = self._expr[0], self._expr[1:]
        return fun(*(("'%s'" % arg) if isinstance(arg, str) else str(arg)
                     for arg in args))

    def simplify(self):
        """Simplify expression tree."""
        raise NotImplementedError

    def _reveal_alias(self):
        """Extract matched alias from expression."""
        if isinstance(self._expr, str):
            return self._expr

        raise NotImplementedError

    def __str__(self):
        """Return display expression for 'SELECT' statement."""
        source = ("%s." % self._source) if self._source is not None else ""
        expr = self._expr if isinstance(self._expr, str) else self.compile()
        return source + expr

    @staticmethod
    def sum(arg):
        """Return display expression of 'SUM' aggregate function."""
        return "SUM(%s)" % str(arg)

    @staticmethod
    def mean(arg):
        """Return display expression of 'AVG' aggregate function."""
        return "AVG(%s)" % str(arg)

    @staticmethod
    def min(arg):
        """Return display expression of 'MIN' aggregate function."""
        return "MIN(%s)" % str(arg)

    @staticmethod
    def max(arg):
        """Return display expression of 'MAX' aggregate function."""
        return "MAX(%s)" % str(arg)

    @staticmethod
    def add(*args):
        """Return display expression of '+' operator."""
        return "(%s + %s)" % args

    @staticmethod
    def sub(*args):
        """Return display expression of '-' operator."""
        return "(%s - %s)" % args

    @staticmethod
    def gt(*args):
        """Return display expression of '>' operator."""
        return "(%s > %s)" % args

    @staticmethod
    def lt(*args):
        """Return display expression of '<' operator."""
        return "(%s < %s)" % args

    @staticmethod
    def eq(*args):
        """Return display expression of '=' operator."""
        return "(%s = %s)" % args


class Query:
    """
    Immutable Structured Query Language wrapper.

    Supported syntax:
        SELECT [distinct] [attributes] FROM [source]
        WHERE [conditional] LIMIT [] OFFSET []

    """

    __slots__ = (
        '_schema',
        '_distinct',
        '_attrs',
        '_source',
        '_where',
        '_start',
        '_stop',
    )

    def __init__(self, schema,
                 distinct=None,
                 attrs=None,
                 source=None,
                 where=None,
                 start=None,
                 stop=None):
        """
        Structured Query Language wrapper.

        Args:
            schema (_Schema): schema object of RBDMS.
            distinct (bool): set SELECT DISTINCT if True.
            attrs (list of _Attribute or str): list of selected attributes
            source (Source): source of attributes
            where (_Attribute): choose row where attribute is True.
            start (int): number of first row
            stop (int): number of last row

        Note:
            use 'copy_with' method to create same query.

        """
        self._schema = schema
        self._distinct = distinct
        self._attrs = attrs
        self._source = source
        self._where = where
        self._start = start
        self._stop = stop

    def todict(self):
        """Dump query to python dictionary."""
        slots = sum((list(getattr(cls, '__slots__', []))
                     for cls in self.__class__.__mro__),
                    [])
        return {slot.strip('_'): getattr(self, slot) for slot in slots}

    def difference(self, other):
        """Return difference of two SQL queries."""
        data = self.todict()
        difference = {}
        for key, value in other.todict().items():
            if data[key] != value:
                difference[key] = (data[key], value)

        return difference

    def copy_with(self, **kwargs):
        """Copy query with replacing some slots."""
        params = self.todict()
        params.update(kwargs)
        return self.__class__(**params)

    def _query_select(self):
        """Return display expression of attributes."""
        if self._attrs is None:
            expr = '*'

        elif isinstance(self._attrs, dict):

            def make(attr, alias):
                expr = str(attr)
                if not alias or expr.split('.')[1] == alias:
                    alias = ""
                else:
                    alias = ' AS %s' % alias
                return str(attr) + alias

            expr = ', '.join(make(attr, alias)
                             for alias, attr in self._attrs.items())

        else:
            expr = str(self._attrs)

        if self._distinct:
            return 'SELECT DISTINCT %s' % expr

        return 'SELECT %s' % expr

    def _query_from(self):
        alias = (" AS %s" % self._source[1]) if self._source[1] else ""
        return ' FROM %s' % (str(self._source[0]) + alias)

    def _query_base(self):
        """Return display part of query with SELECT and FROM."""
        if self._attrs is None:
            expr = '*'
        elif isinstance(self._attrs, dict):
            expr = self._query_attributes()
        else:
            expr = str(self._attrs)

        source = self._query_source()
        distinct = " DISTINCT" if self._distinct else ""
        return "SELECT%(distinct)s %(expr)s FROM %(source)s" % {
            "distinct": distinct,
            "expr": expr,
            "source": source
        }

    def _query_where(self):
        """Return display 'WHERE' part of query."""
        return " WHERE %s" % str(self._where) if self._where else ""

    def _query_limits(self):
        """Return display 'LIMIT/OFFSET' part of query."""
        if self._start is None and self._stop is None:
            return ""

        if self._start is not None and self._stop is not None:
            return " LIMIT %(limit)s OFFSET %(offset)s" % {
                "limit": self._stop - self._start,
                "offset": self._start
            }

        if self._stop is not None:
            return " LIMIT %s" % self._stop

        raise ValueError('bad slice: [%r, %r]' % (self._start, self._stop))

    def query(self):
        """Return display SQL query."""
        query = ''
        query += self._query_select()
        query += self._query_from()
        query += self._query_where()
        query += self._query_limits()
        return query + ';'


class _iLocIndexer:
    """Purely integer-location based indexing for selection by position."""

    __slots__ = ('_qdf',)

    def __init__(self, qdf):
        """
        Create indexer over QDataFrame.

        Args:
            qdf (QDataFrame): target query data frame.

        """
        self._qdf = qdf

    def __getitem__(self, key):
        # TODO: handle wrong indexes.
        if isinstance(key, slice):
            return self._qdf.copy_with(start=key.start, stop=key.stop)

        raise TypeError('unsupported key type: %s' % key.__class__.__name__)


class _QDataFrame(Query, Source, RdbmsMixin):
    """
    Query Data Frame is SQL query managed like Pandas DataFrame.

    Args:
        name (str): name of table.
        schema (nopandas.Schema): schema object of database.

    """

    __slots__ = ()

    def attributes(self, alias=None):
        """Return available attributes."""
        if self._attrs is None:
            return self._source[0].attributes(self._source[1])

        return self._attrs

    @property
    def iloc(self):
        """Purely integer-location based indexing for selection by position."""
        return _iLocIndexer(self)

    @property
    def values(self):
        """Return a tuples representation of the DataFrame."""
        return self._schema.fetchall(self.query())

    @property
    def columns(self):
        """Return column labels of the DataFrame."""
        return [alias for alias in self.attributes().keys()]

    @property
    def shape(self):
        """Return a tuple representing the dimensionality of the DataFrame."""
        width = len(self.columns)
        qdf = self.copy_with(attrs='COUNT(*)')
        height = qdf.values[0][0]
        return (height, width)

    def head(self, n=5):
        """Return the first n rows."""
        qdf = self.iloc[:n]
        return Table(*qdf.values, columns=qdf.columns)

    def drop_dublicates(self):
        """Return Data Frame with duplicate rows removed."""
        return self.copy_with(distinct=True)

    def __getitem__(self, key):
        if isinstance(key, (tuple, list)):
            alien_keys = list(set(key) - self.attributes().keys())
            if len(alien_keys) > 0:
                raise ValueError('unknown keys: %s' % alien_keys)

            attrs = OrderedDict((alias, attr)
                                for alias, attr in self.attributes().items()
                                if alias in key)

            return self.copy_with(attrs=attrs)

        if isinstance(key, str):
            params = self.todict()
            params['attrs'] = {key: self.attributes()[key]}
            return _inheritor(_QSeries, self.ID)(**params)

        if isinstance(key, _QSeries):
            return self.copy_with(where=list(key._attrs.values())[0])

        raise TypeError('unsupported key type: %s' % key.__class__.__name__)

    def rename(self, mapper):
        """Rename alias of attributes."""
        attrs = OrderedDict()
        for alias, attr in self.attributes().items():
            if alias in mapper:
                attrs[mapper[alias]] = attr
            else:
                attrs[alias] = attr

        return self.copy_with(attrs=attrs)

    def merge(self, right, how='inner', on=None):
        """
        Merge QDataFrame objects with a database-style join.

        Args:
            right (QDataFrame): object to join.
            how ({‘left’, ‘right’, ‘outer’, ‘inner’}): default ‘inner’.
            on (str): label of column.

        Return:
            QDataFrame: merged object

        """
        lattrs = self.attributes()
        rattrs = right.attributes()

        common_names = lattrs.keys() & rattrs.keys()
        if on and on not in common_names:
            raise ValueError('column %r is not in both frames' % on)

        if len(common_names) == 1:
            if on is None:
                on = common_names.pop()

        else:
            # TODO: rename overlap columns with suffixes
            raise ValueError('columns overlap %s' % list(common_names - {on}))

        ron = rattrs.pop(on)
        attrs = OrderedDict()
        attrs.update(lattrs)
        attrs.update(rattrs)

        joined_source = _inheritor(_JoinedTable, self.ID)(
            left=self._source,
            right=right._source,
            on=(lattrs[on], ron),
            how=how
        )
        return self.copy_with(attrs=attrs, source=(joined_source, None))

    def drop(self, columns):
        """Drop specified columns."""
        attrs = OrderedDict([(alias, attr)
                             for alias, attr in self.attributes().items()
                             if alias not in columns])
        return self.copy_with(attrs=attrs)

    def dtypes(self):
        """Return the dtypes in the DataFrame."""
        raise NotImplementedError

    def count(self):
        """Count non-Null cells for each column or row."""
        raise NotImplementedError

    def memeory_usage(self):
        """Return the memory usage of each column in bytes."""
        raise NotImplementedError

    def info(self):
        """
        Print a concise summary of a DataFrame.

        This method prints information about a DataFrame
        including the index dtype and columns, non-null values
        and memory usage.
        """
        print(Table(self.columns, self.count(), self.dtypes),
              columns=["Column", "Non-Null Count", "Dtype"])
        # TODO: set of data types and frequency
        print("memory usage: %s+ bytes" % self.memory_usage())


class _QSeries(Query, RdbmsMixin):
    """Super class for Data Frame attribute."""

    __slots__ = ()

    def function(fun):
        """Decorate attribute expression for aggregate function."""
        def wrapper(self, *args):
            alias, attr = list(self._attrs.items())[0]
            method = getattr(attr, fun.__name__.strip('_'))

            if len(args) == 1:
                other = args[0]
                if isinstance(other, _QSeries):
                    difference = set(self.difference(other)) - {'attrs'}
                    if len(difference) == 0:
                        _, other = list(other._attrs.items())[0]

                    else:
                        raise ValueError("too more difference: %s" %
                                         difference.keys())

                expr = (method, attr, other)

            elif len(args) == 0:
                expr = (method, attr)

            else:
                raise TypeError("too more arguments or unsupported method")

            result_attr = _inheritor(_Attribute, self.ID)(expr)
            return self.copy_with(attrs={alias: result_attr})

        return wrapper

    def __getitem__(self, key):
        if isinstance(key, _QSeries):
            return self.copy_with(where=list(key._attrs.values())[0])

        raise TypeError('unsupported key type: %s' % key.__class__.__name__)

    @property
    def values(self):
        """Return a tuples representation of the DataFrame."""
        rows = self._schema.fetchall(self.query())
        values = list(zip(*rows))[0]
        if len(values) == 1:
            return values[0]

        return values

    def __round__(self):
        return round(self.values)

    def __int__(self):
        return int(self.values)

    def __float__(self):
        return float(self.values)

    @function
    def sum(self):
        """Return the sum of the values for column."""

    @function
    def mean(self):
        """Return the mean of the values for column."""

    @function
    def min(self):
        """Return the minimum of the values for column."""

    @function
    def max(self):
        """Return the maximum of the values for column."""

    @function
    def __add__(self, other):
        pass

    @function
    def __sub__(self, other):
        pass

    @function
    def __gt__(self, other):
        pass

    @function
    def __lt__(self, other):
        pass

    @function
    def __eq__(self, other):
        pass
