# -*- coding: utf-8 -*-
"""
Useful tools.

This module collect helpful tools for nopandas. The module provide
custom light twins of popular packages like ``Pandas``.
See ``examples/table.py``.

Copyright 2020 Ilia Lazarev
Licensed under the Apache License, Version 2.0
"""

import copy


class Table:
    """Cheap alternative for 'panadas.DataFrame'."""

    __slots__ = ('_columns', '_values', '_vsep', '_hsep', '_formats')

    @staticmethod
    def outzip(*args):
        """Longest zip which fills missing value with empty string."""
        max_len = max(len(arg) for arg in args)
        extended_args = [list(arg) + [''] * (max_len-len(arg)) for arg in args]
        return zip(*extended_args)

    def __init__(self, *args, **kwargs):
        """
        Make a simple table from same length sequences.

        Args:
            same length sequences interpreted as table rows or
              columns if 'transpose=True'

        Kwargs:
            columns (list, optional): headers of columns; default number.
            transpose (bool, optional): matrix of data will be transposed
                if True; default False.
            vsep (str, optional): a char of a header split line; default '-'.
            hsep (str, optional): a char of a columns split line; default '|'.
            formats (list, optional): a format string type for each columns.

        """
        transpose = kwargs.get('transpose', False)
        if transpose:
            self._values = [list(copy.copy(arg)) for arg in self.outzip(*args)]
        else:
            self._values = [list(copy.copy(arg)) for arg in args]

        self._columns = kwargs.get(
            'columns',
            [str(i) for i in range(len(self._values))]
        )
        self._vsep = kwargs.get('vsep', '-')
        self._hsep = kwargs.get('hsep', '|')
        self._formats = kwargs.get('formats', ['s'] * len(self._columns))

    @property
    def columns(self):
        """Return name of columns."""
        return self._columns

    @property
    def values(self):
        """Return values of table by rows."""
        return self._values

    @property
    def valuest(self):
        """Return transposed values."""
        return [list(x) for x in zip(*self._values)]

    @property
    def config(self):
        """Return dict of the table config."""
        return {
            'vsep': self._vsep,
            'hsep': self._hsep,
            'formats': self._formats,
        }

    @property
    def _widths(self):
        return [
            max(len('%{}'.format(f) % val) for val in [head] + col)
            for f, head, col in zip(self._formats,
                                    self._columns,
                                    self.valuest)
        ]

    def __str__(self):
        widths = self._widths
        hline = [self._vsep * w for w in widths]
        endline = ['=' * w for w in widths]
        template = self._hsep.join(
            ' %-{w}{f} '.format(w=w, f=f)
            for w, f in zip(widths, self._formats)
        )
        data = [self._columns] + [hline] + self._values + [endline]
        return '\n'.join(template % tuple(line) for line in data)

    def __getitem__(self, key):
        if isinstance(key, (list, tuple, Column)):
            if all(isinstance(k, str) for k in key):
                _columns = [
                    (cname, copy.copy(cvalues))
                    for cname, cvalues in zip(self.columns, self.valuest)
                    if cname in key
                ]
                columns, values = zip(*_columns)
                return Table(*values,
                             columns=columns,
                             transpose=True,
                             **self.config)

            if all(isinstance(k, bool) for k in key):
                values = [copy.copy(row)
                          for k, row in zip(key, self._values)
                          if k]
                columns = self._columns
                return Table(*values, columns=columns, **self.config)

        elif isinstance(key, str):
            if key in self._columns:
                indx = self._columns.index(key)
                values = self.valuest[indx]
                return Column(values, name=key)

            raise ValueError('unknown table name: %s' % key)

        raise TypeError('unknown key type: %s' % key.__class__.__name__)


class Column:
    """Cheap alternative for 'pandas.Series'."""

    __slots__ = ('_values', '_name')

    def __init__(self, values, name=None):
        """
        Named wrapper over python list.

        Args:
            values (list, tuple): data values
            name (str, optional): name of columns

        """
        self._values = copy.copy(values)
        self._name = name

    def __str__(self):
        return str(self._values)

    @property
    def values(self):
        """Return values of column."""
        return self._values

    def __len__(self):
        return len(self._values)

    def __getitem__(self, key):
        if isinstance(key, int):
            return self._values[key]

        raise TypeError('key should be integer, not %s' %
                        key.__class__.__name__)

    def __iter__(self):
        return iter(self._values)

    def __eq__(self, key):
        values = [v == key for v in self._values]
        return Column(values)

    def __le__(self, key):
        values = [v <= key for v in self._values]
        return Column(values)

    def __lt__(self, key):
        values = [v < key for v in self._values]
        return Column(values)

    def __ge__(self, key):
        values = [v >= key for v in self._values]
        return Column(values)

    def __gt__(self, key):
        values = [v > key for v in self._values]
        return Column(values)

    def __invert__(self):
        values = [not v for v in self._values]
        return Column(values)

    def __and__(self, seq):
        if isinstance(seq, self.__class__):
            values = [v & v_ for v, v_ in zip(self.values, seq.values)]
            return Column(values)

        raise TypeError("unsupported operand type(s) for &: '%r' & %r" %
                        (self.__class__.__name__, seq.__class__.__name__))

    def isin(self, seq):
        """Whether each element in the Column is contained in values."""
        values = [v in seq for v in self._values]
        return Column(values)
