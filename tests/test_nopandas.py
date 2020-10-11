"""Functional tests."""


import unittest

import nopandas


class TestBasicSQLiteFunctionality(unittest.TestCase):
    """Test api with SQLite database."""

    def setUp(self):
        """Create connection to DB."""
        self.db = nopandas.sqlite.Connection('tests/assets/chinook.db')

    def test_list_table_names(self):
        """Get list of relations in database."""
        self.assertSetEqual(
            set(self.db.tables),
            {'albums', 'employees', 'invoices', 'playlists',
             'artists', 'genres', 'media_types', 'tracks',
             'customers', 'invoice_items', 'playlist_track'}
        )

    def test_list_column_names(self):
        """Get columns names of relation."""
        self.assertSetEqual(
            set(self.db['tracks'].columns),
            {'TrackId', 'Name', 'AlbumId',
             'MediaTypeId', 'GenreId', 'Composer',
             'Milliseconds', 'Bytes', 'UnitPrice'}
        )

    def test_calc_sum(self):
        """Try calc 'sum' of column value."""
        self.assertEqual(
            round(self.db['tracks']['UnitPrice'].sum()),
            3680.0
        )

    def test_calc_mean(self):
        """Try calc 'mean' value of column."""
        self.assertEqual(
            round(self.db['tracks']['Milliseconds'].mean()),
            393599.0
        )

    def test_query_with_filter(self):
        """Slice rows with conditions."""
        qframe = self.db['tracks']
        mean_span = int(qframe['Milliseconds'].mean())
        self.assertEqual(
            str(qframe['UnitPrice'][qframe['Milliseconds'] > mean_span].query),
            ('SELECT a.UnitPrice FROM tracks AS a'
             'WHERE a.Milliseconds > 393599;')
        )

    def test_columns_slicing(self):
        """Slice columns by names."""
        qframe = self.db['tracks']['Milliseconds', 'Bytes']
        self.assertEqual(
            qframe.iloc[0].values.tolist(),
            [343719, 11170334]
        )

    def test_rows_slicing(self):
        """Slice rows with range."""
        qframe = self.db['tracks']['Milliseconds', 'Bytes']
        self.assertEqual(
            qframe.iloc[2:3].values,
            [[230619, 3990994]]
        )

    def test_sum_integers_columns(self):
        """Try pairwise sum of columns values."""
        qframe = self.db['tracks']
        span_ms = qframe['Milliseconds']
        size_bytes = qframe['Bytes']
        self.assertEqual(
            (span_ms + size_bytes).query,
            "SELECT a.Milliseconds + a.Bytes FROM tracks AS a;"
        )
