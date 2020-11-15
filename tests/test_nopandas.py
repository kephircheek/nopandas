"""Functional tests."""

import unittest


class TestBacisFunctionality:
    """
    General test case for any database base on SQLite Sample Database.

    Specific test cases should inherit it and define 'setUp' method
    with schema initialization.
    """

    def test_list_table_names(self):
        """Get list of relations in database."""
        self.assertSetEqual(
            set(self.schema.tables),
            {'albums', 'employees', 'invoices', 'playlists',
             'artists', 'genres', 'media_types', 'tracks',
             'customers', 'invoice_items', 'playlist_track'}
        )

    def test_list_column_names(self):
        """Get columns names of relation."""
        self.assertSetEqual(
            set(self.schema['tracks'].columns),
            {'TrackId', 'Name', 'AlbumId',
             'MediaTypeId', 'GenreId', 'Composer',
             'Milliseconds', 'Bytes', 'UnitPrice'}
        )

    def test_calc_shape(self):
        """Calculate shape of table."""
        self.assertEqual(self.schema['albums'].shape, (347, 3))

    def test_distinct_select(self):
        """Try select unique rows."""
        self.assertEqual(
            self.schema['albums'].drop_dublicates().query(),
            "SELECT DISTINCT * FROM albums;"
        )

    def test_attributes_alias_changing(self):
        """Try rename columns."""
        mapper = {'AlbumId': 'id', 'Title': 'name'}
        self.assertEqual(
            self.schema['albums'].rename(mapper).columns,
            ['id', 'name', 'ArtistId']
        )

    def test_calc_sum(self):
        """Try calc 'sum' of column value."""
        self.assertEqual(
            int(self.schema['tracks']['UnitPrice'].sum()),
            3680
        )

    def test_calc_mean(self):
        """Try calc 'mean' value of column."""
        self.assertEqual(
            int(self.schema['tracks']['Milliseconds'].mean()),
            393599
        )

    def test_query_with_filter(self):
        """Slice rows with conditions."""
        qframe = self.schema['tracks']
        mean = int(qframe['Milliseconds'].mean())
        self.assertEqual(
            str(qframe['UnitPrice'][qframe['Milliseconds'] > mean].query()),
            ('SELECT tracks.UnitPrice FROM tracks'
             ' WHERE (tracks.Milliseconds > 393599);')
        )

    def test_columns_slicing(self):
        """Slice columns by names."""
        qframe = self.schema['tracks']['Milliseconds', 'Bytes']
        self.assertEqual(
            qframe.iloc[:1].values,
            [(343719, 11170334)]
        )

    def test_rows_slicing(self):
        """Slice rows with range."""
        self.assertEqual(
            self.schema['albums'].iloc[2:4].values,
            [(3, 'Restless and Wild', 2),
             (4, 'Let There Be Rock', 1)]
        )

    def test_sum_integers_columns(self):
        """Try pairwise sum of columns values."""
        qframe = self.schema['tracks']
        span_ms = qframe['Milliseconds']
        size_bytes = qframe['Bytes']
        self.assertEqual(
            (span_ms + size_bytes).query(),
            ("SELECT (tracks.Milliseconds + tracks.Bytes) AS Milliseconds "
             "FROM tracks;")
        )

    def test_query_base(self):
        """Basic selection from table."""
        self.assertEqual(
            self.schema['albums'].query(),
            "SELECT * FROM albums;"
        )

    def test_dataframe_shape(self):
        """Try calculate shape of QueryDataFrame."""
        self.assertEqual(
            self.schema['albums'].shape,
            (347, 3)
        )

    def test_drop_columns(self):
        """Try drop some columns by name from selection."""

    def test_regular_join(self):
        """Try join original tables."""
        self.assertListEqual(
            self.schema['albums'].merge(self.schema['artists']).columns,
            ['AlbumId', 'Title', 'ArtistId', 'Name']
        )

    def test_multiple_join(self):
        """Try join multiple original tables."""
        tracks = self.schema['tracks']
        tracks = tracks.rename({'Name': 'track', 'Milliseconds': 'span'})
        tracks = tracks[['AlbumId', 'track', 'span']]
        albums = self.schema['albums'].rename({'Title': 'album'})
        artists = self.schema['artists'].rename({'Name': 'artist'})
        mtracks = tracks.merge(albums).merge(artists)
        mtracks = mtracks.drop(columns=['AlbumId', 'ArtistId'])
        self.assertListEqual(mtracks.columns,
                             ['track', 'span', 'album', 'artist'])


class TestBasicSQLiteFunctionality(unittest.TestCase, TestBacisFunctionality):
    """Test api with SQLite database."""

    PATH_TO_SQLITE_DUMP = 'tests/assets/chinook.db'

    def setUp(self):
        """Create connection to DB."""
        import sqlite3
        from nopandas.sqlite import Schema

        conn = sqlite3.connect(self.PATH_TO_SQLITE_DUMP)
        self.schema = Schema(conn)
