# NoPandas

## Installing
```bash
python3 setup.py install
```

## Testing
```bash
python -m unittest discover
```

## Usage
*Tested on Python 3.8.2*

Download sample SQLite Database [here](https://cdn.sqlitetutorial.net/wp-content/uploads/2018/03/chinook.zip).

- Create schema object (with SQLite connection only)
```python
>>> import sqlite3
>>> from nopandas.sqlite import Schema
>>> conn = sqlite3.connect('path/to/chinook.db')
>>> schema = Schema(conn)
>>> print(schema.tables)
['albums', 'artists', 'customers', 'employees', 'genres', 'invoices', 'invoice_items', 'media_types', 'playlists', 'playlist_track', 'tracks']
>>> print(schema.columns('tracks')['name'])
['TrackId', 'Name', 'AlbumId', 'MediaTypeId', 'GenreId', 'Composer', 'Milliseconds', 'Bytes', 'UnitPrice']
```

- Get `QDataFrame` (QDF)
```python
>>> tracks = schema['tracks']
>>> tracks.query()
'SELECT * FROM tracks;'
>>> tracks.shape
(3503, 9)
```

- Manipulate
```python
>>> songs = tracks.merge(schema['albums'])\
...               .merge(schema['artists'].rename({'Name': 'ArtistName'}),\
...                      on='ArtistId',\
...                      how='left')\
...               [['Name', 'ArtistName']]
>>> songs.query() # actualy plain line will returned, displayed string was pretified manualy
"""
SELECT
	tracks.Name, artists.Name AS ArtistName
FROM tracks
	INNER JOIN albums ON tracks.AlbumId=albums.AlbumId
	LEFT JOIN artists ON albums.ArtistId=artists.ArtistId
;
"""
>>> print(songs.head())
 Name                                    | ArtistName
 --------------------------------------- | ----------
 For Those About To Rock (We Salute You) | AC/DC
 Put The Finger On You                   | AC/DC
 Let's Get It Up                         | AC/DC
 Inject The Venom                        | AC/DC
 Snowballed                              | AC/DC
 ======================================= | ==========
>>> iron_songs = songs[songs['ArtistName'] == 'Iron Maiden']
>>> print("Percent of 'Iron Maiden' songs is",\
...       '%0.1f%%' % (100 * iron_songs.shape[0] / songs.shape[0]))
Percent of 'Iron Maiden' songs is 6.1%
```

- Overview schema
```python
>>> print(schema)
```
| albums   | artists  | customers    | employees  | genres  | invoices          | invoice_items | media_types | playlists  | playlist_track | tracks
| -------- | -------- | ------------ | ---------- | ------- | ----------------- | ------------- | ----------- | ---------- | -------------- | ------------
| AlbumId  | ArtistId | CustomerId   | EmployeeId | GenreId | InvoiceId         | InvoiceLineId | MediaTypeId | PlaylistId | PlaylistId     | TrackId
| Title    | Name     | FirstName    | LastName   | Name    | CustomerId        | InvoiceId     | Name        | Name       | TrackId        | Name
| ArtistId |          | LastName     | FirstName  |         | InvoiceDate       | TrackId       |             |            |                | AlbumId
|          |          | Company      | Title      |         | BillingAddress    | UnitPrice     |             |            |                | MediaTypeId
|          |          | Address      | ReportsTo  |         | BillingCity       | Quantity      |             |            |                | GenreId
|          |          | City         | BirthDate  |         | BillingState      |               |             |            |                | Composer
|          |          | State        | HireDate   |         | BillingCountry    |               |             |            |                | Milliseconds
|          |          | Country      | Address    |         | BillingPostalCode |               |             |            |                | Bytes
|          |          | PostalCode   | City       |         | Total             |               |             |            |                | UnitPrice
|          |          | Phone        | State      |         |                   |               |             |            |                |
|          |          | Fax          | Country    |         |                   |               |             |            |                |
|          |          | Email        | PostalCode |         |                   |               |             |            |                |
|          |          | SupportRepId | Phone      |         |                   |               |             |            |                |
|          |          |              | Fax        |         |                   |               |             |            |                |
|          |          |              | Email      |         |                   |               |             |            |                |
