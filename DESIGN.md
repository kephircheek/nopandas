# NoPandas

*Break the wall between database and you.*

**Drivers**:
- Data are storing in database;
- Welcome, fast basic analytics;

**Philosophy**: [Zen python](https://www.python.org/dev/peps/pep-0020/)

## Backstage

Once I experimented with CSV dumps of PostgreSQL relations.
I wrote some scripts with `pandas` for data visualizing.
When I got access to the database,
I wished to test my scripts on fresh data, but
I crashed of compatibility wall between the database connection and script with pandas.
I joined some pandas tables and selected columns from the result.
Exactly final columns were not large enough to break pandas, but
the joined relation was like a bomb.

Of course, the problem in me, not in pandas.
But if one will fetch only final columns from PostgreSQL
my compatibility with CVS dumps will be broken.
What i wanted is just replace `pandas.from_csv` to `pandas.from_postgresql_relation`

Sometime later I think why not?
Let `DataFrame`  will stay just a SQL query over a database
while one does not print head or does not call the `DataFrame.values` method.
Let's season this with fast access to relation names.


## Experience

```python
>>> import sqlite3
>>> conn = sqlite3.connect('path/to/file')
>>> from nopandas.sqlite import Schema
>>> schema = Schema(conn)
>>> pring(schema)
... availables relations and their columns ...
>>> print(schema.tables) # print list of relations in schema
['MyTable', ]
>>> df = schema['MyTable']
>>> df.query
'SELECT * FROM MyTable;'
>>> df.head(1)
col1 | col2
   1 |    2
>>> (df['col1'] + df['col2']).query
'SELECT a.col1 + a.col2  FROM MyTable AS a;'
```
