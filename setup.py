"""NoPandas: Break the wall between database and you."""


from setuptools import setup, find_packages


setup(
    name='nopandas',
    version='0.0.1',
    author='kephircheek',
    packages=find_packages(exclude=['tests*']),
)
