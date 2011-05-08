django-soupmigration
====================

Author
------
- [Jacob Magnusson](http://twitter.com/pyjacob)

About
-----
Easily migrate MySQL database with lots of irregularities to your Django
project. Requires MySQLdb.

Requirements
------------
- Python 2.7+
- Django 1.0+
- [MySQLdb-Python](http://mysql-python.sourceforge.net/) 1.2+

Install
-------
* `$ cd django-soupmigration`
* `$ python setup.py install`
* Create migration file and subclass `Data` and `Migration`.
* See documentation for class attributes to provide.