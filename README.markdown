# django-soupmigration
Easily migrate MySQL database with lots of irregularities to your Django
project.

* **Author:** [Jacob Magnusson](http://twitter.com/pyjacob)
* **Homepage:** <http://www.github.com/jmagnusson/django-soupmigration/>

## Requirements
- Python 2.7+
- Django 1.0+
- [MySQLdb-Python](http://mysql-python.sourceforge.net/) 1.2+

## Install
1. `$ git clone https://github.com/jmagnusson/django-soupmigration.git`
2. `$ cd django-soupmigration`
3. `$ python setup.py install`

## How-To
1. Create migration file and subclass `Data` and `Migration`.
2. See documentation for class attributes to provide.
3. `$ cd /path/to/djangoproject`
4. `$ python setup.py shell`
5. `>>> from myapp.soup import MyModelMigration`
6. `>>> migration = MyModelMigration()`
7. `>>> migrate.insert()`