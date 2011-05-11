import re
import json
from django.core.exceptions import ObjectDoesNotExist
from soupmigration.utils import regex_lookups, remove_lookup_type
from django.db import settings
import MySQLdb

__all__ = ['Data', 'Migration', 'Log']


class Data(object):
    """ Load tables and their data from a MySQL database.

    Explanation of optional class attributes:

    `data_filter` should be a dictionary of keys and values to determine if
     a dictionary ("row") should be deleted from `data`. Example:
        data_filter = {
            column1: 'not_used',
            ...
        }
        Will remove all dicts in `data` where key 'column1' has the value
        'not_used'.

    `mapping` is an optional but useful attribute that is used to specify
     tables and columns to fetch. Can also be used to rename columns. To get a
     default mapping, use the method `_get_mapping_keys`. Example mapping:
            mapping = {
                'all': {
                    column1: new_column_name1,
                    ...
                },
                table1: {
                    column2: new_column_name2,
                    column3: None, # Not renamed
                },
                table2: {
                    column4: new_column_name3,
                    ...
                }
                ...
            }

    `empty_values` is a list of strings that will be replaced with an empty
     string if found in `data`.

    `data` contains all loaded data and will look something like this:
        {
            table1: [
                {
                    column_name1: data1,
                    column_name2: data2,
                    ...
                },
                ...
            ]
            table2: ...
        }


    If `mapping` is defined the class attribute `merged_data` will contains
    the merged data of the tables in `data`. The following format is used:
        [
            {
                column_name1: data1,
                column_name2: data2,
                ...
            },
            ...
        ]
    """
    empty_values = []

    def __init__(self, **kwargs):
        self.connect_kwargs = {
            'host': getattr(settings, 'OLD_DB_HOST', '') or
                            kwargs.pop('host', ''),
            'user': getattr(settings, 'OLD_DB_USER', '') or
                            kwargs.pop('user', ''),
            'passwd': getattr(settings, 'OLD_DB_PASS', '') or
                              kwargs.pop('passwd', ''),
            'db': getattr(settings, 'OLD_DB_DATABASE', '') or
                          kwargs.pop('db', ''),
            'use_unicode': True,
            'charset': 'utf8',
        }
        self.connection = MySQLdb.connect(**self.connect_kwargs)
        self.cursor = self.connection.cursor()
        self.tables = []
        self.data = {}
        self.merged_data = []
        self.log = Log()
        self.empty_values += [None, 'None', 'NULL', '0']

        # Warn on invalid keyword arguments.
        for kwarg in kwargs:
            print '"{}" is not a valid keyword argument.'.format(kwarg)

        self.load_data()
        self.clean()
        if hasattr(self, 'mapping'):
            self.merge()

    def load_table_names(self):
        """ Load all table names """
        self.cursor.execute('SHOW TABLES')
        self.tables = [table[0] for table in self.cursor.fetchall()]
        if hasattr(self, 'mapping'):
            assert isinstance(self.mapping, dict), '`mapping` must be ' \
                'a dictionary.'
            self.tables = [t for t in self.tables if t in self.mapping]

    def load_data(self):
        """ Load data from tables into `self.data` """

        if not self.tables:
            self.load_table_names()

        for table in self.tables:
            self.data[table] = []

            # Get column names
            self.cursor.execute('DESCRIBE `{}`'.format(table))
            keys = [column[0] for column in self.cursor.fetchall()]

            # Get data
            self.cursor.execute('SELECT * FROM `{}`'.format(table))
            all_data = [data for data in self.cursor.fetchall()]

            # Append data
            for row in all_data:
                assert len(keys) is len(row)
                dic = dict(zip(keys, row))

                # If we have a mapping, rename and delete keys as specified.
                if hasattr(self, 'mapping'):
                    for_all = self.mapping.pop('all', {})
                    for mapping_keys in self.mapping:
                        self.mapping[mapping_keys].update(for_all)
                    # mapping_keys = self._get_mapping_keys()
                    items_to_add = {}
                    keys_to_del = []

                    old_unique, new_unique = self.unique_field

                    # Rename unique key if new name was specified
                    if new_unique and old_unique != new_unique:
                        dic[new_unique] = dic[old_unique]
                        del dic[old_unique]

                    for key in dic:
                        if key == new_unique:
                            continue
                        new_key = self.mapping[table].get(key)
                        if new_key:
                            items_to_add[new_key] = dic[key]
                            keys_to_del.append(key)
                        if key not in self.mapping[table]:
                            dic[key] = ''
                            # keys_to_del.append(key)

                    keys_to_del.reverse()
                    for k in keys_to_del:
                        del dic[k]
                    for i in items_to_add:
                        dic[i] = items_to_add[i]

                    # Don't add if we get any matches from `data_filter`
                    if hasattr(self, 'data_filter'):
                        matches = {}
                        for key, val in self.data_filter.items():
                            if dic.get(key, '') == val:
                                matches.update({key: val})
                        if matches:
                            self.log.add(affected=dic[new_unique], msg=
                                'Found matches for data_filter({}).' \
                                'Removed.'.format(', '.join(matches.keys())),
                            )
                            continue
                self.data[table].append(dic)

            print 'Loaded table `{}`.'.format(table)

    def clean(self):
        """ Clean data.
        Convert all data to unicode strings and strip trailing / leading
        whitespace and clear values that are deemed empty by `empty_values`.
        """
        for table in self.data:
            for dic in self.data[table]:
                for key, value in dic.items():
                    if not isinstance(value, basestring):
                        value = unicode(value)
                    dic[key] = value.strip()
                    if hasattr(self, 'empty_values'):
                        if value in self.empty_values:
                            dic[key] = u''

    def _get_mapping_keys(self):
        assert getattr(self, 'mapping')
        mapping_keys = set()
        for table in self.mapping:
            for key, value in self.mapping[table].items():
                mapping_keys.add(value or key)
        return mapping_keys

    def _get_default_mapping(self, *args):
        assert self.data
        mapping = {}
        for table in self.data:
            if args and table not in args:
                continue
            if not self.data[table]:
                continue
            mapping.update({table: dict.fromkeys(self.data[table][0])})
        return mapping

    def merge(self):
        """ Merge the tables and return as a list of dicts. """
        assert self.unique_field, \
            'You need to supply `unique_field` for this function.'
        assert self.mapping, \
            'You need to supply `mapping` for this function.'

        old_unique_field, unique_field = self.unique_field

        # Set unique_field and default values to avoid KeyError exceptions
        # Also make sure that fields that are not allowed to be empty aren't
        # added.
        self.merged_data = []
        unique_items = set()
        default_dic = dict.fromkeys(self._get_mapping_keys(), '')
        for table in self.data:
            for dic in self.data[table]:
                unique_items.add(dic[unique_field])

        # Append the default data
        for value in filter(None, unique_items):
            defaults = default_dic.items()
            defaults.append([unique_field, value])
            # for key, value in defaults:
            #     if key in self.not_empty and not value:
            #         break
            self.merged_data.append(dict(defaults))

        # Add data in reverse table order so that the more important
        # tables' fields replace less important fields
        table_order = getattr(self, 'table_order', self.mapping.keys())
        for table in reversed(table_order):
            for dic in self.data[table]:
                for m_dic in self.merged_data:
                    if dic[unique_field] == m_dic[unique_field]:
                        # Only update when values value is not empty
                        update_dic = [d for d in dic.items() if d[1]]
                        m_dic.update(update_dic)
                        break
        self.merged_data = sorted(self.merged_data,
                                  key=lambda dic: dic[unique_field])


class Migration(object):
    """ The base migration class where data is processed and inserted.

    Explanation of the most important class attributes:
        `model`, the model that data is insert into.
        `data` and `m2m_data` is where the data from the original database
            is stored.
        `m2m`, a mapping of info needed for related m2m inserts:
                `field` = The field of `model` that links to the m2m model that
                          we will insert data into.
                `lookup_fields` = The m2m model's field that is used to get
                                  and / or insert data.
                `lookup_queryset` = Specify this if you wish to do an initial
                                    filter on the queryset when doing lookups.
                `bool_dict` = If specified, will return a list with the
                              corresponding key names in `self.data` that has a
                              boolean value of True.
                `key_list` = Specify the field name(s) from the original data
                             that values will be taken from.
                `get_or_create` = If set to True, creation of related data will
                                  attempted.
                `remove` = A regex string with values that will be removed
                `split` = A regex string that will be used to split data
        `unique_field` is a value from the source database that has to be
            unique for insertion to work.
        `delete_existing`, if True model will be emptied before inserting.
    """

    def __init__(self, **kwargs):
        """
        Set instance variables by subclassing Migrate.
        """
        self.model = None
        self.data = None
        self.m2m_data = []
        self.unique_field = None
        self.m2m = None
        self.delete_existing = False
        self.log = Log()

    def get_model(self, field_name):
        return self.model._meta.get_field_by_name(
            field_name)[0].related.parent_model

    def fk_to_model(self):
        """
        Takes fields that are set defined in self.fk and turns the string
        into the appropriate model object.
        """
        if not hasattr(self, 'fk'):
            return
        for fk in self.fk:
            field = fk['field']
            for dic in self.data:
                kwargs = {fk['lookup_fields'][0]: dic[field]}
                dic[field] = self.get_model(field).objects.get(**kwargs)

    def prepare_m2m(self):
        """
        Take a string that's to be turned into m2m data, clean it and move key
        to `m2m_data`.
        Always return a list.
        """
        if not isinstance(self.m2m, (set, tuple, list)) or not isinstance(
                self.m2m[0], dict):
            raise TypeError('`m2m` needs to be a list of dictionaries.')

        # Determine the keys that should be removed from `self.data`.
        # The removed keys will be added to `self.m2m_data`.
        keys_to_remove = []
        for m2m in self.m2m:
            keys_to_remove += [m2m['field']]
            if 'bool_dict' in m2m.keys():
                keys_to_remove += m2m['bool_dict'].keys()

        for i, dic in enumerate(self.data):

            # The dict to be appended to `self.m2m_data`
            m2m_dict = {self.unique_field: dic[self.unique_field]}

            for m2m in self.m2m:
                key = m2m.get('key_name') or m2m['field']

                # If `remove` regex string supplied, remove matches.
                regex = m2m.get('remove', '')
                if regex and key in dic:
                    dic[key] = re.sub(regex, '', dic.get(key, ''))

                # Split on supplied regex
                regex = m2m.get('split', '')
                if regex and key in dic:
                    dic[key] = re.split(regex, dic.get(key, ''))

                # Add key if it's not in the original data
                if key not in dic:
                    dic[key] = []

                # Return the values of the items with the key name specified
                # in m2m['key_list'].
                if m2m.get('key_list'):
                    for m2m_key in m2m['key_list']:
                        if dic[m2m_key]:
                            dic[key].append(dic[m2m_key])

                # Return the keys of the items whos values don't evaluate to
                # False.
                if m2m.get('bool_dict'):
                    for m2m_key, value in m2m['bool_dict'].items():
                        if dic[m2m_key]:
                            dic[key].append(value)

                if not isinstance(dic[key], (set, tuple, list)):
                    dic[key] = [dic[key]]

                # Remove leading / trailing whitespace and delete empty values.
                dic[key] = filter(None, [val.strip() for val in dic[key]])

                m2m_dict.update({m2m['field']: dic[key]})

            # Create a new dict without the m2m fields.
            self.data[i] = dict((k, v) for (k, v) in dic.items()
                if k not in keys_to_remove)

            # Append the related fields dict to `m2m_data`.
            self.m2m_data.append(m2m_dict)

    def item_exists(self, item, *unique):
        """
        Return True if the item exists based on the list of fields in `unique`.
        """
        assert unique, "Please specify one or more fields that " \
            "has to be unique (together)."
        uf = self.unique_field
        for dic in self.data:
            dic_values = {dic.get(f, '') for f in unique}
            item_values = {item.get(f, '') for f in unique}
            if dic_values == item_values and dic[uf] != item[uf]:
                return True
        return False

    def get_duplicates(self, *unique):
        """
        Return duplicates based on the list of fields in `unique`.
        """
        assert unique, "Please specify one or more fields that " \
            "has to be unique (together)."
        unique_values = set()
        dupes = []
        for dic in self.data:
            values = tuple([dic.get(field, '') for field in unique])
            if values in unique_values:
                dupes.append(dic[self.unique_field])
            else:
                unique_values.add(values)
        return set(tuple(dupes))

    def insert(self):
        """
        Insert non-relational data.
        """

        # Prepare m2m data if there is any
        if self.m2m and not self.m2m_data:
            self.prepare_m2m_strings()

        if self.delete_existing:
            self.model.objects.all().delete()

        valid_fields = []
        for key in self.data[0].keys():
            if key in self.model._meta.get_all_field_names():
                valid_fields += [key]
            else:
                self.log.add(msg=u'{} is not an available field.'.format(key))

        if hasattr(self, 'fk'):
            self.fk_to_model()

        for i, dic in enumerate(self.data):
            kwargs = {}
            for key in valid_fields:
                kwargs.update({key: dic[key]})
            self.model(**kwargs).save()
            print '{} inserted.'.format(dic[self.unique_field])

        if self.m2m_data:
            self.m2m_insert()

    def m2m_insert(self):
        """
        Do insert of m2m data.
        """
        for m2m in self.m2m:
            field = m2m['field']
            lookup_fields = m2m['lookup_fields']
            m2m_model = self.get_model(field)
            m2m_objs = m2m_model.objects.all()

            for dic in self.m2m_data:
                unique_id = dic[self.unique_field]
                obj = self.model.objects.get(**{self.unique_field: unique_id})
                obj_field = getattr(obj, field)

                for value in dic[field]:
                    m2m_obj = None
                    if not value and m2m.get('warn_on_empty') is True:
                        self.log.add(affected=unique_id,
                            msg='Empty value for field "{}"'.format(field))
                        continue
                    direct_hit = True
                    for lookup in lookup_fields:
                        try:
                            kwargs = regex_lookups({lookup: value})
                            m2m_obj = m2m_objs.get(**kwargs)
                            if m2m_obj:
                                break # Jump out of loop on first match
                        except ObjectDoesNotExist:
                            direct_hit = False
                    if not m2m_obj and m2m.get('get_or_create') is True:
                        m2m_obj = m2m_model(**remove_lookup_type(kwargs))
                        m2m_obj.save()
                    elif not direct_hit and m2m_obj:
                        self.log.add(
                            affected=unique_id,
                            msg=u"Couldn't obtain '{}' with preferred " \
                            "lookup method for value '{}'".format(
                                field, value),
                        )

                    # Connect the related object to 'self.model'
                    if m2m_obj:
                        obj_field.add(m2m_obj)
                    else:
                        self.log.add(
                            affected=unique_id,
                            msg=u'{}({}) does not exist.'.format(field, value),
                        )


class Log(object):
    """ Simple logger that stores each message once.
    If a new log item is added and the message already exists the affected
    item is added to the existing message.
    """

    def __init__(self):
        self.log_messages = []

    def add(self, **kwargs):
        affected = kwargs.pop('affected', None) or 'ALL'
        msg = kwargs.pop('msg')
        exception = kwargs.pop('exception', None)

        # Warn on invalid keyword arguments.
        for kwarg in kwargs:
            print '"{}" is not a valid keyword argument.'.format(kwarg)

        if not isinstance(affected, (list, set, tuple)):
            affected = [unicode(affected)]
        dic = dict(affected=list(affected), msg=msg, exceptions=[])
        if exception and exception not in dic['exceptions']:
            dic['exceptions'].append(exception)
        # Add affected item(s) if not in list
        for logitem in self.log_messages:
            if msg in logitem.values():
                for affected_item in affected:
                    if affected_item not in logitem['affected']:
                        logitem['affected'].append(affected_item)
                return
        print 'Log({})'.format(msg) # Print on new message
        self.log_messages.append(dic)

    def msg_repr(self, dic):
        return u'Log({}: {})'.format(dic['msg'], ', '.join(dic['affected']))

    def print_all(self):
        print '\n'.join([self.msg_repr(log) for log in self.log_messages])

    def save_as_json(self, output_path):
        with open(output_path, 'w') as f:
            f.write(json.dumps(self.log_messages, indent=4))
