import re
import json
from django.core.exceptions import ObjectDoesNotExist, MultipleObjectsReturned
from django.db import connection, IntegrityError
from django.db.models import Model
from soupmigration.utils import regex_lookups, remove_lookup_type
from django.db import settings
import MySQLdb

__all__ = ['Data', 'Migration', 'Log']


class Data(object):
    """ Load tables and their data from a MySQL database.

    Explanation of optional class attributes:

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
    unique_field = ('id', '__UNIQUE_FIELD__')

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
            print u'"{}" is not a valid keyword argument.'.format(kwarg)

        if hasattr(self, 'mapping'):
            if 'all' in self.mapping:
                self.mapping['all'].update([self.unique_field])
            else:
                self.mapping['all'] = dict([self.unique_field])

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

                    keys_to_del.reverse()
                    for k in keys_to_del:
                        del dic[k]
                    for i in items_to_add:
                        dic[i] = items_to_add[i]

                self.data[table].append(dic)

            print u'Loaded table `{}`.'.format(table)

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
            self.merged_data.append(dict(defaults))

        # Add data in reverse table order so that the more important
        # tables' fields replace less important fields
        table_order = getattr(self, 'table_order', self.mapping.keys())
        for table in reversed(table_order):
            for dic in self.data[table]:
                for m_dic in self.merged_data:
                    if dic[unique_field] == m_dic[unique_field]:
                        # Only update when value is not empty
                        update_dic = [d for d in dic.items() if d[1].strip()]
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
    unique_field = 'id'

    def __init__(self, **kwargs):
        """
        Set instance variables by subclassing Migrate.
        """
        self.model = None
        self.data = []
        self.deleted_data = []
        self.rel = []
        self.delete_existing = False
        self.log = Log()
        self.get_or_create = False
        self.instances_prepared = False
        self.m2m_prepared = False

    def get_rel_model(self, field_name):
        """ Get related model """
        fn = field_name.replace('_set', '')
        field = self.model._meta.get_field_by_name(fn)[0]
        try:
            return field.related.parent_model # M2M
        except AttributeError:
            return field.model # Foreign key
    
    def get_rel_obj_field_name(self, rel_obj):
        fields = [field for field in rel_obj._meta.fields \
            if getattr(field.rel, 'to', None) == self.model]
        if fields:
            return fields[0].name

    def valid_fields(self):
        """ Get a list of all valid fields for the model. """
        fields = []
        for key in self.data[0].keys():
            if key in self.model._meta.get_all_field_names():
                fields += [key]
            else:
                self.log.add(msg=u'{} is not an available field.'.format(key))
        return fields

    def required_fields(self, model=None):
        """ Get a list of all required fields. """
        if not model:
            model = self.model
        required = set()
        for field, m in model._meta.get_fields_with_model():
            name = getattr(field, 'name')
            if name in ('id', 'slug'):
                continue
            if getattr(field, 'blank', False) or field.has_default():
                continue
            required.add(name)
        return required

    def empty_required_fields(self, kwargs):
        """ Return True if all required fields are in kwargs' keys. """
        empties = []
        for key, val in kwargs.items():
            if key in self.required_fields() and not val:
                empties.append(key)
        return empties

    def delete_if_all_empty(self, *fields):
        """ Delete item if all supplied fields are empty on it. """
        assert isinstance(fields, (set, list, tuple)), 'fields must be an ' \
            'iterable'
        assert fields, 'You must specify a set of fields'

        items_to_del = []
        for i, dic in enumerate(self.data):
            delete = True
            for key, value in dic.items():
                if key in fields and value.strip():
                    delete = False
                    break
            if delete:
                self.deleted_data.append(dic)
                items_to_del.append(i)
        for i in reversed(items_to_del):
            self.log.add(affected=self.data[i][self.unique_field],
                msg=u"Deleted item from list as the following fields were " \
                    "empty: {}".format(', '.join(fields)),
            )
            del self.data[i]

    def get_rel_values(self, key):
        """ Return all values of the specified key from the `rel` mapping. """
        rels = set()
        rel = getattr(self, 'rel', [])
        for dic in rel:
            if dic.get(key):
                rels.add(dic[key])
        return rels

    def get_m2m_fields(self):
        """ Return all fields in the `rel` mapping that have m2m = True. """
        m2ms = set()
        rel = getattr(self, 'rel', [])
        for dic in rel:
            if dic.get('m2m') is True:
                m2ms.add(dic['field'])
        return m2ms

    def insert_after_fields(self):
        """ Return a list of fields to be inserted after all other fields. """
        afters = []
        rel = getattr(self, 'rel', [])
        for field in rel:
            if field.get('insert_after') is True:
                afters.append(field['field'])
        return afters

    def sub_text(self, **filterset):
        """ Substitutes text from fields based on the supplied regex strings.
        If value is a basestring, text will be removed, otherwise an 2-tuple
        with (regex, repl) is assumed.
        Example:
            filterset = {'name': r'[john|doe]', city: ('NYC', 'New York')}
            Any occurrences of 'john' or 'doe' in field `name` will be removed.
            Any occurrences of 'NYC' in field `city` will be replaced with
            'New York'.
        """
        assert filterset, 'You need to supply a set of filters'

        for dic in self.data:
            for field, regex in filterset.items():
                repl = ''
                if isinstance(regex, (list, set, tuple)):
                    assert len(regex) is 2, 'Please supply 2-tuple ' \
                        '(regex, repl) only as value.'
                    regex, repl = regex
                if isinstance(dic[field], basestring):
                    dic[field] = [dic[field]]
                values = []
                for val in dic[field]:
                    values.append(re.sub(regex, repl, val))
                dic[field] = values[0] if len(dic[field]) is 1 else values

                self.log.add(
                    msg=u'Cleaned {} using u"{}"'.format(field, regex))

    def filter_data(self, **filterset):
        """ Remove items if their values are found. Case insensitive. """
        assert filterset, 'You need to supply a set of filters'
        items_to_del = []
        for i, dic in enumerate(self.data):
            for key, values in filterset.items():
                if isinstance(values, basestring):
                    values = [values]
                for val in values:
                    if dic[key].strip().lower() == val.lower():
                        self.log.add(affected=dic[self.unique_field],
                            msg=u'Filter match: {}="{}"'.format(key, val))
                        items_to_del.append(i)
                        continue
        for i in reversed(items_to_del):
            del self.data[i]

    def item_exists(self, item, *unique):
        """ Find out whether an item in `data` exists
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
        """ Return duplicates based on the list of fields in `unique`. """
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

    def prep_model_instances(self, **kwargs):
        """ String > Model
        Takes fields that are set defined in self.rel and turns the string(s)
        into the appropriate model object(s).
        """
        assert hasattr(self, 'rel'), 'You need to supply `rel` for this method'
        if kwargs.get('after') is True:
            rels = [rel for rel in self.rel if rel.get('insert_after') is True]
        else:
            rels = [rel for rel in self.rel if not rel.get('insert_after')]
        for rel in rels:
            field = rel['field']
            lookup_fields = rel.get('lookup_fields', [])
            rel_model = self.get_rel_model(field)
            rel_objs = rel_model.objects.all()
            extra_kwargs = rel.get('extra_kwargs', {})

            for dic in self.data:
                unique_id = dic[self.unique_field]
                values = dic[field]
                dic[field] = []
                objs_to_add = []

                obj = None
                if rel.get('with_self'):
                    # Implies insert_after
                    try:
                        obj_field = self.get_rel_obj_field_name(rel_model)
                        obj = self.model.objects.get(
                            **{self.unique_field: unique_id})
                        extra_kwargs.update({obj_field: obj})
                    except ObjectDoesNotExist:
                        pass
                    

                if isinstance(values, basestring):
                    values = [values]
                if isinstance(values, Model):
                    continue

                for value in values:
                    if isinstance(value, Model):
                        break
                    if not value:
                        self.log.add(msg=u'Empty value on "{}".'.format(field),
                            affected=unique_id)
                        continue
                    rel_obj = None
                    for lookup in lookup_fields:
                        kwargs = regex_lookups({lookup: value})
                        kwargs.update(extra_kwargs)
                        try:
                            rel_obj = rel_objs.get(**kwargs)
                            break # Jump out of loop on first match
                        except ObjectDoesNotExist:
                            pass
                        except MultipleObjectsReturned as e:
                            rel_obj = rel_objs.filter(**kwargs)[0:1].get()
                            self.log.add(
                                affected=unique_id, exception=e,
                                msg=u"Got {1} on '{0}'.".format(field,
                                    e.__class__.__name__),
                            )
                            if rel_obj:
                                break
                    if not rel_obj and rel.get('get_or_create') is True:
                        rel_obj = rel_model(**remove_lookup_type(kwargs))
                        rel_obj.save()
                    if rel_obj:
                        objs_to_add.append(rel_obj)
                    else:
                        self.log.add(
                            msg=u"Couldn't turn value {} into '{}' instance." \
                                 .format(value, rel_model._meta.module_name),
                            affected=unique_id,
                        )

                if objs_to_add:
                    dic[field] = []
                    for o in objs_to_add:
                        if isinstance(o, Model):
                            dic[field].append(o)
                    if not rel.get('m2m') is True:
                        dic[field] = dic[field][0]
                else:
                    dic[field] = None

        self.instances_prepared = True

    def prep_m2m(self):
        """ Turn values into list. Split on regex if supplied. """

        assert self.rel, 'You need to supply `self.rel` to run this method.'
        if not isinstance(self.rel, (set, tuple, list)) or not isinstance(
                self.rel[0], dict):
            raise TypeError('`m2m` needs to be a list of dictionaries.')

        for dic in self.data:

            for m2m in self.rel:
                if not m2m.get('m2m') is True:
                    continue

                key = m2m.get('key_name') or m2m['field']
                new_key = m2m['field']

                # Split on supplied regex
                dic[new_key] = re.split(m2m.get('split', ''), dic.get(key, ''))

                # Return the values of the items with the key name specified
                # in m2m['key_list'].
                if m2m.get('key_list'):
                    for m2m_key in m2m['key_list']:
                        if dic[m2m_key]:
                            dic[new_key].append(dic[m2m_key].strip())

                # Return the keys of the items whos values don't evaluate to
                # False.
                if m2m.get('bool_dict'):
                    for m2m_key, value in m2m['bool_dict'].items():
                        if dic[m2m_key]:
                            dic[new_key].append(value.strip())

                # Remove leading / trailing whitespace and delete empty values.
                dic[new_key] = [val.strip() for val in dic[new_key]]
                dic[new_key] = filter(
                    None, [val.strip() for val in dic[new_key]],
                )

        self.m2m_prepared = True

    def insert(self, **kwargs):
        """ Do the actual inserting. """
        after = kwargs.get('after', False)

        # Prepare m2m data if it hasn't been already
        if not self.m2m_prepared:
            self.prep_m2m()

        if not self.instances_prepared:
            self.model.objects.all().delete()
            self.prep_model_instances()

        insert_after_fields = set(self.insert_after_fields())
        m2m_fields = set(self.get_m2m_fields()) - insert_after_fields
        fields = set(self.valid_fields()) - insert_after_fields - m2m_fields

        if after is True:
            m2m_fields = insert_after_fields - set(self.get_m2m_fields())
            fields = insert_after_fields - m2m_fields

        for dic in self.data:
            unique_id = dic[self.unique_field]
            obj_kwargs = {}
            m2m_kwargs = {}
            obj = None
            for field in fields:
                # if not dic[field]:
                #     continue # Don't add if empty
                obj_kwargs.update({field: dic[field]})
            for field in m2m_fields:
                m2m_kwargs.update({field: dic[field]})
            # all_kwargs = m2m_kwargs.copy()
            # all_kwargs.update(obj_kwargs)
            # empty_requireds = self.empty_required_fields(all_kwargs)
            # if empty_requireds:
            #     self.log.add(msg=u'Required fields "{}" empty.'.format(
            #         ', '.join(empty_requireds)),
            #         affected=unique_id,
            #     )

            # Save instance (or update if `after` is True)
            try:
                if after is True:
                    obj = self.model.objects.get(**{self.unique_field: unique_id})
                    for key, val in obj_kwargs.items():
                        if val:
                            setattr(obj, key, val)
                    obj.save()
                elif self.get_or_create is True:
                    obj = self.model.objects.get_or_create(**obj_kwargs)[0]
                else:
                    obj = self.model(**obj_kwargs)
                    obj.save()
            except (IntegrityError, ValueError) as e:
                # Required to clear PostgreSQL's failed transaction.
                connection.close()
                self.log.add(msg=e.message, affected=[unique_id])
                continue

            # Save m2m rels
            for m2m_field, m2m_objs in m2m_kwargs.items():
                field = getattr(obj, m2m_field)
                if not m2m_objs:
                    continue
                for m2m_obj in m2m_objs:
                    if not isinstance(m2m_obj, Model):
                        continue
                    field.add(m2m_obj)

            if obj:
                print u'{} inserted (and {} m2m relations).'.format(
                    unique_id, len(m2m_kwargs.values()),
                )

        if self.insert_after_fields() and not after:
            print u'Inserting rel fields that have insert_after = True'
            self.prep_model_instances(after=True)
            self.insert(after=True)


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
            print u'"{}" is not a valid keyword argument.'.format(kwarg)

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
        print u'Log({})'.format(msg) # Print on new message
        self.log_messages.append(dic)

    def msg_repr(self, dic):
        return u'Log({}: {})'.format(dic['msg'], ', '.join(dic['affected']))

    def print_all(self):
        print u'\n'.join([self.msg_repr(log) for log in self.log_messages])

    def save_as_json(self, output_path):
        with open(output_path, 'w') as f:
            f.write(json.dumps(self.log_messages, indent=4))
