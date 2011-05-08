import re


def regex_lookups(lookup_dict):
    """
    Takes a dict of query lookups and turns [i]contains, [i]startswith and
    [i]endswith into a [i]regex version. It takes `value` and replaces
    consecutive non-alphanumerical characters with .*

    Example:
    name__istartswith='the .lookup/value' --> name_regex='the.*lookup.*value.*'
    """
    if not isinstance(lookup_dict, dict):
        raise TypeError('You need to supply a dictionary.')

    accepted = ('contains', 'startswith', 'endswith')
    regex_dict = {}

    for key in lookup_dict:
        lookup, value = key, unicode(lookup_dict[key])

        if lookup.endswith(accepted):
            field, orig_method = re.findall(r'[A-Za-z0-9]+', lookup)
            case = 'i' if orig_method.startswith('i') else ''
            lookup = '%s__%sregex' % (field, case)
            value = '.*'.join(re.findall(r'[A-Za-z0-9]+', value))

            if orig_method.endswith('startswith'):
                value = '%s.*' % value
            elif orig_method.endswith('endswith'):
                value = '.*%s' % value
            else:
                value = '.*%s.*' % value
        regex_dict.update({lookup: value})
    return regex_dict


def remove_lookup_type(lookup_dict):
    """Remove "lookup type" from queryset arguments
    E.g.
    name__istartswith='apple' --> name='apple'
    """
    return {re.sub('__.*', '', k): v for k, v in lookup_dict.iteritems()}
