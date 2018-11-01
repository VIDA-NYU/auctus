from dateutil.parser import parse
import re


_re_int = re.compile(r'^-?[0-9]+$')
_re_float = re.compile(r'^-?(?:[0-9]+\.[0-9]*)|(\.[0-9]+)[Ee][0-9]+$')
_re_phone = re.compile(r'^'
                       r'(?:\+[0-9]{1,3})?'  # Optional country prefix
                       r'(?=(?:[() .-]*[0-9]){4,15}$)'  # 4-15 digits
                       r'(?:[ .]?\([0-9]{3}\))?'  # Area code in parens
                       r'(?:[ .]?[0-9]{1,12})'  # First group of digits
                       r'(?:[ .-][0-9]{1,10}){0,5}'  # More groups of digits
                       r'$')


# Tolerable ratio of unclean data
MAX_UNCLEAN = 0.02  # 2%


def identify_types(array):
    num_total = len(array)
    ratio = 1.0 - MAX_UNCLEAN

    # Identify structural type
    num_float = num_int = num_bool = num_empty = 0
    for elem in array:
        if not elem:
            num_empty += 1
        elif _re_int.match(elem):
            num_int += 1
        elif _re_float.match(elem):
            num_float += 1
        if elem.lower() in ('0', '1', 'true', 'false'):
            num_bool += 1

    if num_empty == num_total:
        structural_type = ('https://metadata.datadrivendiscovery.org/types/' +
                           'MissingData')
    elif (num_empty + num_int) >= ratio * num_total:
        structural_type = 'http://schema.org/Integer'
    elif (num_empty + num_int + num_float) >= ratio * num_total:
        structural_type = 'http://schema.org/Float'
    else:
        structural_type = 'http://schema.org/Text'

    semantic_types_dict = {}

    # Identify booleans
    if num_bool >= ratio * num_total:
        semantic_types_dict['http://schema.org/Boolean'] = None

    # Identify dates
    if structural_type == 'http://schema.org/Text':
        parsed_dates = []
        for elem in array:
            try:
                parsed_dates.append(parse(elem))
            except ValueError:
                pass

        if len(parsed_dates) >= ratio * num_total:
            semantic_types_dict['http://schema.org/DateTime'] = parsed_dates

    # Identify phone numbers
    num_phones = 0
    for elem in array:
        if _re_phone.match(elem) is not None:
            num_phones += 1

    if num_phones >= ratio * num_total:
        semantic_types_dict['https://metadata.datadrivendiscovery.org/types/' +
                            'PhoneNumber'] = None

    return structural_type, semantic_types_dict
