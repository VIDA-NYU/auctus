from dateutil.parser import parse
import re
import unittest

from datamart_core.common import Type


_re_int = re.compile(r'^[+-]?[0-9]+$')
_re_float = re.compile(r'^[+-]?'
                       r'(?:'
                       r'(?:[0-9]+\.[0-9]*)|'
                       r'(?:\.[0-9]+)'
                       r')'
                       r'(?:[Ee][0-9]+)?$')
_re_phone = re.compile(r'^'
                       r'(?:\+[0-9]{1,3})?'  # Optional country prefix
                       r'(?=(?:[() .-]*[0-9]){4,15}$)'  # 4-15 digits
                       r'(?:[ .]?\([0-9]{3}\))?'  # Area code in parens
                       r'(?:[ .]?[0-9]{1,12})'  # First group of digits
                       r'(?:[ .-][0-9]{1,10}){0,5}'  # More groups of digits
                       r'$')
_re_whitespace = re.compile(r'\s')


# Tolerable ratio of unclean data
MAX_UNCLEAN = 0.02  # 2%


# Maximum number of different values for categorical columns
MAX_CATEGORICAL = 6


def identify_types(array, name):
    num_total = len(array)
    ratio = 1.0 - MAX_UNCLEAN

    # Identify structural type
    num_float = num_int = num_bool = num_empty = num_text = 0
    for elem in array:
        if not elem:
            num_empty += 1
        elif _re_int.match(elem):
            num_int += 1
        elif _re_float.match(elem):
            num_float += 1
        elif len(_re_whitespace.findall(elem)) >= 4:
            num_text += 1
        if elem.lower() in ('0', '1', 'true', 'false', 'y', 'n', 'yes', 'no'):
            num_bool += 1

    threshold = ratio * (num_total - num_empty)

    if num_empty == num_total:
        structural_type = Type.MISSING_DATA
    elif num_empty + num_int >= threshold:
        structural_type = Type.INTEGER
    elif num_empty + num_int + num_float >= threshold:
        structural_type = Type.FLOAT
    else:
        structural_type = Type.TEXT

    semantic_types_dict = {}

    # Identify booleans
    if (num_empty + num_bool) >= threshold:
        semantic_types_dict[Type.BOOLEAN] = None

    if structural_type == Type.TEXT:
        if num_empty + num_text >= threshold:
            # Free text
            semantic_types_dict[Type.TEXT] = None
        else:
            # Count distinct values
            values = set()
            for elem in array:
                if elem not in values:
                    values.add(elem)
                    if len(values) > MAX_CATEGORICAL:
                        break
            else:
                semantic_types_dict[Type.CATEGORICAL] = values
    elif structural_type == Type.INTEGER:
        # Identify ids
        # TODO: is this enough?
        # TODO: what about false positives?
        if (name.lower().startswith('id') or
                name.lower().endswith('id') or
                name.lower().startswith('identifier') or
                name.lower().endswith('identifier') or
                name.lower().startswith('index') or
                name.lower().endswith('index')):
            semantic_types_dict[Type.ID] = None

    # Identify lat/long
    num_lat = num_long = 0
    if structural_type == Type.FLOAT:
        for elem in array:
            try:
                elem = float(elem)
            except ValueError:
                pass
            else:
                if -180.0 <= float(elem) <= 180.0:
                    num_long += 1
                    if -90.0 <= float(elem) <= 90.0:
                        num_lat += 1

        if (num_empty + num_lat) >= threshold and 'lat' in name.lower():
            semantic_types_dict[Type.LATITUDE] = None
        if (num_empty + num_long) >= threshold and 'lon' in name.lower():
            semantic_types_dict[Type.LONGITUDE] = None

    # Identify dates
    if structural_type == Type.TEXT:
        parsed_dates = []
        for elem in array:
            try:
                parsed_dates.append(parse(elem))
            except Exception:  # ValueError, OverflowError
                pass

        if (num_empty + len(parsed_dates)) >= threshold:
            semantic_types_dict[Type.DATE_TIME] = parsed_dates

    # Identify phone numbers
    num_phones = 0
    for elem in array:
        if _re_phone.match(elem) is not None:
            num_phones += 1

    if (num_empty + num_phones) >= threshold:
        semantic_types_dict[Type.PHONE_NUMBER] = None

    return structural_type, semantic_types_dict


class TestTypes(unittest.TestCase):
    def do_test(self, match, positive, negative):
        for elem in positive.splitlines():
            elem = elem.strip()
            if elem:
                self.assertTrue(match(elem),
                                "Didn't match: %s" % elem)
        for elem in negative.splitlines():
            elem = elem.strip()
            if elem:
                self.assertFalse(match(elem),
                                 "Shouldn't have matched: %s" % elem)

    def test_phone(self):
        positive = '''\
        +1 347 123 4567
        1 347 123 4567
        13471234567
        +13471234567
        +1 (347) 123 4567
        (347)123-4567
        +1.347-123-4567
        347-123-4567
        +33 6 12 34 56 78
        06 12 34 56 78
        +1.347123456
        347.123.4567
        '''
        negative = '''\
        -3471234567
        12.3
        +145
        -
        '''
        self.do_test(_re_phone.match, positive, negative)
        self.assertFalse(_re_phone.match(''))

    def test_ints(self):
        positive = '''\
        12
        0
        +478
        -17
        '''
        negative = '''\
        1.7
        7A
        ++2
        --34
        +-7
        -+18
        '''
        self.do_test(_re_int.match, positive, negative)
        self.assertFalse(_re_int.match(''))

    def test_floats(self):
        positive = '''\
        12.
        0.
        .7
        123.456
        +123.456
        +.456
        -.4
        .4e17
        -.4e17
        +8.4e17
        +8.e17
        '''
        negative = '''\
        1.7.3
        .7.3
        7.3.
        .
        -.
        +.
        7.A
        .e8
        8e17
        1.3e
        '''
        self.do_test(_re_float.match, positive, negative)
        self.assertFalse(_re_float.match(''))
