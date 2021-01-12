import itertools
import json
import os
import unittest


def data(name, mode='rb', **kwargs):
    return open(
        os.path.join(
            os.path.dirname(__file__),
            'data', name,
        ),
        mode,
        **kwargs,
    )


def _inline_jsonschema(obj, definitions, base_path):
    # Move definitions to top-level
    if 'definitions' in obj:
        for k, v in obj.pop('definitions').items():
            definitions[k] = _inline(v, definitions, base_path)
    # Drop the '$schema' key
    if '$schema' in obj:
        del obj['$schema']
    return _inline(obj, definitions, base_path)


def _inline(obj, definitions, base_path):
    if isinstance(obj, dict):
        if '$ref' in obj:
            assert len(obj) == 1
            if not obj['$ref'].startswith('#/'):
                # Reference to a file: process JSON Schema file
                assert '#' not in obj['$ref']
                with open(os.path.join(base_path, obj['$ref'])) as fp:
                    refd = json.load(fp)
                return _inline_jsonschema(refd, definitions, base_path)
            else:
                if obj['$ref'].startswith('#/definitions/'):
                    # Rewrite definitions, they have been moved to top-level
                    # 'components.schemas'
                    return {'$ref': '#/components/schemas/' + obj['$ref'][14:]}
                else:
                    # Other reference
                    assert obj['$ref'].startswith('#/components/schemas/')
                    return obj
        else:
            # Recursively process
            ret = {}
            for k, v in obj.items():
                if isinstance(k, int):
                    # Convert dict keys to strings, works around
                    # https://github.com/p1c2u/openapi-spec-validator/issues/79
                    k = str(k)
                else:
                    assert isinstance(k, str)
                ret[k] = _inline(v, definitions, base_path)
            return ret
    elif isinstance(obj, list):
        # Recursively process
        return [_inline(v, definitions, base_path) for v in obj]
    elif obj is None or isinstance(obj, (str, int, float)):
        return obj
    else:
        raise TypeError("Found value of type %r" % (type(obj),))


def inline_openapi(obj, base_path):
    """Inlines JSON Schema files referenced from an Open API specification.
    """
    try:
        definitions = dict(obj['components'].pop('schemas'))
    except KeyError:
        definitions = {}
    # Process the document (except definitions, they have been removed)
    obj = _inline(obj, definitions, base_path)
    # Process the definitions (components.schemas)
    for k, v in list(definitions.items()):
        definitions[k] = _inline(v, definitions, base_path)
    # Put the definitions back at 'components.schemas'
    if definitions:
        obj.setdefault('components', {})['schemas'] = definitions
    return obj


class DataTestCase(unittest.TestCase):
    def assertJson(self, actual, expected, pos='@'):
        if callable(expected):
            # The reason this function exists
            try:
                ret = expected(actual)
            except AssertionError as e:
                raise AssertionError(
                    "Validation failed for %r at %s" % (actual, pos)
                ) from e
            else:
                if not isinstance(ret, bool):
                    raise TypeError(
                        "Validation function: expected bool, returned %r at %s" % (
                            type(ret), pos
                        )
                    )
                if not ret:
                    raise AssertionError(
                        "Validation failed for %r at %s" % (actual, pos)
                    )
                return

        if (
            not isinstance(actual, type(expected))
            and not isinstance(expected, type(actual))
        ):
            raise AssertionError(
                "Type mismatch: expected %r, got %r at %s" % (
                    type(expected), type(actual), pos,
                )
            )
        elif isinstance(actual, list):
            if len(actual) != len(expected):
                raise AssertionError(
                    "List lengths don't match: expected %d, got %d at %s" % (
                        len(expected), len(actual), pos,
                    )
                )
            for i, (a, e) in enumerate(zip(actual, expected)):
                self.assertJson(a, e, '%s[%d]' % (pos, i))
        elif isinstance(actual, dict):
            if actual.keys() != expected.keys():
                msg = "Dict keys don't match at %s" % pos
                if len(actual) != len(expected):
                    msg += "; expected %d, got %d" % (
                        len(expected), len(actual),
                    )
                if len(actual) > len(expected):
                    unexpected = set(actual) - set(expected)
                    msg += "\nUnexpected keys: "
                else:
                    unexpected = set(expected) - set(actual)
                    msg += "\nMissing keys: "
                if len(unexpected) > 3:
                    msg += ', '.join(
                        repr(key)
                        for key in itertools.islice(unexpected, 3)
                    )
                    msg += ', ...'
                else:
                    msg += ', '.join(repr(key)
                                     for key in unexpected)
                raise AssertionError(msg)
            for k, a in actual.items():
                e = expected[k]
                self.assertJson(a, e, '%s.%r' % (pos, k))
        else:
            self.assertEqual(actual, expected, msg="at %s" % pos)

    def assertCsvEqualNoOrder(self, actual, expected_header, expected_data):
        lines = actual.splitlines(False)
        self.assertEqual(lines[0], expected_header)
        self.assertEqual(sorted(lines[1:]), sorted(expected_data))
