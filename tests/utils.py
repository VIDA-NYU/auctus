import itertools


def assert_json(actual, expected, pos='@'):
    if callable(expected):
        # The reason this function exists
        if not expected(actual):
            raise AssertionError(
                "Validation failed for %r at %s" % (actual, pos)
            )
        return

    if type(actual) != type(expected):
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
            assert_json(a, e, '%s[%d]' % (pos, i))
    elif isinstance(actual, dict):
        if actual.keys() != expected.keys():
            msg = "Dict lengths don't match; expected %d, got %d at %s" % (
                len(expected), len(actual), pos,
            )
            if len(actual) > len(expected):
                unexpected = set(actual) - set(expected)
                msg += "\nUnexpected keys: "
            else:
                unexpected = set(expected) - set(actual)
                msg += "\nMissing keys: "
            if len(unexpected) > 3:
                msg += ', '.join(repr(key)
                                 for key in itertools.islice(unexpected, 3))
                msg += ', ...'
            else:
                msg += ', '.join(repr(key)
                                 for key in unexpected)
            raise AssertionError(msg)
        for k, a in actual.items():
            e = expected[k]
            assert_json(a, e, '%s.%r' % (pos, k))
    else:
        if actual != expected:
            raise ValueError("%r != %r at %s" % (actual, expected, pos))
