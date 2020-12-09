import contextlib
import warnings


@contextlib.contextmanager
def ignore_warnings(*categories):
    """Context manager to ignore specific warning categories.
    """
    orig_showarning = warnings.showwarning

    def record(message, category, filename, lineno, file=None, line=None):
        if not any(issubclass(category, c) for c in categories):
            orig_showarning(message, category, filename, lineno, file, line)

    try:
        warnings.showwarning = record
        yield
    finally:
        warnings.showwarning = orig_showarning


@contextlib.contextmanager
def raise_warnings(*categories):
    orig_showarning = warnings.showwarning

    def record(message, category, filename, lineno, file=None, line=None):
        if any(issubclass(category, c) for c in categories):
            raise category(message)
        orig_showarning(message, category, filename, lineno, file, line)

    try:
        warnings.showwarning = record
        yield
    finally:
        warnings.showwarning = orig_showarning
