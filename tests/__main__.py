import locale
import os
import sys
import unittest


top_level = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
start_dir = os.path.join(top_level, 'tests')
if top_level not in sys.path:
    sys.path.insert(0, top_level)


sys.path.append(start_dir)


class Program(unittest.TestProgram):
    def createTests(self):
        if self.testNames is None:
            self.test = self.testLoader.discover(
                start_dir=start_dir,
                top_level_dir=top_level,
                pattern='test_*.py')
        else:
            self.test = self.testLoader.loadTestsFromNames(self.testNames)


if __name__ == '__main__':
    # Locale
    locale.setlocale(locale.LC_ALL, '')

    prog = Program(argv=['tests'] + sys.argv[1:])
