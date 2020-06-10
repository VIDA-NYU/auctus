import tempfile
import textwrap
import unittest

from apiserver import dataset_transforms


class TestTransforms(unittest.TestCase):
    def test_sample(self):
        table = textwrap.dedent('''\
            name,age
            James
            Linda
            John
            Jennifer
            Michael
            Maria
            William
            Susan
            David
            Lisa
            Charles
            Nancy
            Thomas
            Karen
        ''')
        with tempfile.NamedTemporaryFile('w', newline='\n') as src:
            src.write(table)
            src.flush()
            with tempfile.NamedTemporaryFile('r') as dst:
                dataset_transforms.sample(src.name, dst.name, 5)
                self.assertEqual(
                    dst.read(),
                    textwrap.dedent('''\
                        name,age
                        James
                        Linda
                        Susan
                        William
                        Michael
                    '''),
                )
