import os
import sys
import shutil
import unittest
import threading
import inspect

if sys.path[0] != '.':
    sys.path.insert(0, os.path.abspath('.'))

from lib.util import print_message
from lib.util import render


class TestFileManager(unittest.TestCase):

    def test_render(self):
        print '\n'
        print_message(
            '---- Starting Test: {} ----'.format(inspect.stack()[0][3]), 'ok')
        render_target = 'tests/test_render_target.txt'
        render_reference = 'tests/test_render_reference.txt'
        render_output = 'tests/render_output.txt'
        reference = ''
        with open(render_reference, 'r') as fp:
            for line in fp.readlines():
                reference += line

        vals = {
            'a': 'a',
            'b': 'b',
            'd': 'd',
            'e': 'e'
        }
        self.assertTrue(render(vals, render_target, render_output))
        with open(render_output, 'r') as fp:
            line = fp.readline()
            self.assertTrue(line in reference)

    def test_render_bad_input_file(self):
        print '\n'
        print_message(
            '---- Starting Test: {} ----'.format(inspect.stack()[0][3]), 'ok')
        render_target = 'tests/DOES_NOT_EXIST'
        render_output = 'tests/render_output.txt'
        self.assertFalse(render({}, render_target, render_output))

    def test_render_bad_outout_file(self):
        print '\n'
        print_message(
            '---- Starting Test: {} ----'.format(inspect.stack()[0][3]), 'ok')
        render_target = 'tests/test_render_target.txt'
        render_output = '/usr/local/NO_PERMISSIONS'
        self.assertFalse(render({}, render_target, render_output))


if __name__ == '__main__':
    unittest.main()
