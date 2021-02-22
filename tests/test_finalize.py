import inspect
import os
import sys
import threading
import unittest

from processflow.lib.initialize import initialize
from processflow.lib.finalize import finalize
from processflow.lib.util import print_line
from processflow.version import __version__, __branch__


class TestFinalize(unittest.TestCase):
    """
    A test class for processflows finilization methods

    These tests should be run from the main project directory
    """

    def __init__(self, *args, **kwargs):
        super(TestFinalize, self).__init__(*args, **kwargs)
        self.config_path = 'tests/test_configs/e3sm_diags_complete.cfg'

    def test_finilize_complete(self):
        print '\n'
        print_line(
            '---- Starting Test: {} ----'.format(inspect.stack()[0][3]), status='ok')
        pargv = ['--test', '-c', self.config_path]
        config, _, runmanager = initialize(
            argv=pargv,
            version=__version__,
            branch=__branch__)

        try:
            finalize(
                config=config,
                status=1,
                runmanager=runmanager)
        except:
            self.assertTrue(False)


if __name__ == '__main__':
    unittest.main()
