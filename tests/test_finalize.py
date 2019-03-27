import inspect
import os
import sys
import threading
import unittest

from configobj import ConfigObj

from processflow.lib.initialize import initialize
from processflow.lib.finalize import finalize
from processflow.lib.events import EventList
from processflow.lib.util import print_message

if sys.path[0] != '.':
    sys.path.insert(0, os.path.abspath('.'))


class TestFinalize(unittest.TestCase):
    """
    A test class for processflows finilization methods

    These tests should be run from the main project directory
    """
    def __init__(self, *args, **kwargs):
        super(TestFinalize, self).__init__(*args, **kwargs)
        self.event_list = EventList()
        self.config_path = 'tests/test_configs/e3sm_diags_complete.cfg'
        self.config = ConfigObj(self.config_path)
        self.event_list = EventList()

    def test_finilize_complete(self):
        print '\n'; print_message('---- Starting Test: {} ----'.format(inspect.stack()[0][3]), 'ok')
        pargv = ['--test', '-c', 'tests/test_configs/test_amwg_complete.cfg']
        config, filemanager, runmanager = initialize(
            argv=pargv,
            version="2.0.0",
            branch="master",
            event_list=EventList(),
            kill_event=threading.Event())

        finalize(
            config=config,
            event_list=self.event_list,
            status=1,
            runmanager=runmanager)


if __name__ == '__main__':
    unittest.main()
