import inspect
import os
import sys
import unittest

from threading import Event
from shutil import rmtree

 

from processflow.lib.jobstatus import JobStatus
from processflow.lib.initialize import initialize, setup_directories
from processflow.lib.util import print_message
from processflow.lib.events import EventList
from processflow.jobs.climo import Climo
from utils import mock_climos


class TestClimo(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(TestClimo, self).__init__(*args, **kwargs)
        self.config_path = 'tests/test_configs/valid_config_simple.cfg'
        self.event_list = EventList()
        self.config, _, _ = initialize(
            argv=['--test', '-c', self.config_path],
            version="2.2.0",
            branch="testing",
            event_list=self.event_list)
        
        if os.path.exists(self.config['global']['project_path']):
            rmtree(self.config['global']['project_path'])
        setup_directories(self.config)

        self.case_name = '20180129.DECKv1b_piControl.ne30_oEC.edison'
        self.short_name = 'piControl_testing'

    def test_climo_setup(self):
        """
        Run ncclimo setup on valid config
        """
        print '\n'
        print_message(
            '---- Starting Test: {} ----'.format(inspect.stack()[0][3]), 'ok')

        climo = Climo(
            short_name=self.short_name,
            case=self.case_name,
            start=1,
            end=2,
            config=self.config)

        self.assertEqual(
            climo.status,
            JobStatus.VALID)

    def test_climo_valid_postvalidate(self):
        """
        Test that climo.postvalidate will return true on a case thats been run
        """
        print '\n'
        print_message(
            '---- Starting Test: {} ----'.format(inspect.stack()[0][3]), 'ok')

        climo = Climo(
            short_name=self.short_name,
            case=self.case_name,
            start=1,
            end=2,
            config=self.config)

        mock_climos(climo._output_path, climo._regrid_path)        

        self.assertTrue(
            climo.postvalidate(self.config))
    
    def test_climo_invalid_postvalidate(self):
        """
        Test that climo.postvalidate will return false on a case that hasnt been run
        """
        print '\n'
        print_message(
            '---- Starting Test: {} ----'.format(inspect.stack()[0][3]), 'ok')

        climo = Climo(
            short_name=self.short_name,
            case=self.case_name,
            start=2,
            end=4,
            config=self.config)

        self.assertFalse(
            climo.postvalidate(self.config))

    def test_climo_execute_dryrun(self):
        """
        Test that ncclimo will do all proper setup in an incomplete run
        """
        print '\n'
        print_message(
            '---- Starting Test: {} ----'.format(inspect.stack()[0][3]), 'ok')

        climo = Climo(
            short_name=self.short_name,
            case=self.case_name,
            start=1,
            end=2,
            config=self.config)
        
        self.assertEqual(
            climo.status,
            JobStatus.VALID)
        climo.execute(
            config=self.config,
            event_list=self.event_list,
            dryrun=True)
        self.assertEquals(
            climo.status,
            JobStatus.COMPLETED)

    def test_ncclimo_execute_completed(self):
        """
        test that if ncclimo is told to run on a project thats already completed ncclimo
        for the given yearset it will varify that the output is present and not run again
        """
        print '\n'
        print_message(
            '---- Starting Test: {} ----'.format(inspect.stack()[0][3]), 'ok')

        climo = Climo(
            short_name=self.short_name,
            case=self.case_name,
            start=1,
            end=2,
            config=self.config)
        
        mock_climos(climo._output_path, climo._regrid_path)
        
        self.assertEqual(
            climo.status,
            JobStatus.VALID)
        climo.execute(
            config=self.config,
            event_list=self.event_list,
            dryrun=True)
        self.assertTrue(
            climo.postvalidate(
                config=self.config))


if __name__ == '__main__':
    unittest.main()
