"""
Test module for CMOR job class
"""
import inspect
import os
import sys
import unittest

from configobj import ConfigObj


from processflow.jobs.cmor import Cmor
from processflow.jobs.timeseries import Timeseries
from processflow.lib.jobstatus import JobStatus
from processflow.lib.util import print_line
from processflow.lib.filemanager import FileManager
from processflow.lib.verify_config import verify_config
from processflow.version import __version__, __branch__


class TestCmor(unittest.TestCase):
    """
    Test class for the CMOR job
    """

    def __init__(self, *args, **kwargs):
        super(TestCmor, self).__init__(*args, **kwargs)
        self.valid_config_path = config_path = os.path.join(
            os.getcwd(),
            'tests',
            'test_configs',
            'valid_config_cmor.cfg')

    def test_cmor_valid_completed(self):
        """
        tests that a valid config on a completed case will mark itself as
        already being run and not start
        """
        print_line('\n---- Starting Test: {} ----'.format(
            inspect.stack()[0][3]), status='ok')
        config = ConfigObj(self.valid_config_path)
        config['post-processing']['cmor']['variable_list'] = [
            config['post-processing']['cmor']['variable_list']]
        case_name = '20180129.DECKv1b_piControl.ne30_oEC.edison'
        case = config['simulations'][case_name]
        messages = verify_config(config)
        self.assertEqual(len(messages), 0)
        config['global']['resource_path'] = 'resources/'
        filemanager = FileManager(
            config=config)
        filemanager.populate_file_list()
        filemanager.update_local_status()

        timeseries = Timeseries(
            short_name=case['short_name'],
            case=case_name,
            start=config['simulations']['start_year'],
            end=config['simulations']['end_year'],
            config=config,
            run_type='atm')
        timeseries.check_data_ready(
            filemanager=filemanager)
        timeseries.setup_data(
            config=config,
            filemanager=filemanager,
            case=case_name)
        timeseries.execute(
            config=config)
        timeseries.handle_completion(
            filemanager=filemanager,
            config=config)

        cmor = Cmor(
            short_name=case['short_name'],
            case=case_name,
            start=config['simulations']['start_year'],
            end=config['simulations']['end_year'],
            config=config)
        cmor.check_data_ready(
            filemanager=filemanager)
        cmor.setup_data(
            config=config,
            filemanager=filemanager,
            case=case_name)
        self.assertTrue(cmor.postvalidate(config=config))
        self.assertTrue(cmor.execute(
            config=config))
        self.assertEquals(cmor.status, JobStatus.COMPLETED)
        self.assertTrue(cmor.handle_completion(
            filemanager=filemanager,
            config=config))


if __name__ == '__main__':
    unittest.main()
