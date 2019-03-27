import inspect
import os
import sys
import unittest

from threading import Event
from shutil import rmtree

if sys.path[0] != '.':
    sys.path.insert(0, os.path.abspath('.'))

from processflow.lib.events import EventList
from processflow.lib.jobstatus import JobStatus
from processflow.lib.util import print_message
from processflow.lib.initialize import initialize, setup_directories
from processflow.jobs.amwg import AMWG
from processflow.tests.utils import mock_climos



class TestAMWG(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(TestAMWG, self).__init__(*args, **kwargs)

        self.event_list = EventList()
        self.config_path = 'processflow/tests/test_configs/amwg_complete.cfg'
        self.config, self.filemanager, self.runmanager = initialize(
            argv=['--test', '-c', self.config_path, '--dryrun'],
            version="2.2.0",
            branch="testing",
            event_list=self.event_list)
        
        self.config['data_types']['climo_regrid'] = {}
        self.config['data_types']['climo_regrid']['monthly'] = True

        if os.path.exists(self.config['global']['project_path']):
            rmtree(self.config['global']['project_path'])
        setup_directories(self.config)

        self.case_name = '20180129.DECKv1b_piControl.ne30_oEC.edison'
        self.short_name = 'piControl_testing'

    def test_amwg_setup(self):
        """
        Test the amwg initialization and data setup works correctly
        """

        print '\n'
        print_message(
            '---- Starting Test: {} ----'.format(inspect.stack()[0][3]), 'ok')        
        
        amwg = AMWG(
            short_name=self.short_name,
            case=self.case_name,
            start=1,
            end=2,
            comparison='obs',
            config=self.config)
        
        self.assertEqual(amwg._host_url, 'https://acme-viewer.llnl.gov/baldwin32/piControl_testing/amwg/0001_0002_vs_obs/index.html')
        self.assertEqual(amwg._host_path, '/var/www/acme/acme-diags/baldwin32/piControl_testing/amwg/0001_0002_vs_obs')
        self.assertEqual(amwg._output_path, '/p/user_pub/e3sm/baldwin32/testing/amwg/output/diags/piControl_testing/amwg/0001_0002_vs_obs')

    def test_amwg_prevalidate(self):
        """
        test that the amwg prevalidate fails when ncclimo hasn't been run yet
        """
        print '\n'
        print_message(
            '---- Starting Test: {} ----'.format(inspect.stack()[0][3]), 'ok')

        amwg = AMWG(
            short_name=self.short_name,
            case=self.case_name,
            start=1,
            end=2,
            comparison='obs',
            config=self.config)
        amwg.setup_data(
            config=self.config,
            filemanager=self.filemanager,
            case=self.case_name)

        self.assertFalse(amwg.prevalidate())

    def test_amwg_execution_completed_job(self):
        """
        test that when run on a completed set of jobs, amwg recognizes that the run has already
        taken place and doesnt start again
        """
        print '\n'
        print_message(
            '---- Starting Test: {} ----'.format(inspect.stack()[0][3]), 'ok')

        for case in self.runmanager.cases:
            for job in case['jobs']:
                if job.job_type == 'climo':
                    mock_climos(job._output_path, job._regrid_path)
                    job.status = JobStatus.COMPLETED

        self.runmanager.check_data_ready()
        self.runmanager.start_ready_jobs()

        for case in self.runmanager.cases:
            for job in case['jobs']:
                if job.job_type == 'amwg':
                    job.setup_data(
                        config=self.config,
                        filemanager=self.filemanager,
                        case=self.case_name)
                    job.execute(
                        config=self.config,
                        event_list=self.event_list,
                        dryrun=True)
                    self.assertEquals(
                        job.status,
                        JobStatus.COMPLETED)


if __name__ == '__main__':
    unittest.main()
