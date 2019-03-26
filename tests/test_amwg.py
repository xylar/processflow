
import os
import sys
import unittest
import shutil
import inspect

from configobj import ConfigObj
from threading import Event

if sys.path[0] != '.':
    sys.path.insert(0, os.path.abspath('.'))

from jobs.amwg import AMWG
from lib.initialize import initialize
from lib.runmanager import RunManager
from lib.filemanager import FileManager
from lib.util import print_message
from lib.jobstatus import JobStatus
from lib.events import EventList

class TestAMWGDiagnostic(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(TestAMWGDiagnostic, self).__init__(*args, **kwargs)
        self.config_path = 'tests/test_configs/amwg_complete.cfg'
        self.event_list = EventList()

    def test_amwg_setup(self):
        """
        Test the amwg initialization and data setup works correctly
        """
        print '\n'
        print_message(
            '---- Starting Test: {} ----'.format(inspect.stack()[0][3]), 'ok')
        _args = ['-c', self.config_path]
        config, _, _ = initialize(
            argv=_args,
            version="2.2.0",
            branch="testing",
            event_list=self.event_list,
            kill_event=Event(),
            testing=True)

        amwg = AMWG(
            short_name='piControl_testing',
            case='20180129.DECKv1b_piControl.ne30_oEC.edison',
            start=1,
            end=2,
            comparison='obs',
            config=config)
        
        self.assertEqual(amwg._host_url, 'https://acme-viewer.llnl.gov/baldwin32/piControl_testing/amwg/0001_0002_vs_obs/index.html')
        self.assertEqual(amwg._host_path, '/var/www/acme/acme-diags/baldwin32/piControl_testing/amwg/0001_0002_vs_obs')
        self.assertEqual(amwg._output_path, '/p/user_pub/e3sm/baldwin32/testing/amwg/output/diags/piControl_testing/amwg/0001_0002_vs_obs')

    def test_amwg_prevalidate(self):
        """
        test that the amwg prevalidate fails when ncclimo hasnt been run yet
        """
        print '\n'
        print_message(
            '---- Starting Test: {} ----'.format(inspect.stack()[0][3]), 'ok')
        _args = ['-c', self.config_path, '--dryrun']
        config, filemanager, _ = initialize(
            argv=_args,
            version="2.0.0",
            branch="testing",
            event_list=self.event_list,
            kill_event=Event(),
            testing=True)
        
        self.assertFalse(config is None)
        self.assertFalse(filemanager is None)
        config['data_types']['climo_regrid'] = {}
        config['data_types']['climo_regrid']['monthly'] = True


        amwg = AMWG(
            short_name='piControl_testing',
            case='20180129.DECKv1b_piControl.ne30_oEC.edison',
            start=1,
            end=2,
            comparison='obs',
            config=config)
        amwg.setup_data(
            config=config,
            filemanager=filemanager,
            case='20180129.DECKv1b_piControl.ne30_oEC.edison')

        self.assertFalse(amwg.prevalidate())

    def test_amwg_execution_completed_job(self):
        """
        test that when run on a completed set of jobs, amwg recognizes that the run has already
        taken place and doesnt start again
        """
        print '\n'
        print_message(
            '---- Starting Test: {} ----'.format(inspect.stack()[0][3]), 'ok')
        _args = ['-c', self.config_path, '-r', 'resources/']
        config, filemanager, runmanager = initialize(
            argv=_args,
            version="2.0.0",
            branch="testing",
            event_list=self.event_list,
            kill_event=Event(),
            testing=True)

        runmanager.check_data_ready()
        runmanager.start_ready_jobs()

        for case in runmanager.cases:
            for job in case['jobs']:
                if job.job_type == 'amwg':
                    job.setup_data(
                        config=config,
                        filemanager=filemanager,
                        case='20180129.DECKv1b_piControl.ne30_oEC.edison')
                    job.execute(
                        config=config,
                        event_list=self.event_list,
                        dryrun=True)
                    self.assertEquals(
                        job.status,
                        JobStatus.COMPLETED)


if __name__ == '__main__':
    unittest.main()
