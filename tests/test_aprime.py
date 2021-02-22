import inspect
import os
import sys
import unittest
from threading import Event


from processflow.lib.jobstatus import JobStatus
from processflow.lib.util import print_line
from processflow.lib.initialize import initialize
from processflow.jobs.aprime import Aprime
from processflow.version import __version__, __branch__


class TestAprime(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(TestAprime, self).__init__(*args, **kwargs)
        self.config_path = 'tests/test_configs/aprime_complete.cfg'

    def test_aprime_skip_complete(self):
        """
        Checks that the aprime job successfully marks a job thats already
        been run as complete and wont get executed
        """
        print '\n'
        print_line(
            '---- Starting Test: {} ----'.format(inspect.stack()[0][3]), status='ok')

        _args = ['--test', '-c', self.config_path]
        config, _, _ = initialize(
            argv=_args,
            version=__version__,
            branch=__branch__)

        aprime = Aprime(
            short_name='testing_1pctCO2',
            case='20180215.DECKv1b_1pctCO2.ne30_oEC.edison',
            start=1,
            end=2,
            comparison='obs',
            config=config)

        self.assertTrue(aprime.postvalidate(config))

    def test_aprime_execute_dryrun(self):
        """
        test that the e3sm_diags prevalidate and prerun setup works correctly
        """
        print '\n'
        print_line(
            '---- Starting Test: {} ----'.format(inspect.stack()[0][3]), status='ok')

        _args = ['--test', '-c', self.config_path]
        config, filemanager, runmanager = initialize(
            argv=_args,
            version=__version__,
            branch=__branch__)

        self.assertFalse(config is None)
        self.assertFalse(filemanager is None)
        self.assertFalse(runmanager is None)

        config['global']['dryrun'] = True

        runmanager.check_data_ready()
        runmanager.start_ready_jobs()

        for case in runmanager.cases:
            for job in case['jobs']:
                if job.job_type == 'aprime':
                    job.setup_data(
                        config=config,
                        filemanager=filemanager,
                        case='20180215.DECKv1b_1pctCO2.ne30_oEC.edison')
                    job.execute(
                        config=config,
                        dryrun=True)
                    self.assertEquals(
                        job.status,
                        JobStatus.COMPLETED)


if __name__ == '__main__':
    unittest.main()
