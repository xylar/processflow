import os
import sys
import unittest
import inspect
from threading import Event
from configobj import ConfigObj

if sys.path[0] != '.':
    sys.path.insert(0, os.path.abspath('.'))

from lib.util import print_message
from lib.events import EventList
from lib.jobstatus import JobStatus
from jobs.climo import Climo
from lib.initialize import initialize

def touch(fname):
    with open(fname, 'w') as fp:
        fp.write('\n')

class TestClimo(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(TestClimo, self).__init__(*args, **kwargs)
        self.config_path = 'tests/test_configs/valid_config_simple.cfg'
        self.event_list = EventList()

    def test_climo_setup(self):
        """
        Run ncclimo setup on valid config
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

        climo = Climo(
            short_name='piControl_testing',
            case='20180129.DECKv1b_piControl.ne30_oEC.edison',
            start=1,
            end=2,
            config=config)

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

        
        _args = ['-c', self.config_path]
        config, _, _ = initialize(
            argv=_args,
            version="2.2.0",
            branch="testing",
            event_list=self.event_list,
            kill_event=Event(),
            testing=True)

        climo = Climo(
            short_name='piControl_testing',
            case='20180129.DECKv1b_piControl.ne30_oEC.edison',
            start=1,
            end=2,
            config=config)

        # make some dummy files that match the expected output
        for month in range(1, 13):
            name = '20180129.DECKv1b_piControl.ne30_oEC.edison_{month:02d}_0001{month:02d}_0002{month:02d}_climo.nc'.format(month=month)
            outpath = os.path.join(climo._output_path, name)
            touch(outpath)

            outpath = os.path.join(climo._regrid_path, name)
            touch(outpath)
        
        for season in ['ANN_000101_0002012', 'DJF_000101_000212', 'JJA_000106_000208', 'MAM_000103_000205', 'SON_000109_000211']:
            name = '20180129.DECKv1b_piControl.ne30_oEC.edison_{season}_climo.nc'.format(season=season)
            outpath = os.path.join(climo._output_path, name)
            touch(outpath)

            outpath = os.path.join(climo._regrid_path, name)
            touch(outpath)
        
        
        self.assertTrue(
            climo.postvalidate(config))
    
    def test_climo_invalid_postvalidate(self):
        """
        Test that climo.postvalidate will return false on a case that hasnt been run
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

        climo = Climo(
            short_name='piControl_testing',
            case='20180129.DECKv1b_piControl.ne30_oEC.edison',
            start=2,
            end=4,
            config=config)

        self.assertFalse(
            climo.postvalidate(config))

    def test_climo_execute_dryrun(self):
        """
        Test that ncclimo will do all proper setup in an incomplete run
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

        climo = Climo(
            short_name='piControl_testing',
            case='20180129.DECKv1b_piControl.ne30_oEC.edison',
            start=1,
            end=2,
            config=config)
        
        self.assertEqual(
            climo.status,
            JobStatus.VALID)
        climo.execute(
            config=config,
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
        
        _args = ['-c', self.config_path]
        config, _, _ = initialize(
            argv=_args,
            version="2.2.0",
            branch="testing",
            event_list=self.event_list,
            kill_event=Event(),
            testing=True)

        climo = Climo(
            short_name='piControl_testing',
            case='20180129.DECKv1b_piControl.ne30_oEC.edison',
            start=1,
            end=2,
            config=config)
        
        self.assertEqual(
            climo.status,
            JobStatus.VALID)
        climo.execute(
            config=config,
            event_list=self.event_list,
            dryrun=True)
        self.assertTrue(
            climo.postvalidate(
                config=config))

if __name__ == '__main__':
    unittest.main()
