import inspect
import os
import sys
import unittest

from threading import Event

from processflow.jobs.timeseries import Timeseries
from processflow.lib.events import EventList
from processflow.lib.initialize import initialize
from processflow.lib.jobstatus import JobStatus
from processflow.lib.util import print_message

if sys.path[0] != '.':
    sys.path.insert(0, os.path.abspath('.'))


def touch(fname):
    tail, _ = os.path.split(fname)
    if not os.path.exists(tail):
        os.makedirs(tail)
    with open(fname, 'w') as fp:
        fp.write('\n')


class TestTimeseries(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(TestTimeseries, self).__init__(*args, **kwargs)
        self.config_path = 'tests/test_configs/valid_config_timeseries.cfg'
        self.event_list = EventList()

    def test_timeseries_setup(self):
        """
        test a valid timeseries setup
        """
        print '\n'
        print_message(
            '---- Starting Test: {} ----'.format(inspect.stack()[0][3]), 'ok')
        
        _args = ['--test', '-c', self.config_path]
        config, _, _ = initialize(
            argv=_args,
            version="2.2.0",
            branch="testing",
            event_list=self.event_list,
            kill_event=Event())
        
        ts = Timeseries(
            short_name='piControl_testing',
            case='20180129.DECKv1b_piControl.ne30_oEC.edison',
            start=1,
            end=2,
            run_type='atm',
            config=config)
        
        self.assertEqual(
            ts.status,
            JobStatus.VALID)
    
    def test_timeseries_invalid_postvalidate(self):
        print '\n'
        print_message(
            '---- Starting Test: {} ----'.format(inspect.stack()[0][3]), 'ok')
        
        _args = ['--test', '-c', self.config_path]
        config, _, _ = initialize(
            argv=_args,
            version="2.2.0",
            branch="testing",
            event_list=self.event_list,
            kill_event=Event())
        
        ts = Timeseries(
            short_name='piControl_testing',
            case='20180129.DECKv1b_piControl.ne30_oEC.edison',
            start=2,
            end=4,
            run_type='atm',
            config=config)
        
        self.assertEqual(
            ts.status,
            JobStatus.VALID)
        self.assertFalse(ts.postvalidate(
            config=config))
    
    def test_timeseries_execute_completed(self):
        print '\n'
        print_message(
            '---- Starting Test: {} ----'.format(inspect.stack()[0][3]), 'ok')
        
        _args = ['--test', '-c', self.config_path]
        config, _, _ = initialize(
            argv=_args,
            version="2.2.0",
            branch="testing",
            event_list=self.event_list,
            kill_event=Event())
        
        ts = Timeseries(
            short_name='piControl_testing',
            case='20180129.DECKv1b_piControl.ne30_oEC.edison',
            start=1,
            end=2,
            run_type='atm',
            config=config)
        
        self.assertEqual(
            ts.status,
            JobStatus.VALID)
        ts.execute(
            config=config,
            event_list=self.event_list)

        # create some dummy files for the post validator
        for var in config['post-processing']['timeseries'][ts._run_type]:
            file_name = "{var}_{start:04d}01_{end:04d}12.nc".format(
                var=var,
                start=ts.start_year,
                end=ts.end_year)
            file_path = os.path.join(ts._output_path, file_name)
            touch(file_path)
            file_path = os.path.join(ts._regrid_path, file_name)
            touch(file_path)

        self.assertTrue(
            ts.postvalidate(
                config=config))


if __name__ == '__main__':
    unittest.main()
