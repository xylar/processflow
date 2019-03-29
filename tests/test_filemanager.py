import inspect
import os
import sys
import threading
import unittest

from configobj import ConfigObj

 

from processflow.lib.filemanager import FileManager
from processflow.lib.events import EventList
from processflow.lib.util import print_message
from processflow.lib.initialize import initialize


class TestFileManager(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(TestFileManager, self).__init__(*args, **kwargs)
        self.file_types = ['atm', 'ice', 'ocn', 'rest', 'streams.ocean', 'streams.cice', 'mpas-o_in', 'mpas-cice_in', 'meridionalHeatTransport', 'lnd']
        self.local_path = '/p/user_pub/e3sm/baldwin32/E3SM_test_data/DECKv1b_1pctCO2_complete'
        self.remote_endpoint = '9d6d994a-6d04-11e5-ba46-22000b92c6ec'
        self.remote_path = '/global/cscratch1/sd/golaz/ACME_simulations/20180215.DECKv1b_1pctCO2.ne30_oEC.edison'
        self.local_endpoint = 'a871c6de-2acd-11e7-bc7c-22000b9a448b'
        self.experiment = '20180215.DECKv1b_1pctCO2.ne30_oEC.edison'

    def test_filemanager_setup_valid_from_scratch(self):
        """
        run filemansger setup from scratch
        """

        print '\n'; print_message('---- Starting Test: {} ----'.format(inspect.stack()[0][3]), 'ok')
        db = '{}.db'.format(inspect.stack()[0][3])
        config_path = 'processflow/tests/test_configs/valid_config_from_scratch.cfg'
        config = ConfigObj(config_path)
        experiment = '20170926.FCT2.A_WCYCL1850S.ne30_oECv3.anvil'

        filemanager = FileManager(
            database=db,
            event_list=EventList(),
            config=config)

        self.assertTrue(isinstance(filemanager, FileManager))
        self.assertTrue(os.path.exists(db))
        os.remove(db)


    def test_filemanager_setup_valid_with_inplace_data(self):
        """
        run the filemanager setup with sta turned on
        """
        print '\n'; print_message('---- Starting Test: {} ----'.format(inspect.stack()[0][3]), 'ok')
        config_path = 'processflow/tests/test_configs/e3sm_diags_complete.cfg'
        config = ConfigObj(config_path)
        db = '{}.db'.format(inspect.stack()[0][3])

        filemanager = FileManager(
            database=db,
            event_list=EventList(),
            config=config)
        filemanager.populate_file_list()
        filemanager.update_local_status()

        self.assertTrue(isinstance(filemanager, FileManager))
        self.assertTrue(os.path.exists(db))
        self.assertTrue(filemanager.all_data_local())
        os.remove(db)
    
    def test_filemanager_get_file_paths(self):
        """
        run the filemanager setup with short term archive turned on
        """
        print '\n'; print_message('---- Starting Test: {} ----'.format(inspect.stack()[0][3]), 'ok')
        config_path = 'processflow/tests/test_configs/filemanager_partial_data.cfg'
        db = '{}.db'.format(inspect.stack()[0][3])

        pargv = ['--test', '-c', config_path]
        config, _, _ = initialize(
            argv=pargv,
            version='0.0.0',
            branch='__testing__',
            event_list=EventList())

        filemanager = FileManager(
            database=db,
            event_list=EventList(),
            config=config)
        filemanager.populate_file_list()
        self.assertTrue(isinstance(filemanager, FileManager))
        self.assertTrue(os.path.exists(db))

        filemanager.update_local_status()
        filemanager.write_database()
        self.assertFalse(filemanager.all_data_local())
        
        # test that the filemanager returns correct paths
        paths = filemanager.get_file_paths_by_year(
            datatype='atm',
            case='20180129.DECKv1b_piControl.ne30_oEC.edison',
            start_year=1,
            end_year=2)
        for path in paths:
            self.assertTrue(os.path.exists(path))

        # test that the filemanager returns correct paths with no year
        paths = filemanager.get_file_paths_by_year(
            datatype='ocn',
            case='20180129.DECKv1b_piControl.ne30_oEC.edison')
        for path in paths:
            self.assertTrue(os.path.exists(path))

        # test nothing is returned for incorrect yeras
        paths = filemanager.get_file_paths_by_year(
            datatype='ocn',
            case='20180129.DECKv1b_piControl.ne30_oEC.edison',
            start_year=1,
            end_year=100)
        self.assertTrue(paths is None)

        # test the filemanager knows when data is ready
        ready = filemanager.check_data_ready(
            data_required=['atm'], 
            case='20180129.DECKv1b_piControl.ne30_oEC.edison',
            start_year=1,
            end_year=2)
        self.assertTrue(ready)

        # test the filemanager knows when data is NOT ready
        ready = filemanager.check_data_ready(
            data_required=['dummy'], 
            case='20180129.DECKv1b_piControl.ne30_oEC.edison',
            start_year=1,
            end_year=3)
        self.assertFalse(ready)

        ready = filemanager.check_data_ready(
            data_required=['ocn'], 
            case='20180129.DECKv1b_piControl.ne30_oEC.edison')
        self.assertTrue(ready)

        os.remove(db)

if __name__ == '__main__':
    unittest.main()
