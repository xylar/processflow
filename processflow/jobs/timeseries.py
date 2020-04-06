from __future__ import absolute_import, division, print_function, unicode_literals
import logging
import os
import xarray as xr
from tqdm import tqdm

from processflow.jobs.job import Job
from processflow.lib.jobstatus import JobStatus
from processflow.lib.util import get_ts_output_files, print_line
from processflow.lib.filemanager import FileStatus


class Timeseries(Job):
    """
    A Job subclass for managing time series variable extraction
    """

    def __init__(self, *args, **kwargs):
        super(Timeseries, self).__init__(*args, **kwargs)
        self._job_type = 'timeseries'
        self._data_required = [self._run_type]
        self._regrid = False
        self._regrid_path = ''
        
        config = kwargs['config']
        custom_args = config['post-processing'][self.job_type].get(
            'custom_args')
        if custom_args:
            self.set_custom_args(custom_args)
        
        self._var_list = config['post-processing']['timeseries'][self._run_type]
        if isinstance(self._var_list, list) and ' ' in self._var_list[0]:
            self._var_list = self._var_list[0].split(' ')
        elif isinstance(self._var_list, list) and ',' in self._var_list[0]:
            self._var_list = self._var_list[0].split(',')
        self._var_list = list(filter(lambda x: x != '', self._var_list))

        # setup the output directory, creating it if it doesnt already exist
        custom_output_path = config['post-processing'][self.job_type].get(
            'custom_output_path')
        if custom_output_path:
            self._output_path = self.setup_output_directory(custom_output_path)
        else:
            self._output_path = os.path.join(
                config['global']['project_path'],
                'output',
                'pp',
                'ts',
                self._short_name,
                config['simulations'][self.case]['native_grid_name'],
                '{length}yr'.format(length=self.end_year - self.start_year + 1))
        if not os.path.exists(self._output_path):
            os.makedirs(self._output_path)

        regrid_map_path = config['post-processing']['timeseries'].get(
            'regrid_map_path')
        if regrid_map_path and os.path.exists(regrid_map_path):
            self._regrid = True
            self._regrid_path = os.path.join(
                config['global']['project_path'],
                'output',
                'pp',
                'ts',
                self._short_name,
                config['post-processing']['timeseries']['destination_grid_name'],
                '{length}yr'.format(length=self.end_year - self.start_year + 1))
        else:
            self._regrid = False
    # -----------------------------------------------

    def setup_dependencies(self, *args, **kwargs):
        """
        Timeseries doesnt require any other jobs
        """
        return True
    # -----------------------------------------------

    def postvalidate(self, config, *args, **kwargs):
        """
        validate that all the timeseries variable files were producted as expected

        Parameters
        ----------
            config (dict): the global config object
        Returns
        -------
            True if all the files exist
            False otherwise
        """
        if self._dryrun:
            return True
        if not self._input_base_path or not self._input_file_paths:
            return False

        # if the check removes all the variables, then they are all
        # in the output location
        self.check_all_variables_present(config)
        if not self._var_list:
            return True

        for var in self._var_list:
            file_name = "{var}_{start:04d}01_{end:04d}12.nc".format(
                var=var,
                start=self.start_year,
                end=self.end_year)
            file_path = os.path.join(self._output_path, file_name)
            if not os.path.exists(file_path):
                if self._has_been_executed:
                    msg = "{prefix}: Unable to find {file} after execution".format(
                        prefix=self.msg_prefix(),
                        file=file_path)
                    logging.error(msg)
                return False

        # next, if regridding is turned on check that all regrid ts files were created
        if self._regrid:

            for var in self._var_list:
                file_name = "{var}_{start:04d}01_{end:04d}12.nc".format(
                    var=var,
                    start=self.start_year,
                    end=self.end_year)
                file_path = os.path.join(self._regrid_path, file_name)
                if not os.path.exists(file_path):
                    if self._has_been_executed:
                        msg = "{prefix}: Unable to find {file} after execution".format(
                            prefix=self.msg_prefix(),
                            file=file_path)
                        logging.error(msg)
                    return False

        # if nothing was missing then we must be done
        return True
    # -----------------------------------------------

    def check_all_variables_present(self, config):
        
        # Load the first file as an xarray dataset
        path = self._input_file_paths[0]
        if not os.path.exists(path):
            msg = "Unable to find input file: {}".format(path)
            print_line(msg)

        ds = xr.open_dataset(path)
        if self._regrid:
            files_in_output = os.listdir(self._regrid_path)
        else:
            files_in_output = os.listdir(self._output_path)
        
        to_remove = list()
        # Check that each of the variables we're trying to extract is present
        for variable in self._var_list:
            if variable and variable not in ds.data_vars:
                self._var_list.remove(variable)
                msg = "Variable not found in dataset: {}".format(variable)
                print_line(msg)
            else:
                for f in files_in_output:
                    if variable + '_' in f or variable + '.nc' == f:
                        to_remove.append(variable)
                        msg = "Time series variable already exists: {}".format(os.path.join(self._output_path, f))
                        print_line(msg)
                        break

        self._var_list = list(filter(lambda x: x not in to_remove, self._var_list))
        ds.close()
        return
    # -----------------------------------------------

    def extract_scalar(self):

        msg = 'Checking for scalar variables'
        print_line(msg)
        # Load the first file as an xarray dataset
        path = self._input_file_paths[0]
        if not os.path.exists(path):
            msg = "Unable to find input file: {}".format(path)
            print_line(msg)

        ds = xr.open_dataset(path)
        to_remove = list()
        for variable in self._var_list:
            if 'time' not in ds[variable].coords:
                if 'ncol' not in ds[variable].coords:
                    msg = 'Found scalar variable {}, extracting'.format(variable)
                    print_line(msg)
                    to_remove.append(variable)
                    outpath = os.path.join(self._output_path, variable + '.nc')
                    os.popen('ncks -v {variable} {inpath} {outpath}'.format(
                        variable=variable,
                        inpath=self._input_file_paths[0],
                        outpath=outpath))
                    if self._regrid:
                        os.popen('cp {} {}'.format(outpath, self._regrid_path))
                else:
                    msg = 'No time axis for variable {}, removing from variable list'.format(variable)
                    print_line(msg)
                    to_remove.append(variable)
        ds.close()
        self._var_list = list(filter(lambda x: x not in to_remove, self._var_list))

    def execute(self, config, event_list, dryrun=False):
        """
        Generates and submits a run script for e3sm_diags

        Parameters
        ----------
            config (dict): the global processflow config object
            event_list (EventList): an event list to push user notifications into
            dryrun (bool): a flag to denote that all the data should be set,
                and the scripts generated, but not actually submitted
        """
        self._dryrun = dryrun

        # sort the input files
        self._input_file_paths = sorted(self._input_file_paths)
        input_path, _ = os.path.split(self._input_file_paths[0])

        self.check_all_variables_present(config)
        self.extract_scalar()

        # create the ncclimo command string
        cmd = [
            'ncclimo',
            '-7',
            '--d2f',
            '--dfl_lvl=1',
            '-j', '8',
            '--no_cll_msr',
            '--no_frm_trm',
            '--no_stg_grd',
            '--input={}'.format(input_path),
            '-v', ','.join(self._var_list),
            '-s', str(self.start_year),
            '-e', str(self.end_year),
            '--ypf={}'.format(self.end_year - self.start_year + 1),
            '-o', self._output_path
        ]
        if self._regrid:
            cmd.extend([
                '-O', self._regrid_path,
                '--map={}'.format(config['post-processing']['timeseries'].get(
                    'regrid_map_path')),
            ])
        if self._run_type == 'land' or self._run_type == 'lnd':
            cmd.extend(['--sgs_frc={}/landfrac'.format(self._input_file_paths[0])])
        elif self._run_type == 'ocn' or self._run_type == 'ocean':
            cmd.extend(['-m', 'mpas'])
        elif self._run_type == 'ice' or self._run_type == 'sea-ice':
            cmd.extend('-m', 'mpas', '--sgs_frc={}/timeMonthly_avg_iceAreaCell'.format(self._input_file_paths[0]))
 
        return self._submit_cmd_to_manager(config, cmd, event_list)
    # -----------------------------------------------

    def handle_completion(self, filemanager, event_list, config, *args, **kwargs):
        """
        Post run handler, adds produced timeseries variable files into
        the filemanagers database

        Parameters
        ----------
            filemanager (FileManager): The filemanager to add the files to
            event_list (EventList): an event list to push user notifications into
            config (dict): the global config object
        """

        if not config['data_types'].get('ts_native'):
            config['data_types']['ts_native'] = {'monthly': False}
        if not config['data_types'].get('ts_regrid'):
            config['data_types']['ts_regrid'] = {'monthly': False}
        if self._dryrun:
            return

        if self.status != JobStatus.COMPLETED:
            msg = '{prefix}: Job failed, not running completion handler'.format(
                prefix=self.msg_prefix())
            print_line(msg)
            logging.info(msg)
            return
        else:
            msg = '{prefix}: Job complete'.format(
                prefix=self.msg_prefix())
            print_line(msg)

        new_files = list()
        ts_files = get_ts_output_files(
            self._output_path, 
            self._var_list, 
            self.start_year, 
            self.end_year)
        if not ts_files:
            self.status = JobStatus.FAILED
            msg = '{prefix}: Job failed, not running completion handler'.format(
                prefix=self.msg_prefix())
            print_line(msg)
            return
        for ts_file in ts_files:
            new_files.append({
                'name': ts_file,
                'local_path': os.path.join(self._output_path, ts_file),
                'case': self.case,
                'year': self.start_year,
                'month': self.end_year,
                'local_status': FileStatus.PRESENT.value
            })
        filemanager.add_files(
            data_type='ts_native',
            file_list=new_files,
            super_type='derived')

        if self._regrid:
            new_files = list()
            ts_files = get_ts_output_files(
                self._regrid_path,
                self._var_list,
                self.start_year,
                self.end_year)
            if not ts_files:
                self.status = JobStatus.FAILED
                msg = '{prefix}: Job failed, not running completion handler'.format(
                    prefix=self.msg_prefix())
                print_line(msg)
                return
            for ts_file in ts_files:
                new_files.append({
                    'name': ts_file,
                    'local_path': os.path.join(self._regrid_path, ts_file),
                    'case': self.case,
                    'year': self.start_year,
                    'month': self.end_year,
                    'local_status': FileStatus.PRESENT.value
                })
            filemanager.add_files(
                data_type='ts_regrid',
                file_list=new_files,
                super_type='derived')

        filemanager.write_database()
        msg = '{prefix}: Job completion handler done'.format(
            prefix=self.msg_prefix())
        print_line(msg)
    # -----------------------------------------------

    def setup_temp_path(self, config, *args, **kwards):
        """
        creates the input structure for the regrid job
        /project/output/temp/case_short_name/job_type_run_type/start_end
        """
        return os.path.join(
            config['global']['project_path'],
            'output', 'temp', self._short_name,
            '{}_{}'.format(self._job_type, self._run_type),
            '{:04d}_{:04d}'.format(self._start_year, self._end_year))
    # -----------------------------------------------

    def get_run_name(self):
        return '{type}_{run_type}_{start:04d}_{end:04d}_{case}'.format(
            type=self.job_type,
            run_type=self._run_type,
            start=self.start_year,
            end=self.end_year,
            case=self.short_name)
    # -----------------------------------------------
