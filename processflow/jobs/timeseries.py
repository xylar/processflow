from __future__ import absolute_import, division, print_function, unicode_literals
import logging
import os
import xarray as xr
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, as_completed

from processflow.jobs.job import Job
from processflow.lib.jobstatus import JobStatus
from processflow.lib.util import get_ts_output_files, print_line, colors
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
        custom_args = config['post-processing']['timeseries'].get(
            'custom_args')
        if custom_args:
            self.set_custom_args(custom_args)

        self._original_var_list = config['post-processing']['timeseries'][self._run_type]
        if isinstance(self._original_var_list, list) and ' ' in self._original_var_list[0]:
            self._original_var_list = self._original_var_list[0].split(' ')
        elif isinstance(self._original_var_list, list) and ',' in self._original_var_list[0]:
            self._original_var_list = self._original_var_list[0].split(',')
        self._original_var_list = list(filter(lambda x: x != '', self._original_var_list))
        self._var_list = self._original_var_list[:]

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
                '{length}yr'.format(length=self.end_year - self.start_year + 1),
                self._run_type)
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
                '{length}yr'.format(length=self.end_year - self.start_year + 1),
                self._run_type)
        else:
            self._regrid = False
        self.setup_job_args(config)

    @property
    def output_path(self):
        if self._regrid:
            return self._regrid_path
        else:
            return self._output_path
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

        msg = f'{self.msg_prefix()}: Running output check'
        print_line(msg)

        if self._dryrun:
            return True
        
        # filter out variables that exist
        self.filter_var_list()
        if not self._var_list:
            self.status = JobStatus.COMPLETED
            return True

        if self._has_been_executed:
            for var in self._var_list:
                msg = f"{self.msg_prefix()}: Unable to find {var} after execution",
                print_line(msg, status='error')

        # if anything is left in the var list then the job had an error or needs to run
        return False
    # -----------------------------------------------

    def check_all_variables_present(self, config):

        # Load the first file as an xarray dataset
        path = self._input_file_paths[0]
        if not os.path.exists(path):
            msg = f"Unable to find input file: {path}"
            print_line(msg)

        ds = xr.open_dataset(path)
        to_remove = list()
        # Check that each of the variables we're trying to extract is present
        for variable in self._var_list:
            if variable and variable not in ds.data_vars:
                to_remove.append(variable)
                msg = f"Variable not found in dataset: {variable}"
                print_line(msg, status='err')

        self._var_list = list(
            filter(lambda x: x not in to_remove, self._var_list))
        ds.close()
        return
    # -----------------------------------------------

    def check_file_integrity(self, file_path, var):
        if os.path.exists(file_path):
            try:
                _ = xr.open_dataset(file_path)
            except (IndexError, ValueError):
                print_line(f'Found and error in {file_path}', status='error')
                return None
            else:
                return var
        else:
            return None
        
    def filter_var_list(self):
        to_remove = list()

        # if regridding is turned on check that all regrid ts files were created
        if self._regrid:
            file_source = self._regrid_path
        else:
            file_source = self._output_path
        
        if not os.path.exists(file_source) or not len(os.listdir(file_source)):
            return
        
        pbar = tqdm(total=len(self._var_list), desc=f"{colors.OKGREEN}[+]{colors.ENDC} {self.msg_prefix()}: Checking time-series output")
        futures = []
        with ProcessPoolExecutor(max_workers=8) as pool:
            for var in self._var_list:
                if os.path.exists(os.path.join(file_source, f"{var}.nc")):
                    file_name = f"{var}.nc"
                else:
                    file_name = "{var}_{start:04d}01_{end:04d}12.nc".format(
                        var=var,
                        start=self.start_year,
                        end=self.end_year)
                file_path = os.path.join(file_source, file_name)
                futures.append(
                    pool.submit(self.check_file_integrity, file_path, var))
            
            for future in as_completed(futures):
                pbar.update(1)
                res = future.result()
                if res:
                    to_remove.append(res)
        pbar.close()
        self._var_list = list(
            filter(lambda x: x not in to_remove, self._var_list)
        )
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
                    msg = 'Found scalar variable {}, extracting'.format(
                        variable)
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
                    msg = 'No time axis for variable {}, removing from variable list'.format(
                        variable)
                    print_line(msg)
                    to_remove.append(variable)
        ds.close()
        self._var_list = list(
            filter(lambda x: x not in to_remove, self._var_list))

    def execute(self, config, event_list, *args, dryrun=False, **kwargs):
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
        if not self._var_list:
            msg = "Variable list is empty\n"
            print_line(msg)
            return 0

        # create the ncclimo command string
        cmd = ['ncclimo']
        if self._job_args:
            cmd.extend(self._job_args)
        cmd.extend([
            '--input={}'.format(input_path),
            '-v', ','.join(self._var_list),
            '-s', str(self.start_year),
            '-e', str(self.end_year),
            '--ypf={}'.format(self.end_year - self.start_year + 1),
            '-o', self._output_path
        ])

        if self._regrid:
            cmd.extend([
                '-O', self._regrid_path,
                '--map={}'.format(config['post-processing']['timeseries'].get(
                    'regrid_map_path')),
            ])
            
        if self._run_type == 'land' or self._run_type == 'lnd':
            cmd.append(f'--sgs_frc={self._input_file_paths[0]}/landfrac')

        elif self._run_type == 'ocn' or self._run_type == 'ocean':
            cmd.extend(['-m', 'mpas'])

        elif self._run_type == 'ice' or self._run_type == 'sea-ice':
            cmd.extend(
                ['-m', 'mpas', f'--sgs_frc={self._input_file_paths[0]}/timeMonthly_avg_iceAreaCell'])

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

        for dtype in [f'ts_native_{self._run_type}', f'ts_regrid_{self._run_type}']:
            if not config['data_types'].get(dtype):
                config['data_types'][dtype] = {'monthly': False}
        
        if self._dryrun:
            return

        if self.status != JobStatus.COMPLETED:
            
            msg = f'{self.msg_prefix()}: Job failed, not running completion handler'
            print_line(msg)
            logging.info(msg)
            return
        else:
            msg = f'{self.msg_prefix()}: Job complete'
            print_line(msg)

        new_files = list()
        ts_files = get_ts_output_files(
            self._output_path,
            self._original_var_list,
            self.start_year,
            self.end_year)
        if not ts_files:
            self.status = JobStatus.FAILED
            msg = f'{self.msg_prefix()}: Job failed, not running completion handler'
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
            data_type=f'ts_native_{self._run_type}',
            file_list=new_files,
            super_type='derived')

        if self._regrid:
            new_files = list()
            ts_files = get_ts_output_files(
                self._regrid_path,
                self._original_var_list,
                self.start_year,
                self.end_year)
            if not ts_files:
                self.status = JobStatus.FAILED
                msg = f'{self.msg_prefix()}: Job failed, not running completion handler'
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
                data_type='ts_regrid_' + self._run_type,
                file_list=new_files,
                super_type='derived')

        filemanager.write_database()
        msg = f'{self.msg_prefix()}: Job completion handler done\n'
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
