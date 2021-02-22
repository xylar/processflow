from __future__ import absolute_import, division, print_function, unicode_literals
import logging
import os
import xarray as xr

from tqdm import tqdm
from processflow.jobs.job import Job
from processflow.lib.util import print_line, get_cmip_file_info, colors
from processflow.lib.filemanager import FileStatus


class Cmor(Job):
    """
    CMORize e3sm model output
    """

    def __init__(self, *args, **kwargs):
        """
        Parameters
        ----------
            config (dict): the global configuration object
        """
        super(Cmor, self).__init__(*args, **kwargs)
        config = kwargs['config']
        
        self._job_type = 'cmor'
        self._requires = []
        self._data_required = []
        self._table = ''
        self._completed_vars = []
        self._simple = False if config['simulations'][self.case].get('user_input_json_path') else True

        custom_args = config['post-processing'][self.job_type].get(
            'custom_args')
        if custom_args:
            self.set_custom_args(custom_args)


        if not kwargs.get('run_type'):
            raise ValueError(f'CMOR job must be given a list of tables to run on')
        
        if kwargs['run_type'] == 'Amon':
            self._data_required = ['ts_regrid_atm']
            self._requires = ['timeseries']
            self._run_type = 'atm'

        elif kwargs['run_type'] == 'Lmon':
            self._data_required = ['ts_regrid_lnd']
            self._requires = ['timeseries']
            self._run_type = 'lnd'

        elif kwargs['run_type'] == 'SImon':
            self._data_required = ['cice']
            self._run_type = 'cice'
            
        elif kwargs['run_type'] == 'Omon':
            self._data_required = ['ocn', 'ts_regrid_atm']
            self._run_type = 'ocn'
        else:
            raise ValueError(f"{self._run_type} isnt an expected CMOR data type")
    
        self._table = kwargs['run_type']
        
        if not kwargs['config']['post-processing']['cmor'].get(kwargs['run_type']):
            raise ValueError(f'CMOR job must be given a set of variables to run')

        self._variables = kwargs['config']['post-processing']['cmor'][kwargs['run_type']]['variables'].copy()
        if not isinstance(self._variables, list):
            self._variables = [self._variables]
        
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
                'cmor',
                self.short_name)
        if not os.path.exists(self._output_path):
            os.makedirs(self._output_path)
        self.setup_job_args(config)
    # -----------------------------------------------

    def _dep_filter(self, job):
        if job.job_type not in self._requires:
            return False
        if job.start_year != self.start_year:
            return False
        if job.end_year != self.end_year:
            return False
        return True
    # -----------------------------------------------

    def postvalidate(self, config, *args, **kwargs):
        """
        Validate that the CMOR job completed successfuly

        If any variables are found with the correct start and end date, they are removed
        from the list of variables left to be run.

        Parameters
        ----------
            config (dict) the global config object
        Returns
        -------
            True if the job completed successfully
            False otherwise
        """
        found = list()
        for root, _, files in os.walk(self._output_path):
            if files is None:
                continue
            for f in files:
                if f[-3:] == '.nc':
                    _, name = os.path.split(f)
                    var, start, end = get_cmip_file_info(name)
                    if var and var in self._variables and not start and not end:
                        # found an fx variable
                        found.append(os.path.join(root, f))
                    elif not var or not start or not end:
                        # not recognized
                        continue
                    elif var in self._variables \
                       and start == self._start_year \
                       and end == self._end_year:
                        found.append(os.path.join(root, f))

        if not found:
            return False
        
        pbar = tqdm(total=len(self._variables), desc=f"{colors.OKGREEN}[+]{colors.ENDC} {self.msg_prefix()}: Validating CMOR variables")

        for filename in found:
            _, name = os.path.split(filename)
            var, _, _ = get_cmip_file_info(name)
            pbar.set_description(f"{colors.OKGREEN}[+]{colors.ENDC} {self.msg_prefix()}: Checking {name}")
            try:
                _ = xr.open_dataset(filename)
            except IndexError:
                msg = f"{self.msg_prefix()}: Error in {filename}"
                print_line(msg, status='error')
            except ValueError:
                msg = f"{self.msg_prefix()}: bad CMOR file in CMOR output {name}, removing"
                print_line(msg, status='error')
                os.remove(filename)
            else:
                self._completed_vars.append(var)
            finally:
                pbar.update(1)
        pbar.close()

        if not self._completed_vars and self._has_been_executed:
            print_line(f'{self.msg_prefix()}: No completed variables for {self.short_name}', status='err')

        if self._completed_vars:
            msg = f"{self.msg_prefix()}: Found {len(self._completed_vars)} of {len(self._variables)} variables complete"
            print_line(msg)
        
        for var in self._completed_vars:
            if var in self._variables:
                self._variables.remove(var)

        # if there are any variables left, it means they weren't produced in the run
        if len(self._variables) != 0:
            msg = f"{self.msg_prefix()}: Starting remaining {len(self._variables)} variables"
            return False
        return True
    # -----------------------------------------------

    def setup_dependencies(self, *args, **kwargs):
        """
        CMOR requires timeseries output
        """
        if not self._requires:
            return
        for job in kwargs['jobs']:
            if job.case != self._case:
                continue
            if job.job_type in self._requires \
                and job.run_type == self.run_type \
                and job.start_year == self.start_year \
                and job.end_year == self.end_year:
                self.depends_on.append(job.id)
        if not self.depends_on:
            msg = f'Unable to find timeseries for {self.msg_prefix()}, does this case generate timeseries?'
            raise Exception(msg)
    # -----------------------------------------------

    def execute(self, config, *args, dryrun=False, **kwargs):
        """
        Execute the CMOR job

        Parameters
        ----------
            config (dict): the global config object
            dryrun (bool): if true this job will generate all scripts,
                setup data, and exit without submitting the job
        Returns
        -------
            True if the job has already been executed
            False if the job cannot be executed
            jobid (str): the resource managers assigned job id
                if the job was submitted to the resource manager
        """
        self._dryrun = dryrun
        
        input_path, _ = os.path.split(self._input_file_paths[0])
        additional_files = []
        if self._run_type in ['ocn', 'cice']:
            additional_files.append(config['post-processing']['cmor']['mpas_map_path'])
            _, map_path = os.path.split(config['post-processing']['cmor']['mpas_map_path'])
            additional_files.append(config['post-processing']['cmor']['regions_path'])
            additional_files.append(config['post-processing']['cmor']['mpas_mesh_path'])
            if self._run_type == 'ocn':
                additional_files.append(config['post-processing']['cmor']['mpaso-namelist'])
        
        for path in additional_files:
            _, filename = os.path.split(path)
            dest = os.path.join(input_path, filename)
            if not os.path.lexists(dest):
                os.symlink(path, dest)
        
        numproc = config['post-processing']['cmor'].get('numproc', 24)

        cmd = [
            'e3sm_to_cmip',
            '--input', input_path,
            '--output', self._output_path,
            '--var-list', ' '.join(self._variables),
            '--tables', config['post-processing']['cmor']['cmor_tables_path'],
            '--num-proc', str(numproc)
        ]
        
        if self._simple:
            cmd.append('--simple')
        else:
            cmd.extend(['-u', config['simulations'][self.case]['user_input_json_path']])
        
        if self._run_type == 'atm':
            mode = 'atm'
        elif self._run_type == 'lnd':
            mode = 'lnd'
        elif self._run_type == 'cice':
            mode = 'ice'
            cmd.extend(['--map', os.path.join(input_path, map_path)])
        elif self._run_type == 'ocn':
            mode = 'ocn'
            cmd.extend(['--map', os.path.join(input_path, map_path)])
        cmd.extend(['--mode', mode])

        custom_handlers = config['post-processing']['cmor'].get(
            'custom_handlers_path')
        if custom_handlers is not None:
            cmd.extend(['--handlers', custom_handlers])

        return self._submit_cmd_to_manager(config, cmd)
    # -----------------------------------------------

    def handle_completion(self, filemanager, config, *args, **kwargs):
        """
        Adds the output from cmor into the filemanager database as type 'cmorized'

        Paremeters
        ----------
            filemanager (FileManager): the manager to add files to
            config (dict): the global config object
        Returns
        -------
            True if files added correctly
            False if there was any error
        """
        for dtype in [f'cmorized-{var}' for var in self._completed_vars]:
            if not config['data_types'].get(dtype):
                config['data_types'][dtype] = {'monthly': False}

        try:
            for root, _, files in os.walk(self._output_path):
                if files is None:
                    continue
                for f in files:
                    var, start, end = get_cmip_file_info(f)
                    if not var or var not in self._completed_vars or not start == self.start_year or not end == self.end_year:
                        continue
                    new_file = {
                        'name': f,
                        'local_path': os.path.join(root, f),
                        'case': self.case,
                        'year': self.start_year,
                        'month': self.end_year,  # use the month to hold the end year field
                        'local_status': FileStatus.PRESENT.value
                    }
                    filemanager.add_files(
                        data_type=f'cmorized-{var}',
                        file_list=[new_file],
                        super_type='derived')
            filemanager.write_database()
            msg = f'{self.msg_prefix()}: Job completion handler done\n'
            print_line(msg)
            return True
        except Exception as e:
            raise e
        return False
    # -----------------------------------------------

    @property
    def variables(self):
        return self._variables