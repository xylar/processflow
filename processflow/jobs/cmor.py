from __future__ import absolute_import, division, print_function, unicode_literals
import logging
import os

from processflow.jobs.job import Job
from processflow.lib.util import print_line, get_cmip_output_files
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
        self._job_type = 'cmor'
        self._requires = []
        self._data_required = []
        self._table = ''

        config = kwargs['config']
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
            self._data_required = ['ocn']
            self._run_type = 'ocn'
        else:
            raise ValueError(f"{self._run_type} isnt an expected CMOR data type")
    
        self._table = kwargs['run_type']
        
        if not kwargs['config']['post-processing']['cmor'].get(kwargs['run_type']):
            raise ValueError(f'CMOR job must be given a set of variables to run')

        self._variables = kwargs['config']['post-processing']['cmor'][kwargs['run_type']]
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
                self.short_name,
                f'{self.start_year:04d}_{self.end_year:04d}')
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

        Parameters
        ----------
            config (dict) the global config object
        Returns
        -------
            True if the job completed successfully
            False otherwise
        """
        found = list()
        for _, _, files in os.walk(self._output_path):
            if files is None:
                continue
            for f in files:
                if f.endswith('nc'):
                    found.append(f)
        
        found_vars = []
        for filename in files:
            var, start, end = get_cmip_output_files(filename)
            if not var or not start or not end:
                continue
            if var in config['post-processing']['cmor'][self._table]['variables'] \
               and start == self._start_year \
               and end == self._end_year:
                found_vars.append(var)
        
        for var in config['post-processing']['cmor'][self._table]['variables']:
            if var in found_vars:
                continue
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
            import ipdb; ipdb.set_trace()
            msg = f'Unable to find timeseries for {self.msg_prefix()}, does this case generate timeseries?'
            raise Exception(msg)
    # -----------------------------------------------

    def execute(self, config, event_list, *args, dryrun=False, **kwargs):
        """
        Execute the CMOR job

        Parameters
        ----------
            config (dict): the global config object
            event_list (EventList): an EventList to push notifications into
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
        if self._run_type == 'atm':
            mode = 'atm'
        elif self._run_type == 'lnd':
            mode = 'lnd'
        elif self._run_type == 'cice':
            mode = 'ice'
        elif self._run_type == 'ocn':
            mode = 'ocn'
        cmd = [
            'e3sm_to_cmip',
            '--input', input_path,
            '--output', self._output_path,
            '--var-list', ' '.join(config['post-processing']
                                   ['cmor'][self._table]['variables']),
            '-u', config['simulations'][self.case]['user_input_json_path'],
            '--tables', config['post-processing']['cmor']['cmor_tables_path'],
            '--num-proc', '24',
            '--mode', mode        
        ]
        custom_handlers = config['post-processing']['cmor'].get(
            'custom_handlers_path')
        if custom_handlers is not None:
            cmd.extend(['--handlers', custom_handlers])

        return self._submit_cmd_to_manager(config, cmd, event_list)
    # -----------------------------------------------

    def handle_completion(self, filemanager, event_list, config, *args, **kwargs):
        """
        Adds the output from cmor into the filemanager database as type 'cmorized'

        Paremeters
        ----------
            filemanager (FileManager): the manager to add files to
            event_list (EventList): an EventList to add notification messages to
            config (dict): the global config object
        Returns
        -------
            True if files added correctly
            False if there was any error
        """
        try:
            new_files = list()
            for _, _, files in os.walk(self._output_path):
                if files is None:
                    continue
                for f in files:
                    new_files.append({
                        'name': f,
                        'local_path': os.path.abspath(f),
                        'case': self.case,
                        'year': self.start_year,
                        'month': self.end_year,  # use the month to hold the end year field
                        'local_status': FileStatus.PRESENT.value
                    })
            filemanager.add_files(
                data_type='cmorized',
                file_list=new_files,
                super_type='derived')
            filemanager.write_database()
            msg = f'{self.msg_prefix()}: Job completion handler done'
            print_line(msg)
            logging.info(msg)
            return True
        except Exception as e:
            raise e
        return False
    # -----------------------------------------------
