import os
import re
import logging

from jobs.job import Job
from lib.util import render, print_line
from lib.jobstatus import JobStatus

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
        self._requires = 'timeseries'
        self._data_required = ['ts_regrid']
        config = kwargs.get('config')
        if config:
            custom_args = config['post-processing']['timeseries'].get('custom_args')
            if custom_args:
                self.set_custom_args(custom_args)
        cmor_path = os.path.join(
            config['global']['project_path'], 'output', 'pp',
            'cmor', self._short_name)
        if not os.path.exists(cmor_path):
            os.makedirs(cmor_path)
        self._output_path = cmor_path
    # -----------------------------------------------
    def _dep_filter(self, job):
        if job.job_type != self._requires: 
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
        return False
    # -----------------------------------------------
    def setup_dependencies(self, *args, **kwargs):
        """
        CMOR requires timeseries output
        """
        jobs = kwargs['jobs']
        try:
            ts_job, = filter(lambda job: self._dep_filter(job), jobs)
        except ValueError:
            raise Exception('Unable to find timeseries for {}, is this case set to generate timeseries output?'.format(self.msg_prefix()))
        self.depends_on.append(ts_job.id)
    # -----------------------------------------------
    def execute(self, config, event_list, dryrun=False):
        self._dryrun = dryrun

        input_path, _ = os.path.split(self._input_file_paths[0])
        cmd = [
            'e3sm_to_cmip',
            '--input', input_path,
            '--output', self._output_path,
            '--var-list', ' '.join(config['post-processing']['cmor']['variable_list']),
            '--user-input', config['post-processing']['cmor'][self.case]['user_input_json_path'],
            '--tables', config['post-processing']['cmor']['cmor_tables_path'],
            '--num-proc', '24'
        ]
        custom_handlers = config['post-processing']['cmor'].get('custom_handlers_path')
        if custom_handlers is not None:
            cmd.extend(['--handlers', custom_handlers])

        self._has_been_executed = True
        return self._submit_cmd_to_manager(config, cmd)
    # -----------------------------------------------
    def handle_completion(self, filemanager, event_list, config):
        pass
    # -----------------------------------------------