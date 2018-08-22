import os
import re
import logging

from jobs.job import Job
from lib.util import render, print_line
from lib.jobstatus import JobStatus
from lib.filemanager import FileStatus


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
        found = list()
        for root, dirs, files in os.walk(self._output_path):
            if files is not None:
                found.extend(files)
        if len(found) == len(config['post-processing']['cmor']['variable_list']):
            return True
        else:
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
        new_files = list()
        for root, dirs, files in os.walk(self._output_path):
            if files is not None:
                for file in files:
                    new_files.append({
                        'name': file,
                        'local_path': os.path.abspath(file),
                        'case': self.case,
                        'year': self.start_year,
                        'month': self.end_year, # use the month to hold the end year field
                        'local_status': FileStatus.PRESENT.value
                    })
        filemanager.add_files(
            data_type='cmorized',
            file_list=new_files)
        filemanager.write_database()
        msg = '{prefix}: Job completion handler done'.format(
            prefix=self.msg_prefix())
        print_line(msg, event_list)
        logging.info(msg)
    # -----------------------------------------------