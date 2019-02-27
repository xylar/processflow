"""
A child of Job, the Diag class is the parent for all diagnostic jobs
"""
import json
import os

from shutil import copytree, rmtree
from subprocess import call

from lib.jobstatus import JobStatus
from lib.util import print_line
from jobs.job import Job


class Diag(Job):
    def __init__(self, *args, **kwargs):
        super(Diag, self).__init__(*args, **kwargs)
        self._short_comp_name = ""
        self._host_url = ""
        self._comparison = kwargs.get('comparison', 'obs')
    # -----------------------------------------------

    @property
    def comparison(self):
        return self._comparison
    # -----------------------------------------------

    def __str__(self):
        return json.dumps({
            'type': self._job_type,
            'start_year': self._start_year,
            'end_year': self._end_year,
            'data_required': self._data_required,
            'depends_on': self._depends_on,
            'id': self._id,
            'comparison': self._comparison,
            'status': self._status.name,
            'case': self._case
        }, sort_keys=True, indent=4)
    # -----------------------------------------------

    def setup_hosting(self, config, img_source, host_path, event_list):
        """
        Performs file copys for images into the web hosting directory
        
        Parameters
        ----------
            config (dict): the global config object
            img_source (str): the path to where the images are coming from
            host_path (str): the path for where the images should be hosted
            event_list (EventList): an eventlist to push user notifications into
        """
        if config['global']['always_copy']:
            if os.path.exists(host_path):
                msg = '{prefix}: Removing previous output from host location'.format(
                    prefix=self.msg_prefix())
                print_line(msg, event_list)
                rmtree(host_path)
        if not os.path.exists(host_path):
            msg = '{prefix}: Moving files for web hosting'.format(
                prefix=self.msg_prefix())
            print_line(msg, event_list)
            copytree(
                src=img_source,
                dst=host_path)
        else:
            msg = '{prefix}: Files already present at host location, skipping'.format(
                prefix=self.msg_prefix())
            print_line(msg, event_list)
        # fix permissions for apache
        msg = '{prefix}: Fixing permissions'.format(
            prefix=self.msg_prefix())
        print_line(msg, event_list)
        call(['chmod', '-R', 'go+rx', host_path])
        tail, _ = os.path.split(host_path)
        for _ in range(2):
            call(['chmod', 'go+rx', tail])
            tail, _ = os.path.split(tail)
    # -----------------------------------------------

    def get_report_string(self):
        """
        Returns a nice report string of job status information
        """
        if self.status == JobStatus.COMPLETED:
            msg = '{prefix} :: {status} :: {url}'.format(
                prefix=self.msg_prefix(),
                status=self.status.name,
                url=self._host_url)
        else:
            msg = '{prefix} :: {status} :: {console_path}'.format(
                prefix=self.msg_prefix(),
                status=self.status.name,
                console_path=self._console_output_path)
        return msg
    # -----------------------------------------------

    def msg_prefix(self):
        return '{type}-{start:04d}-{end:04d}-{case}-vs-{comp}'.format(
                type=self.job_type,
                start=self.start_year,
                end=self.end_year,
                case=self.short_name,
                comp=self._short_comp_name)
    # -----------------------------------------------
