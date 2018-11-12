import os
import re
import logging

from bs4 import BeautifulSoup
from shutil import move

from jobs.diag import Diag
from lib.util import render, print_line
from lib.jobstatus import JobStatus


class MPASAnalysis(Diag):
    def __init__(self, *args, **kwargs):
        """
        Parameters
        ----------
            config (dict): the global configuration object
            custom_args (dict): a dictionary of user supplied arguments
                to pass to the resource manager
        """
        super(MPASAnalysis, self).__init__(*args, **kwargs)
        self._job_type = 'mpas_analysis'
        self._requires = ''
        self._host_path = ''
        self._host_url = ''
        self._input_base_path = ''
        self._data_required = ['cice', 'ocn',
                               'ocn_restart', 'cice_restart',
                               'ocn_streams', 'cice_streams',
                               'ocn_in', 'cice_in',
                               'meridionalHeatTransport']
        custom_args = kwargs['config']['diags']['aprime'].get('custom_args')
        if custom_args:
            self.set_custom_args(custom_args)
        if self.comparison == 'obs':
            self._short_comp_name = 'obs'
        else:
            self._short_comp_name = kwargs['config']['simulations'][self.comparison]['short_name']
    # -----------------------------------------------
    def setup_dependencies(self, *args, **kwargs):
        """
        aprime doesnt depend on any other jobs
        """
        return
    # -----------------------------------------------
    def execute(self, config, event_list, dryrun=False):
        """
        Generates and submits a run script for ncremap to regrid model output

        Parameters
        ----------
            config (dict): the globus processflow config object
            event_list (EventList): an EventList to push user notifications into
            dryrun (bool): a flag to denote that all the data should be set,
                and the scripts generated, but not actually submitted
        """
        self._dryrun = dryrun

        # sets up the output path, creating it if it doesnt already exist
        self._output_path = os.path.join(
            config['global']['project_path'],
            'output', 'diags', self.short_name, 'mpas_analysis',
            '{start:04d}_{end:04d}_vs_{comp}'.format(
                start=self.start_year,
                end=self.end_year,
                comp=self._short_comp_name))
        if not os.path.exists(self._output_path):
            os.makedirs(self._output_path)

        self._host_path = os.path.join(
            config['img_hosting']['host_directory'],
            self.short_name,
            self._job_type)

        # setup template
        template_out = os.path.join(
            config['global']['run_scripts_path'],
            '{job}_{start:04d}_{end:04d}_{case}_vs_{comp}.bash'.format(
                job=self._job_type,
                start=self.start_year,
                end=self.end_year,
                case=self.short_name,
                comp=self._short_comp_name))
        variables = {
            'case': self.case,
            'numWorkers': config['diags']['mpas_analysis']['num_workers'],
            'baseInputPath': self._input_base_path
        }
        # TODO: finish

    # -----------------------------------------------
    def postvalidate(self, config, *args, **kwargs):
        """
        Validates that the diagnostic produced its expected output

        Parameters
        ----------
            config (dict): the global cofiguration object
        Returns
        -------
            True if all output exists as expected
            False otherwise
        """
        pass
    # -----------------------------------------------

    def handle_completion(self, event_list, config, *args):
        """
        Setup for webhosting after a successful run
        
        Parameters
        ----------
            event_list (EventList): an event list to push user notifications into
            config (dict): the global config object
        """
        pass
    # -----------------------------------------------
