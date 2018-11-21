import os
import re
import logging

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
        self.case_start_year = kwargs['config']['simulations']['start_year']
        self._data_required = ['cice', 'ocn',
                               'ocn_restart', 'cice_restart',
                               'ocn_streams', 'cice_streams',
                               'ocn_in', 'cice_in',
                               'meridionalHeatTransport']
        custom_args = kwargs['config']['diags']['mpas_analysis'].get('custom_args')
        if custom_args:
            self.set_custom_args(custom_args)
        if self.comparison == 'obs':
            self._short_comp_name = 'obs'
        else:
            self._short_comp_name = kwargs['config']['simulations'][self.comparison]['short_name']
    # -----------------------------------------------
    def setup_dependencies(self, *args, **kwargs):
        """
        mpas_analysis doesnt depend on any other jobs
        """
        return
    # -----------------------------------------------
    def execute(self, config, event_list, dryrun=False):
        """
        Generates and submits a run script for mpas_analysis

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

        if config.get('img_hosting'):
            self._host_path = os.path.join(
                config['img_hosting']['host_directory'],
                self.short_name,
                self._job_type)
        else:
            self._host_path = ''

        # setup template
        template_out = os.path.join(
            config['global']['run_scripts_path'],
            '{job}_{start:04d}_{end:04d}_{case}_vs_{comp}.cfg'.format(
                job=self._job_type,
                start=self.start_year,
                end=self.end_year,
                case=self.short_name,
                comp=self._short_comp_name))
        template_input_path = os.path.join(
            config['global']['resource_path'],
            'mpas_a_vs_obs.cfg')
        
        generate_string = '["' + '", "'.join([x for x in config['diags']['mpas_analysis'].get('generate_plots', '')]) + '"]'
        variables = {
            'case': self.case,
            'numWorkers': config['diags']['mpas_analysis'].get('num_workers', 8),
            'baseInputPath': self._input_base_path,
            'restartSubPath': self._input_base_path,
            'ocnHistSubPath': self._input_base_path,
            'iceHistSubPath': self._input_base_path,
            'meshName': config['simulations'][self.case].get('native_mpas_grid_name', ''),
            'mappingDirectory': config['diags']['mpas_analysis'].get('mapping_directory', ''),
            'ocnNamePath': self._input_base_path,
            'ocnStreamsPath': self._input_base_path,
            'iceNamePath': self._input_base_path,
            'iceStreamsPath': self._input_base_path,
            'outputBasePath': self._output_path,
            'generatePlots': generate_string,
            'startYear': self.start_year,
            'endYear': self.end_year,
            'tsYear': config['diags']['mpas_analysis'].get('start_year_offset', self.start_year),
            'tsEnd': self.end_year,
            'ninoStart': config['diags']['mpas_analysis'].get('start_year_offset', self.start_year),
            'ninoEnd': self.end_year,
            'ocnObsPath': config['diags']['mpas_analysis'].get('ocn_obs_data_path', ''),
            'iceObs': config['diags']['mpas_analysis'].get('seaice_obs_data_path', ''),
            'regionMaskPath': config['diags']['mpas_analysis'].get('region_mask_path', ''),
            'runMOC': config['diags']['mpas_analysis'].get('run_MOC', '')
        }
        render(
            variables=variables,
            input_path=template_input_path,
            output_path=template_out)
        
        cmd = ['mpas_analysis', template_out]
        self._has_been_executed = True
        return self._submit_cmd_to_manager(config, cmd)
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
        return self.status == JobStatus.COMPLETED
    # -----------------------------------------------

    def handle_completion(self, filemanager, event_list, config, *args, **kwargs):
        """
        Setup for webhosting after a successful run
        
        Parameters
        ----------
            event_list (EventList): an event list to push user notifications into
            config (dict): the global config object
        """
        pass
    # -----------------------------------------------
