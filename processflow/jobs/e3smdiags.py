"""
A wrapper class around E3SM Diags
"""
from __future__ import absolute_import, division, print_function, unicode_literals
import logging
import os

from bs4 import BeautifulSoup

from processflow.jobs.diag import Diag
from processflow.lib.jobstatus import JobStatus
from processflow.lib.util import render, print_line

class E3SMDiags(Diag):
    def __init__(self, *args, **kwargs):
        super(E3SMDiags, self).__init__(*args, **kwargs)
        self._job_type = 'e3sm_diags'
        self._requires = []
        self._data_required = []

        config = kwargs['config']
        custom_args = config['diags']['e3sm_diags'].get(
            'custom_args')
        if custom_args:
            self.set_custom_args(custom_args)
        
        if 'area_mean_time_series' in config['diags']['e3sm_diags']['sets_to_run']:
            self._requires.append('timeseries')
            self._data_required.append('ts_regrid_atm')
        else:
        # if config['diags']['e3sm_diags']['sets_to_run'] != ['area_mean_time_series']:
            self._requires.append('climo')
            self._data_required.append('climo_regrid')

        if config['global']['host']:
            self._host_path = os.path.join(
                kwargs['config']['img_hosting']['host_directory'],
                self.short_name,
                'e3sm_diags',
                '{start:04d}_{end:04d}_vs_{comp}'.format(
                    start=self.start_year,
                    end=self.end_year,
                    comp=self._short_comp_name))
        else:
            self._host_path = ''

        # setup the output directory, creating it if it doesnt already exist
        custom_output_path = config['diags'][self.job_type].get(
            'custom_output_path')
        if custom_output_path:
            self._replace_dict['COMPARISON'] = self._short_comp_name
            self._output_path = self.setup_output_directory(custom_output_path)
        else:
            self._output_path = os.path.join(
                config['global']['project_path'],
                'output',
                'diags',
                self.short_name,
                self.job_type,
                '{start:04d}_{end:04d}_vs_{comp}'.format(
                    start=self.start_year,
                    end=self.end_year,
                    comp=self._short_comp_name))
        if not os.path.exists(self._output_path):
            os.makedirs(self._output_path)
        self.setup_job_args(config)
        self.setup_job_params(config)
    # -----------------------------------------------

    def _dep_filter(self, job):
        """
        find the climo/ts job we're waiting for, assuming there's only
        one climo/ts job in this case with the same start and end years
        """
        if job.job_type not in self._requires:
            return False
        if job.start_year != self.start_year:
            return False
        if job.end_year != self.end_year:
            return False
        if job.job_type == 'timeseries':
            if job.run_type != 'atm': # we dont care about lnd/ocn/sea-ice ts jobs
                return False
        return True
    # -----------------------------------------------

    def setup_dependencies(self, jobs, *args, **kwargs):
        """
        Adds climo jobs from this or the comparison case to the list of dependent jobs

        Parameters
        ----------
            jobs (list): a list of the rest of the run managers jobs
            optional: comparison_jobs (list): if this job is being compared to
                another case, the climos for that other case have to be done already too
        """
        if self.comparison != 'obs':
            # TODO: get model-vs-model to work for ts
            other_jobs = kwargs['comparison_jobs']
            try:
                self_climo, = [job for job in jobs if self._dep_filter(job)]
            except ValueError:
                msg = 'Unable to find climo for {}, is this case set to generate climos?'.format(
                    self.msg_prefix())
                raise Exception(msg)
            try:
                comparison_climo, = [job for job in other_jobs if self._dep_filter(job)]
            except ValueError:
                msg = 'Unable to find climo for {}, is that case set to generates climos?'.format(
                    self.comparison)
                raise Exception(msg)
            self.depends_on.extend((self_climo.id, comparison_climo.id))
        else:
            for job in jobs:
                if self._dep_filter(job):
                    self.depends_on.append(job.id)
            if not self.depends_on:
                raise ValueError('Unable to find job dependencies for {}'.format(str(self)))
    # -----------------------------------------------

    def execute(self, config, event_list, *args, slurm_args=None, dryrun=False, **kwargs):
        """
        Generates and submits a run script for e3sm_diags

        Parameters
        ----------
            config (dict): the global processflow config object
            dryrun (bool): a flag to denote that all the data should be set,
                and the scripts generated, but not actually submitted
        """
        self._dryrun = dryrun
        variables = dict()

        if dryrun:
            input_path = os.path.join(config['global']['project_path'], 'dummpy_input_path')
        else:
            input_path, _ = os.path.split(self._input_file_paths[0])

        variables['short_test_name'] = self.short_name
        variables['test_data_path'] = input_path
        variables['test_name'] = self.case
        variables['results_dir'] = self._output_path
        variables['num_workers'] = config['diags']['e3sm_diags'].get('num_workers', 24)
        variables['machine_path_prefix'] = config['diags']['e3sm_diags']['machine_path_prefix']

        if isinstance(config['diags']['e3sm_diags']['sets_to_run'], list):
            variables['sets_to_run'] = "' , '".join(config['diags']['e3sm_diags']['sets_to_run'])
        else:
            variables['sets_to_run'] = config['diags']['e3sm_diags']['sets_to_run']

        if self.comparison == 'obs':
            if 'area_mean_time_series' in config['diags']['e3sm_diags']['sets_to_run']:
                template_input_path = os.path.join(
                    config['global']['resource_path'],
                    'e3sm_diags_template_ts_vs_obs.py')
                variables['ts_start'] = self.start_year
                variables['ts_end'] = self.end_year
                variables['ts_test_data_path'] = [x for x in kwargs['depends_jobs'] if x.job_type == 'timeseries'].pop().output_path
            else:
                template_input_path = os.path.join(
                    config['global']['resource_path'],
                    'e3sm_diags_template_vs_obs.py')
        else:
            template_input_path = os.path.join(
                config['global']['resource_path'],
                'e3sm_diags_template_vs_model.py')
            input_path, _ = os.path.split(self._input_file_paths[0])
            variables['reference_data_path'] = input_path
            variables['ref_name'] = self.comparison
            variables['reference_name'] = config['simulations'][self.comparison]['short_name']
    
        if self._job_params:
            variables['custom_params'] = ''
            for p in self._job_params:
                variables['custom_params'] += '{} = "{}"\n'.format(p, self._job_params[p])

        # render the parameter file from the template
        param_template_out = os.path.join(
            config['global']['run_scripts_path'],
            'e3sm_diags_{start:04d}_{end:04d}_{case}_vs_{comp}_params.py'.format(
                start=self.start_year,
                end=self.end_year,
                case=self.short_name,
                comp=self._short_comp_name))


        # remove previous run script if it exists
        if os.path.exists(param_template_out):
            os.remove(param_template_out)
        render(
            variables=variables,
            input_path=template_input_path,
            output_path=param_template_out)
        
        cmd = ['python', param_template_out]
        return self._submit_cmd_to_manager(config, cmd, event_list)
    # -----------------------------------------------

    def postvalidate(self, config, *args, **kwargs):
        """
        Check that all the links created by the diagnostic are correct

        Parameters
        ----------
            config (dict): the global config object
        Returns
        -------
            True if all links are found
            False otherwise
        """
        return self._check_links(config)
    # -----------------------------------------------

    def handle_completion(self, filemanager, event_list, config, *args, **kwargs):
        """
        Perform setup for webhosting

        Parameters
        ----------
            event_list (EventList): an event list to push user notifications into
            config (dict): the global config object
        """
        if self.status != JobStatus.COMPLETED:
            msg = f'{self.msg_prefix()}: Job failed, not running completion handler'
            print_line(msg)
            return
        else:
            msg = f'{self.msg_prefix()}: Job complete'
            print_line(msg)

        # if hosting is turned off, simply return
        if not config['global'].get('host'):
            msg = f'{self.msg_prefix()}: Job completion handler done\n'
            print_line(msg)
            return
        
        msg = f'{self.msg_prefix()}: Setting up webhosting for diagnostic output'
        print_line(msg)

        self.setup_hosting(
            always_copy=config['global'].get('always_copy', False),
            img_source=self._output_path,
            host_path=self._host_path,
            event_list=event_list)

        self._host_url = 'https://{server}/{prefix}/{case}/e3sm_diags/{start:04d}_{end:04d}_vs_{comp}/viewer/index.html'.format(
            server=config['img_hosting']['img_host_server'],
            prefix=config['img_hosting']['url_prefix'],
            case=self.short_name,
            start=self.start_year,
            end=self.end_year,
            comp=self._short_comp_name)
        
        msg = f'{self.msg_prefix()}: Webhosting setup complete, diagnostic available at {self._host_url}'
        print_line(msg)
        
        msg = f'{self.msg_prefix()}: Job completion handler done\n'
        print_line(msg)
    # -----------------------------------------------

    def _check_links(self, config):

        viewer_path = os.path.join(self._output_path, 'viewer', 'index.html')
        if not os.path.exists(viewer_path):
            if self._has_been_executed:
                msg = f'{self.msg_prefix()}: could not find page index at {viewer_path}'
                logging.error(msg)
            return False

        viewer_head = os.path.join(self._output_path, 'viewer')
        if not os.path.exists(viewer_head):
            msg = '{}: could not find output viewer at {}'.format(
                self.msg_prefix(), viewer_head)
            logging.error(msg)
            return False
        missing_links = list()
        with open(viewer_path, 'r') as viewer_pointer:
            viewer_page = BeautifulSoup(viewer_pointer, 'lxml')
            viewer_links = viewer_page.findAll('a')
            for link in viewer_links:
                link_path = os.path.join(viewer_head, link.attrs['href'])
                if not os.path.exists(link_path):
                    missing_links.append(link_path)
                    continue
                if link_path[-4:] == 'html':
                    link_tail, _ = os.path.split(link_path)
                    with open(link_path, 'r') as link_pointer:
                        link_page = BeautifulSoup(link_pointer, 'lxml')
                        link_links = link_page.findAll('a')
                        for sublink in link_links:
                            try:
                                sublink_preview = sublink.attrs['data-preview']
                            except:
                                continue
                            else:
                                sublink_path = os.path.join(
                                    link_tail, sublink_preview)
                                if not os.path.exists(sublink_path):
                                    missing_links.append(sublink_path)
        if missing_links:
            msg = f'{self.msg_prefix()}: missing the following links'
            logging.error(msg)
            logging.error(missing_links)
            return False
        else:
            msg = f'{self.msg_prefix()}: all links found'
            logging.info(msg)
            return True
    # -----------------------------------------------
