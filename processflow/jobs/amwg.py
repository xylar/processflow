"""
A python wrapper around the AMWG diagnostics
"""
from __future__ import absolute_import, division, print_function, unicode_literals
import logging
import os
import re

from subprocess import call
from bs4 import BeautifulSoup

from processflow.jobs.diag import Diag
from processflow.lib.jobstatus import JobStatus
from processflow.lib.util import render, print_line


class AMWG(Diag):
    def __init__(self, *args, **kwargs):
        """
        Parameters
        ----------
            config (dict): the global configuration object
            custom_args (dict): a dictionary of user supplied arguments
                to pass to the resource manager
        """
        super(AMWG, self).__init__(*args, **kwargs)
        if not os.environ.get('NCARG_ROOT'):
            msg = 'ERROR: NCL doesnt appear to be installed in your environment, unable to run AMWG'
            print_line(msg, status='err')
            self._status = JobStatus.FAILED

        self._job_type = 'amwg'
        self._requires = ['climo']
        self._data_required = ['climo_regrid']

        config = kwargs.get('config')
        if config:
            if config['global'].get('dryrun') == True:
                self._dryrun = True
            if config['global'].get('host'):
                self._host_path = os.path.join(
                    config['img_hosting']['host_directory'],
                    self.short_name,
                    'amwg',
                    '{start:04d}_{end:04d}_vs_{comp}'.format(
                        start=self.start_year,
                        end=self.end_year,
                        comp=self._short_comp_name))
                self._host_url = 'https://{server}/{prefix}/{case}/amwg/{start:04d}_{end:04d}_vs_{comp}/index.html'.format(
                    server=config['img_hosting']['img_host_server'],
                    prefix=config['img_hosting']['url_prefix'],
                    case=self.short_name,
                    start=self.start_year,
                    end=self.end_year,
                    comp=self._short_comp_name)
            custom_args = config['diags'][self.job_type].get(
                'custom_args')
            if custom_args:
                self.set_custom_args(custom_args)

            # setup the output directory, creating it if it doesnt already exist
            custom_output_path = config['diags'][self.job_type].get(
                'custom_output_path')
            if custom_output_path:
                self._replace_dict['COMPARISON'] = self._short_comp_name
                self._output_path = self.setup_output_directory(
                    custom_output_path)
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
        else:
            self._host_path = ''
            self._output_path = ''

        self.setup_job_args(config)
    # -----------------------------------------------

    def setup_data(self, config, filemanager, case):
        """
        Change input file names to match what amwg expects
        """
        super(AMWG, self).setup_data(config, filemanager, case)
        self._change_input_file_names()
    # -----------------------------------------------

    def _dep_filter(self, job):
        """
        find the climo job we're waiting for, assuming there's only
        one climo job in this case with the same start and end years
        """
        if job.job_type not in self._requires:
            return False
        if job.start_year != self.start_year:
            return False
        if job.end_year != self.end_year:
            return False
        return True
    # -----------------------------------------------

    def setup_dependencies(self, *args, **kwargs):
        """
        AMWG requires climos
        """
        jobs = kwargs['jobs']
        if self.comparison != 'obs':
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
            try:
                self_climo, = [job for job in jobs if self._dep_filter(job)]
            except ValueError:
                msg = 'Unable to find climo for {}, is this case set to generate climos?'.format(
                    self.msg_prefix())
                raise Exception(msg)
            self.depends_on.append(self_climo.id)
    # -----------------------------------------------

    def execute(self, config, *args, custom_args=None, dryrun=False, **kwargs):
        """
        Execute the AMWG job

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

        # setup template
        csh_template_out = os.path.join(
            config['global']['run_scripts_path'],
            'amwg_{start:04d}_{end:04d}_{case}_vs_{comp}.csh'.format(
                start=self.start_year,
                end=self.end_year,
                case=self.short_name,
                comp=self._short_comp_name))
        variables = dict()

        if dryrun:
            input_path = os.path.join(
                config['global']['project_path'], 'dummpy_input_path')
        else:
            input_path, _ = os.path.split(self._input_file_paths[0])

        variables['test_casename'] = self.case
        variables['short_name'] = self.short_name
        variables['test_path_history'] = input_path + os.sep
        variables['test_path_climo'] = input_path + os.sep
        variables['test_path_diag'] = self._output_path + os.sep
        variables['diag_home'] = config['diags']['amwg']['diag_home']

        if 'all' in config['diags']['amwg']['sets']:
            variables['all_sets'] = '0'
            set_names = ['set_' + str(x) for x in range(1, 16)] + ['set_4a']
            for diag in set_names:
                variables[diag] = '0'
        else:
            variables['all_sets'] = '1'
            set_numbers = [str(x) for x in range(1, 17)] + ['4a']
            set_names = ['set_' + str(x) for x in range(1, 16)] + ['set_4a']
            for idx, diag in enumerate(set_names):
                variables[diag] = '0' if set_numbers[idx] in config['diags']['amwg']['sets'] else '1'

        if self.comparison == 'obs':
            template_input_path = os.path.join(
                config['global']['resource_path'],
                'amwg_template_vs_obs.csh')
        else:
            template_input_path = os.path.join(
                config['global']['resource_path'],
                'amwg_template_vs_model.csh')
            variables['cntl_casename'] = self.comparison
            variables['cntl_short_name'] = self._short_comp_name
            variables['cntl_path_history'] = input_path + os.sep
            variables['cntl_path_climo'] = input_path + os.sep

        # get environment path to use as NCARG_ROOT
        variables['NCARG_ROOT'] = os.environ['NCARG_ROOT']

        # remove previous amwg script if it exists
        if os.path.exists(csh_template_out):
            os.remove(csh_template_out)
        render(
            variables=variables,
            input_path=template_input_path,
            output_path=csh_template_out)

        # create the run command and submit it
        cmd = ['csh', csh_template_out]
        return self._submit_cmd_to_manager(config, cmd)
    # -----------------------------------------------

    def postvalidate(self, config, *args, **kwargs):
        """
        Validates that the job ran correctly

        Parameters
        ----------
            config (dict): the global config object
        Returns
        -------
            True if the job ran successfully
            False if there's any missing output
        """
        # the numbet of plot sets with missing plots
        number_missing = 0

        # the minimum number of files expected from each plotset
        expected_files = {
            "set4": 60,
            "set5_6": 100,
            "set7": 100,
            "set1": 10,
            "set2": 5,
            "set3": 100,
            "set8": 20,
            "set9": 5,
            "set16": 3,
            "set14": 10,
            "set15": 50,
            "set12": 50,
            "set13": 300,
            "set10": 50,
            "set11": 10,
            "set4a": 20
        }
        # where we expect to find output plots
        img_source = os.path.join(
            self._output_path,
            '{case}-vs-{comp}'.format(
                case=self.short_name,
                comp=self._short_comp_name))
        img_source_tar = img_source + '.tar'
        if not os.path.exists(img_source):
            if os.path.exists(img_source_tar):
                return self._check_tar(
                    img_source_tar, 
                    img_source, 
                    config, 
                    expected_files) == 0
            else:
                return False

        sets = []
        if 'all' in config['diags']['amwg']['sets']:
            sets = [str(x) for x in range(1, 16)] + ['4a']
        else:
            sets = config['diags']['amwg']['sets']

        # check that there have been enough plots created to call this a successful run
        for item in sets:
            if item == 'all':
                continue
            setname = 'set5_6' if item == '6' or item == '5' else 'set' + item
            setpath = os.path.join(
                img_source,
                setname)
            if not os.path.exists(setpath):
                # the job hasnt been started yet, and the directory is missing
                # so its probably never been run before
                if not self._has_been_executed:
                    return False
                # if the job HAS been run, then its possible that the output might
                # be in the .tar file, so we cant exit yet
                msg = '{prefix}: could not find output directory {dir}'.format(
                    prefix=self.msg_prefix(),
                    dir=setpath)
                logging.error(msg)
            else:
                count = len(os.listdir(setpath))
                if count < expected_files[setname]:
                    msg = '{prefix}: set {set} only produced {numProduced} when at least {numExpected} were expected'.format(
                        prefix=self.msg_prefix(),
                        set=setname,
                        numProduced=count,
                        numExpected=expected_files[setname])
                    logging.error(msg)
                    number_missing += 1

        if number_missing > 0:
            number_missing = 0
            if os.path.exists(img_source_tar):
                number_missing = self._check_tar(
                    img_source_tar, img_source, config, expected_files)

        if number_missing == 0:
            msg = '{prefix}: all expected output images found'.format(
                prefix=self.msg_prefix())
            print_line(msg)
            logging.info(msg)
            self._check_links(config, img_source)
            return True
        elif number_missing > 0 and number_missing <= 2:
            msg = '{prefix}: this job was found to be missing plots, please check the console output for additional information'.format(
                prefix=self.msg_prefix())
            print_line(msg)
            self._check_links(config, img_source)
            return True
        else:
            return False
    # -----------------------------------------------

    def handle_completion(self, filemanager, config, *args, **kwargs):
        """
        Sets up variables needed to web hosting

        Parameters
        ----------
            filemanager: the global filemanager instance
            config (dict): the global config object
        """
        if self.status != JobStatus.COMPLETED:
            msg = f'{self.msg_prefix()}: Job failed, not running completion handler'
            print_line(msg, status='error')
            return

        # if hosting is turned off, simply return
        if not config['global'].get('host'):
            msg = f'{self.msg_prefix()}: Job completion handler done\n'
            print_line(msg)
            return

        img_source = os.path.join(
            self._output_path,
            '{case}-vs-{comp}'.format(
                case=self.short_name,
                comp=self._short_comp_name))

        if not os.path.exists(img_source):
            if os.path.exists(img_source + '.tar'):
                call(['tar', '-xf', img_source + '.tar',
                      '--directory', self._output_path])
            else:
                msg = '{prefix}: Unable to find output directory or tar archive'.format(
                    prefix=self.msg_prefix())
                print_line(msg)
                self.status = JobStatus.FAILED
                logging.info(msg)
                return

        self.setup_hosting(
            always_copy=config['global']['always_copy'],
            img_source=img_source,
            host_path=self._host_path)
        
        msg = f'{self.msg_prefix()}: Job completion handler done\n'
        print_line(msg)

    # -----------------------------------------------

    def _check_links(self, config, img_source):
        """
        Checks output page for all links, as well as first level subpages

        Parameters:
            config,
            img_source
        Returns:
            True if all links are found, False otherwise
        """

        missing_links = list()
        page_path = os.path.join(
            img_source,
            'index.html')
        page_tail, page_head = os.path.split(page_path)
        if not os.path.exists(page_path):
            msg = '{prefix}: No output page found'.format(
                prefix=self.msg_prefix())
            logging.error(msg)
            return False

        # read in the base output page and parse it
        with open(page_path, 'r') as page_pointer:
            output_page = BeautifulSoup(page_pointer, 'lxml')
            output_links = output_page.findAll('a')

        # iterate over all the links on the page
        missing_subpage_links = None
        for link in output_links:
            link_path = link.attrs['href']
            if link_path[-3:] == 'htm':
                subpage_path = os.path.join(page_tail, link.attrs['href'])
                subpage_tail, subpage_head = os.path.split(subpage_path)
                missing_subpage_links = list()
                if not os.path.exists(subpage_path):
                    link.replace_with_children()
                    msg = '{prefix}: web page missing {page}'.format(
                        prefix=self.msg_prefix(),
                        page=subpage_head)
                    logging.error(msg)
                    missing_links.append(subpage_path)
                    continue
                with open(subpage_path, 'r') as subpage_pointer:
                    subpage = BeautifulSoup(subpage_pointer, 'lxml')
                    subpage_links = subpage.findAll('a')
                for sublink in subpage_links:
                    sublink_href = sublink.attrs['href']
                    if sublink_href[-3:] != 'png':
                        continue
                    sublink_path = os.path.join(subpage_tail, sublink_href)
                    if not os.path.exists(sublink_path):
                        sublink.replace_with_children()
                        missing_subpage_links.append(sublink_path)
                if missing_subpage_links:
                    os.rename(subpage_path, subpage_path + '.bak')
                    with open(subpage_path, 'w') as outfile:
                        outfile.write(str(subpage))
            if missing_subpage_links:
                missing_links.extend(missing_subpage_links)
        if missing_links:
            os.rename(page_path, page_path + '.bak')
            with open(page_path, 'w') as outfile:
                outfile.write(str(output_page))
        else:
            msg = '{prefix}: all links found'.format(
                prefix=self.msg_prefix())
            logging.info(msg)
        return True
    # -----------------------------------------------

    def _change_input_file_names(self):
        """
        change case_01_000101_000201_climo.nc to
               case_01_climo.nc
        """
        if self._dryrun:
            return

        # get a reference to the input directory
        input_path, _ = os.path.split(self._input_file_paths[0])
        pattern = r'\d{6}_\d{6}_'

        for input_index, input_file in enumerate(self._input_file_paths):
            _, filename = os.path.split(input_file)

            try:
                string_index = re.search(pattern, filename).start()
            except AttributeError:
                # the file is alreay in the format AMWG expects
                continue

            # chop off the last part of the string which holds the year stamp
            new_name = os.path.join(
                input_path, filename[:string_index] + 'climo.nc')
            # change the internal list
            self._input_file_paths[input_index] = new_name
            # change the name of the file
            os.rename(input_file, new_name)
    # -----------------------------------------------

    def _check_tar(self, img_source_tar, img_source, config, expected_files):
        number_missing = 0
        msg = '{prefix}: extracting images from tar archive'.format(
            prefix=self.msg_prefix())
        print_line(msg)
        call(['tar', '-xf', img_source_tar,
              '--directory', self._output_path])
        passed = True
        for item in config['diags']['amwg']['sets']:
            if item == 'all':
                continue
            setname = 'set5_6' if item == '6' or item == '5' else 'set' + item
            setpath = os.path.join(
                img_source,
                setname)
            if not os.path.exists(setpath):
                number_missing += 1
                if self._has_been_executed:
                    msg = '{prefix}: could not find output directory after inflating tar archive: {dir}'.format(
                        prefix=self.msg_prefix(),
                        dir=setpath)
                    logging.error(msg)
                    print_line(msg)
            else:
                count = len(os.listdir(setpath))
                if count < expected_files[setname]:
                    msg = '{prefix}: set {set} only produced {numProduced} when at least {numExpected} were expected'.format(
                        prefix=self.msg_prefix(),
                        set=setname,
                        numProduced=count,
                        numExpected=expected_files[setname])
                    logging.error(msg)
                    print_line(msg)
                    number_missing += 1
        return number_missing
    
    def validate(self, config):
        messages = []
        if 'job_options' in config.keys():
            amwg_global_config = config['job_options'].get('amwg')
            if amwg_global_config:
                if 'diag_home' not in amwg_global_config.keys():
                    msg = "Global job_options does not contain the amwg code path"
                    messages.append(msg)
                if not amwg_global_config.get('frequency'):
                    config['job_options']['amwg']['frequency'] = set()
                if not isinstance(amwg_global_config['frequency'], set):
                    amwg_global_config['frequency']  = set(amwg_global_config['frequency'])
                amwg_sets = amwg_global_config.get('plot_sets')
                if amwg_sets:
                    if not isinstance(amwg_sets, set):
                        amwg_sets = set(amwg_sets)
                    allowed_sets = set([str(x) for x in range(1, 17)] + ['all', '4a'])
                    for s in amwg_sets:
                        if s not in allowed_sets:
                            msg = "{} is not an allowed AMWG set".format(s)
                            messages.append(msg)

        for case in config['simulations']:
            if "amwg" not in case['jobs']:
                continue
            case_options = case['jobs']['amwg']

            freqs = case_options.get('frequency')
            if freqs:
                if not isinstance(freqs, set):
                    freqs = set(freqs)
                freqs.update(amwg_global_config['frequency'])

            case_sets = case_options.get('plot_sets')
            if case_sets:
                if not isinstance(case_sets, set):
                    case_sets = set(case_sets)
                allowed_sets = set([str(x) for x in range(1, 17)] + ['all', '4a'])
                for s in case_sets:
                    if s not in allowed_sets:
                        msg = "case {case} contains invalid AMWG set {set}".format(
                            case=case_options['shortname'], set=s)
                        messages.append(msg)
                case_sets.update(amwg_sets)
            
            if not amwg_global_config.get('output_grid_name') and not case_options.get('output_grid_name'):
                msg = 'Please specify a name for the climo output_grid_name in either the global job_options under amwg, or under the job entry in each simulation\'s config'
                messages.append(msg)
            else:
                if not case_options.get('output_grid_name'):
                    case_options['output_grid_name'] = amwg_global_config.get('output_grid_name')
            if not amwg_global_config.get('atm_map_path') and not case_options.get('atm_map_path'):
                msg = 'Please specify a atm_map_path in either the global job_options under amwg, or under the job entry in each simulation\'s config'
                messages.append(msg)
            else:
                if not case_options.get('atm_map_path'):
                    case_options['atm_map_path'] = amwg_global_config.get('atm_map_path')
        
        if messages:
            return messages
        return None

            
            
