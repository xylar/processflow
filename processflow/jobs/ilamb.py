"""
A wrapper class around ILAMB
"""
import os
import shutil
import errno
import glob
import re
import logging

from processflow.jobs.diag import Diag
from processflow.lib.util import render, print_line, format_debug
from processflow.lib.jobstatus import JobStatus


class ILAMB(Diag):
    def __init__(self, *args, **kwargs):
        """
        Parameters
        ----------
            config (dict): the global configuration object
            custom_args (dict): a dictionary of user supplied arguments
                to pass to the resource manager
        """
        super(ILAMB, self).__init__(*args, **kwargs)

        config = kwargs['config']
        self._job_type = 'ilamb'
        self._requires = ['cmor']
        self._host_url = ''
        self._variables = config['diags']['ilamb']['variables']
        self._data_required = [f'cmorized-{var}' for var in self._variables]

        self._run_name = f'{self.start_year:04d}_{self.end_year:04d}_vs_{self._short_comp_name}'
        if config['global']['host']:
            self._host_path = os.path.join(
                config['img_hosting']['host_directory'],
                self.short_name,
                'ilamb',
                self._run_name)
            self._host_url = 'https://{server}/{prefix}/{case}/ilamb/{start:04d}_{end:04d}_vs_{comp}/index.html'.format(
                server=config['img_hosting']['img_host_server'],
                prefix=config['img_hosting']['url_prefix'],
                case=self.short_name,
                start=self.start_year,
                end=self.end_year,
                comp=self._short_comp_name)
        else:
            self._host_path = os.path.join(
                'html', 
                'ilamb', 
                self.short_name,
                self._run_name)
        if not os.path.exists(self._host_path):
            os.makedirs(self._host_path)

        custom_args = config['diags'][self.job_type].get(
            'custom_args')
        if custom_args:
            self.set_custom_args(custom_args)


        self._output_path = os.path.join(
            config['global']['project_path'],
            'output',
            'diags',
            self.short_name,
            'ilamb',
            self._run_name)
        os.makedirs(self._output_path, exist_ok=True)
        
        self._input_base_path = os.path.join(
            self._output_path,
            'MODELS',
            self.short_name)
        os.makedirs(self._input_base_path, exist_ok=True)
    

    def _dep_filter(self, job):
        """
        find the CMOR job we're waiting for, assuming there's only
        one CMOR job in this case with the same start and end years
        """
        
        if not hasattr(job, 'variables'):
            return False

        if self._comparison == 'obs':
            if job.case != self.case:
                return False
            if job.job_type not in self._requires:
                return False
            if job.start_year != self.start_year:
                return False
            if job.end_year != self.end_year:
                return False
            
            for var in self._variables:
                if var in job.variables:
                    return True
                    
            return False

    def setup_dependencies(self, jobs, *args, **kwargs):
        """
        Adds CMOR jobs from this or the comparison case to the list of
        dependent jobs
        Parameters
        ----------
            jobs (list): a list of the rest of the run managers jobs
            optional: comparison_jobs (list): if this job is being compared to
                another case, the cmorized output for that other case has to
                be done already too
        """
        if self.comparison != 'obs':
            other_jobs = kwargs['comparison_jobs']
            try:
                cmor_jobs = filter(lambda job: self._dep_filter(job),
                                    other_jobs)
            except ValueError:
                msg = f'Unable to find CMOR for {self.msg_prefix()}, is this case set to ' \
                      'generate CMORized output?'
                raise Exception(msg)
        else:
            try:
                
                cmor_jobs = filter(lambda job: self._dep_filter(job), jobs)
            except ValueError:
                msg = f'Unable to find CMOR for {self.msg_prefix()}, is this case set to ' \
                      'generate CMORized output?'
                raise Exception(msg)
        for job in cmor_jobs:
            self.depends_on.append(job.id)

    def setup_data(self, config, filemanager, case):
        """
        Copy all data_types specified in the jobs _data_required field,
        and appends the path to the copies into the _input_file_paths list
        """
        # loop over the data types, linking them in one at a time
        for datatype in self._data_required:

            files = filemanager.get_file_paths_by_year(
                datatype, case, self.start_year, self.end_year)

            if not files or len(files) == 0:
                msg = f"{self.msg_prefix()}: filemanager can't find input files for " \
                       "datatype {datatype}"
                print_line(msg, status='error')
                continue

            for file_ in files:
                _, name = os.path.split(file_)
                destination = os.path.join(
                    self._input_base_path,
                    name)
                if os.path.exists(destination) or os.path.lexists(destination):
                    continue
                # keep a reference to the input data for later
                self._input_file_paths.append(destination)
                try:
                    # from shutil import copyfile
                    # copyfile(file_, destination)
                    os.symlink(file_, destination)
                except IOError as e:
                    msg = format_debug(e)
                    logging.error(msg)

    def generate_config(self, resource_path, config_path):
        variable_groups = {
            'Ecosystem and Carbon Cycle': {
                "bgcolors": "#ECFFE6",
                "variables": [ "cVeg", "burntArea", "co2", 'lai', 'gpp', 'nbp', 'nee', "reco", "cSoil" ],
                "prefix-written": False
            }, 
            'Hydrology Cycle': {
                "bgcolors": "#E6F9FF",
                "variables": [ 'hfls', 'hfss', 'tsl', 'evspsbl', "EvapFrac", "mrro", "twsa", ],
                "prefix-written": False
            },
            'Radiation and Energy Cycle': {
                "bgcolors": "#FFECE6",
                "variables":[ "rsus", "rlus", "albedo", "rsns", "rlns", "rns" ],
                "prefix-written": False
            },
            'Forcings': {
                "bgcolors": "#EDEDED",
                "variables": [ "tas", "pr", "rsds", "rlds", "hurs" ],
                "prefix-written": False
            }
        }


        if os.path.exists(config_path):
            os.remove(config_path)

        with open(config_path, 'w') as op:

            for group in variable_groups:
                for var in variable_groups[group]['variables']:
                    if var not in self._variables:
                        continue

                    config_template = os.path.join(resource_path, f'ilamb.{var}.cfg')
                    if not os.path.exists(config_template):
                        raise ValueError(f'No template exists for variable {var} in ' \
                                         f'the processflow resources directory {resource_path}')

                    if not variable_groups[group]['prefix-written']:
                        variable_groups[group]['prefix-written'] = True
                        op.write(f"""\n#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
[h1: {group}]
bgcolors: "{variable_groups[group]['bgcolors']}"

""")
                    # write out the info for the requested variable
                    with open(config_template, 'r') as ip:
                        for line in ip.readlines():
                            op.write(line)

    def execute(self, config, event_list, depends_jobs, *args, slurm_args=None, dryrun=False, **kwargs):
        """
        Generates and submits a run script for ILAMB
        Parameters
        ----------
            config (dict): the globus processflow config object
            dryrun (bool): a flag to denote that all the data should be set,
                and the scripts generated, but not actually submitted
        """
        self._dryrun = dryrun
        ilamb_config = config['diags']['ilamb']

        # setup template
        resource_path = config['global']['resource_path']
        config_path = os.path.join(self.output_path, 'ilamb.cfg')
        self.generate_config(
            resource_path, config_path)

        # ILAMB wants the MODELS directory
        model_root, _ = os.path.split(self._input_base_path)

        cmd = [f'export ILAMB_ROOT={ilamb_config["obs_data_root"]}\n',
               'ilamb-run',
               '--config', config_path,
               '--model_root', model_root,
               '--models', self.short_name,
               '--build_dir', self._host_path,
               '--title', f'{self.short_name}_vs_{self._comparison}']

        if ilamb_config.get('confrontation'):
            cmd.extend(['--confrontation',
                        ' '.join(list(ilamb_config['confrontation']))])

        if ilamb_config.get('shift_year_to'):
            shift_start = self.start_year
            shift_end = ilamb_config['shift_year_to']
            cmd.extend(['--model_year', f'{shift_start} {shift_end}'])

        if ilamb_config.get('regions'):
            cmd.extend(['--regions', ' '.join(list(ilamb_config['regions']))])

        if ilamb_config.get('region_definition_files'):
            cmd.extend(['--define_regions',
                        ' '.join(list(ilamb_config['region_definition_files']))])
        if ilamb_config.get('clean') in [1, '1', 'true', 'True']:
            cmd.append('--clean')

        if ilamb_config.get('disable_logging') in [1, '1', 'true', 'True']:
            cmd.append('--disable_logging')
        

        if os.path.exists(self._host_path):
            shutil.rmtree(self._host_path)

        self._has_been_executed = True
        return self._submit_cmd_to_manager(config, cmd, event_list)

    def postvalidate(self, config, *args, **kwargs):
        """
        Validates that the diagnostic produced its expected output
        Parameters
        ----------
            config (dict): the global config object
        Returns
        -------
            True if all output exists as expected
            False otherwise
        """
        # if the job ran through slurm can came back complete, just return True
        if self.status == JobStatus.COMPLETED:
            return True
        elif self.status == JobStatus.FAILED:
            return False
        # otherwise, maybe this job hasnt been run yet in this instance, but the
        # output might be there
        else:
            logs = glob.glob(os.path.join(self._host_path, 'ILAMB*.log'))
            if not logs or not os.path.exists(logs[0]):
                return False
            else:
                with open(logs[0], 'r') as ip:
                    for line in ip.readlines():
                        if re.search('error', line, re.IGNORECASE):
                            return False
                return True

    def handle_completion(self, filemanager, event_list, config, *args, **kwargs):
        print('\n')