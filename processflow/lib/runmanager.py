from __future__ import absolute_import, division, print_function, unicode_literals
from time import sleep

from processflow.jobs.aprime import Aprime
from processflow.jobs.timeseries import Timeseries
from processflow.jobs.amwg import AMWG
from processflow.jobs.climo import Climo
from processflow.jobs.cmor import Cmor
from processflow.jobs.diag import Diag
from processflow.jobs.e3smdiags import E3SMDiags
from processflow.jobs.mpasanalysis import MPASAnalysis
from processflow.jobs.regrid import Regrid
from processflow.jobs.ilamb import ILAMB

from processflow.lib.jobstatus import JobStatus, StatusMap, ReverseMap
from processflow.lib.serial import Serial
from processflow.lib.slurm import Slurm
from processflow.lib.util import print_line, print_debug


job_map = {
    'climo': Climo,
    'timeseries': Timeseries,
    'regrid': Regrid,
    'e3sm_diags': E3SMDiags,
    'amwg': AMWG,
    'aprime': Aprime,
    'cmor': Cmor,
    'mpas_analysis': MPASAnalysis,
    'ilamb': ILAMB
}


class RunManager(object):

    def __init__(self, config, filemanager):

        self.config = config
        self.account = config['global'].get('account', '')
        self.filemanager = filemanager
        self.dryrun = True if config['global'].get('dryrun') == True else False
        self.debug = True if config['global'].get('debug') == True else False
        self._resource_path = config['global'].get('resource_path')
        """
        A list of cases, dictionaries structured as:
            case (str): the full case name
            jobs (list): a list of job.Jobs
            short_name (str): the short name of the case
        """
        self.cases = list()

        self.running_jobs = list()
        self._job_total = 0
        self._job_complete = 0

        if config['global'].get('serial'):
            msg = '\n\n=== Running in Serial Mode ===\n'
            print_line(msg)
            self.manager = Serial()
        else:
            self.manager = Slurm()

        max_jobs = config['global'].get('max_jobs', 1)
        self.max_running_jobs = max_jobs if max_jobs else self.manager.get_node_number()
        while self.max_running_jobs == 0:
            sleep(1)
            msg = 'Unable to communication with scontrol, checking again'
            print_line(msg)
            self.max_running_jobs = self.manager.get_node_number()
    # -----------------------------------------------

    def _duplicate_check(self, job):
        """
        iterate over all the jobs and check if the input job is already in the list

        Parameters
        ----------
            job (Job): The job to check for duplicates
        Returns
        -------
            True if there is a duplicate
            False if there is NO duplicate
        """
        case = [x for x in self.cases if x['case'] == job.case].pop()
        if case:
            for other_job in case['jobs']:
                if job.job_type == other_job.job_type \
                        and job.start_year == other_job.start_year \
                        and job.end_year == other_job.end_year:
                    if job.run_type:
                        if job.run_type == other_job.run_type:
                            return True
                    else:
                        if job.comparison and job.comparison == other_job.comparison:
                            return True
            return False
    # -----------------------------------------------

    def add_pp_type_to_cases(self, freqs, job_type, start, end, case, run_type=None):
        """
        Add post processing jobs to the case.jobs list

        Parameters:
            freqs (list, int, None): the year length frequency to add this job
            job_type (str): what type of job to add
            start (int): the first year of simulated data
            end (int): the last year of simulated data
            data_type (str): what type of data to run this job on (regrid atm or lnd only)
            case (dict): the case to add this job to
            """
        if not freqs:
            freqs = end - start + 1
        if not isinstance(freqs, list):
            freqs = [freqs]

        for year in range(start, end + 1):
            for freq in freqs:
                freq = int(freq)
                if (year - start) % freq == 0:
                    job_end = year + freq - 1
                    if job_end > end:
                        job_end = end
                    new_job = job_map[job_type](
                        short_name=case['short_name'],
                        case=case['case'],
                        start=year,
                        end=job_end,
                        dryrun=self.config['global'].get('dryrun'),
                        run_type=run_type,
                        config=self.config,
                        manager=self.manager)
                    if not self._duplicate_check(new_job):
                        case['jobs'].append(new_job)
    # -----------------------------------------------

    def add_diag_type_to_cases(self, freqs, job_type, start, end, case):
        """
        Add diagnostic jobs to the case.jobs list

        Parameters:
            freqs (list): a list of year lengths to add this job for
            job_type (str): the name of the job type to add
            start (int): the first year of simulated data
            end (int): the last year of simulated data
            case (dict): the case to add this job to
        """
        if not isinstance(freqs, list):
            freqs = [freqs]

        case_name = case['case']

        for year in range(start, end + 1):
            for freq in freqs:
                freq = int(freq)
                if (year - start) % freq == 0:

                    # get the comparisons from the config
                    comparisons = self.config['simulations'][case_name].get(
                        'comparisons')
                    if not comparisons:
                        continue
                    if not isinstance(comparisons, list):
                        comparisons = [comparisons]

                    if job_type in ['aprime', 'mpas_analysis']:
                        comparisons = ['obs']
                    job_end = year + freq - 1
                    if job_end > end:
                        job_end = end
                    # for each comparison, add a job to this case
                    for item in comparisons:
                        if item == 'all':
                            for other_case in self.config['simulations']:

                                if other_case in ['start_year', 'end_year', case_name]:
                                    continue

                                new_diag = job_map[job_type](
                                    short_name=case['short_name'],
                                    case=case_name,
                                    start=year,
                                    end=job_end,
                                    comparison=other_case,
                                    config=self.config,
                                    dryrun=self.config['global'].get('dryrun'),
                                    manager=self.manager)
                                if not self._duplicate_check(new_diag):
                                    case['jobs'].append(new_diag)
                            new_diag = job_map[job_type](
                                short_name=case['short_name'],
                                case=case_name,
                                start=year,
                                end=job_end,
                                comparison='obs',
                                config=self.config,
                                dryrun=self.config['global'].get('dryrun'),
                                manager=self.manager)
                            if not self._duplicate_check(new_diag):
                                case['jobs'].append(new_diag)
                        else:
                            new_diag = job_map[job_type](
                                short_name=case['short_name'],
                                case=case_name,
                                start=year,
                                end=job_end,
                                dryrun=self.config['global'].get('dryrun'),
                                comparison=item,
                                config=self.config,
                                manager=self.manager)
                            if not self._duplicate_check(new_diag):
                                case['jobs'].append(new_diag)
    # -----------------------------------------------

    def setup_cases(self):
        """
        Setup each case with all the jobs it will need
        """
        start = self.config['simulations']['start_year']
        end = self.config['simulations']['end_year']
        for case in self.config['simulations']:
            if case in ['start_year', 'end_year']:
                continue
            self.cases.append({
                'case': case,
                'short_name': self.config['simulations'][case]['short_name'],
                'jobs': list()
            })

        pp = self.config.get('post-processing')
        if pp:
            for key, val in list(pp.items()):
                cases_to_add = list()
                for case in self.cases:
                    if not self.config['simulations'][case['case']].get('job_types'):
                        continue
                    if 'all' in self.config['simulations'][case['case']]['job_types'] or key in self.config['simulations'][case['case']]['job_types']:
                        cases_to_add.append(case)
                if key in ['regrid', 'timeseries']:
                    for dtype in val:
                        if dtype not in self.config['data_types']:
                            continue
                        for case in cases_to_add:
                            if 'all' in self.config['simulations'][case['case']]['data_types'] or dtype in self.config['simulations'][case['case']]['data_types']:
                                self.add_pp_type_to_cases(
                                    freqs=val.get('run_frequency'),
                                    job_type=key,
                                    start=start,
                                    end=end,
                                    run_type=dtype,
                                    case=case)
                elif key == 'cmor':
                    for case in cases_to_add:
                        for table in ['Amon', 'Lmon', 'SImon', 'Omon']:
                            if self.config['post-processing']['cmor'].get(table):
                                self.add_pp_type_to_cases(
                                    freqs=val.get('run_frequency'),
                                    job_type=key,
                                    start=start,
                                    end=end,
                                    run_type=table,
                                    case=case)
                else:
                    for case in cases_to_add:
                        self.add_pp_type_to_cases(
                            freqs=val.get('run_frequency'),
                            job_type=key,
                            start=start,
                            end=end,
                            case=case)
        diags = self.config.get('diags')
        if diags:
            for key, val in list(diags.items()):
                # cases_to_add = list()
                for case in self.cases:
                    if not self.config['simulations'][case['case']].get('job_types'):
                        continue

                    if 'all' in self.config['simulations'][case['case']]['job_types'] or key in self.config['simulations'][case['case']]['job_types']:
                        self.add_diag_type_to_cases(
                            freqs=diags[key]['run_frequency'],
                            job_type=key,
                            start=start,
                            end=end,
                            case=case)

        self._job_total = 0
        for case in self.cases:
            self._job_total += len(case['jobs'])
    # -----------------------------------------------

    def setup_jobs(self):
        """
        Setup the dependencies for each job in each case
        """
        for case in self.cases:
            for job in case['jobs']:
                if job.comparison != 'obs':
                    other_case, = [
                        case for case in self.cases if case['case'] == job.comparison]
                    job.setup_dependencies(
                        jobs=case['jobs'],
                        comparison_jobs=other_case['jobs'])
                else:
                    job.setup_dependencies(
                        jobs=case['jobs'])
    # -----------------------------------------------

    def check_data_ready(self):
        """
        Loop over all jobs, checking if their data is ready, and setting
        the internal job.data_ready variable
        """
        for case in self.cases:
            for job in case['jobs']:
                job.check_data_ready(self.filemanager)
    # -----------------------------------------------

    def start_ready_jobs(self):
        """
        Loop over the list of jobs for each case, first setting up the data for, and then
        submitting each job to the queue
        """
        for case in self.cases:
            for job in case['jobs']:
                if job.status != JobStatus.VALID:
                    continue
                if len(self.running_jobs) >= self.max_running_jobs:
                    msg = 'running {} of {} jobs, waiting for queue to shrink'.format(
                        len(self.running_jobs), self.max_running_jobs)
                    if self.debug:
                        print_line(msg)
                    return
                deps_ready = True
                for depjobid in job.depends_on:
                    depjob = self.get_job_by_id(depjobid)
                    if depjob.status != JobStatus.COMPLETED:
                        deps_ready = False
                        break

                # if 'ilamb' in job.msg_prefix():
                #     import ipdb; ipdb.set_trace()
                job.check_data_ready(self.filemanager)
                if deps_ready and job.data_ready:

                    # if the job was finished by a previous run of the processflow

                    if job.postvalidate(self.config):
                        job.status = JobStatus.COMPLETED
                        self._job_complete += 1
                        job.handle_completion(
                            filemanager=self.filemanager,
                            config=self.config)
                        self.report_completed_job()
                        continue

                    # set to pending before data setup so we dont double submit
                    job.status = JobStatus.PENDING

                    # setup the data needed for the job
                    job.setup_data(
                        config=self.config,
                        filemanager=self.filemanager,
                        case=job.case)
                    # if this job needs data from another case, set that up too
                    if isinstance(job, Diag):
                        if job.comparison != 'obs':
                            job.setup_data(
                                config=self.config,
                                filemanager=self.filemanager,
                                case=job.comparison)

                    # get the instances of jobs this job is dependent on
                    dep_jobs = [self.get_job_by_id(
                        job_id) for job_id in job._depends_on]
                    run_id = job.execute(
                        config=self.config,
                        dryrun=self.dryrun,
                        depends_jobs=dep_jobs)
                    self.running_jobs.append({
                        'manager_id': run_id,
                        'job_id': job.id
                    })
                    if run_id == 0:
                        job.status = JobStatus.COMPLETED
                        self.monitor_running_jobs()
                        
    # -----------------------------------------------

    def get_job_by_id(self, jobid):
        for case in self.cases:
            for job in case['jobs']:
                if job.id == jobid:
                    return job
        raise Exception(f"no job with id {jobid} found")
    # -----------------------------------------------

    def write_job_sets(self, path):
        out_str = ''
        with open(path, 'w') as fp:
            for case in self.cases:
                out_str += '\n==' + '='*len(case['case']) + '==\n'
                out_str += '# {} #\n'.format(case['case'])
                out_str += '==' + '='*len(case['case']) + '==\n'
                for job in case['jobs']:
                    out_str += '\n\tname: ' + job.job_type
                    out_str += '\n\tperiod: {:04d}-{:04d}'.format(
                        job.start_year, job.end_year)
                    if job._run_type:
                        out_str += '\n\trun_type: ' + job._run_type
                    out_str += '\n\tstatus: ' + job.status.name
                    deps_jobs = [self.get_job_by_id(x) for x in job.depends_on]
                    if deps_jobs:
                        out_str += '\n\tdependent_on: ' + str(
                            ['{}'.format(x.msg_prefix()) for x in deps_jobs])
                    out_str += '\n\tdata_ready: ' + str(job.data_ready)
                    out_str += '\n\tprocessflow_id: ' + job.id
                    out_str += '\n\tmanager_id: ' + str(job.job_id)
                    if case['jobs'].index(job) != len(case['jobs']) - 1:
                        out_str += '\n------------------------------------'
                    else:
                        out_str += '\n'
            fp.write(out_str)
    # -----------------------------------------------

    def _precheck(self, year_set, jobtype, data_type=None):
        """
        Check that the jobtype for that given yearset isnt
        already in the job_list

        Parameters:
            set_number (int): the yearset number to check
            jobtype (str): the type of job to check for
        Returns:
            1 if the job/yearset combo are NOT in the job_list
            0 if they are
        """
        for job in year_set.jobs:
            if job.type == jobtype:
                if job.type != 'regrid':
                    return False
                else:  # regrid is the only job type that can have multiple instances in a year_set
                    if job.data_type == data_type:  # but only one instance per data type
                        return False
        return True
    # -----------------------------------------------

    def report_completed_job(self):
        msg = f'Job progress: {self._job_complete}/{self._job_total} or {self._job_complete * 1.0 / self._job_total * 100:.2f}%\n'
        print_line(msg)
    # -----------------------------------------------

    def monitor_running_jobs(self, debug=False):
        """
        Lookup job status for all current jobs, 
        start jobs that are ready, 
        run post-completion handlers for any jobs that have failed or completed.

        Any new jobs that are started are added to the self.running_jobs list
        """
        for_removal = list()
        for item in self.running_jobs:
            # each item is a mapping of job UUIDs to the id given by the resource manager
            job = self.get_job_by_id(item['job_id'])

            # if the job ID is 0 it means it was previously run
            if item['manager_id'] == 0:
                self._job_complete += 1
                for_removal.append(item)
                job.handle_completion(
                    filemanager=self.filemanager,
                    config=self.config)
                self.report_completed_job()
                continue
            try:
                job_info = self.manager.showjob(item['manager_id'])
                if job_info.state is None:
                    continue
            except Exception as e:
                
                print_debug(e)
                # if the job is old enough it wont be in the slurm list anymore
                # which will throw an exception
                self._job_complete += 1
                for_removal.append(item)

                if job.postvalidate(self.config):
                    job.status = JobStatus.COMPLETED
                    job.handle_completion(
                        filemanager=self.filemanager,
                        config=self.config)
                    self.report_completed_job()
                else:
                    job.status = JobStatus.FAILED
                    line = f"{job.msg_prefix()}: resource manager lookup error for jobid {item['manager_id']}. The job may have failed, check the error output"
                    print_line(line)
                continue

            status = StatusMap[job_info.state]
            if debug:
                print(str(job_info))
            if status != job.status:
                msg = '{prefix}: Job changed from {s1} to {s2}'.format(
                    prefix=job.msg_prefix(),
                    s1=ReverseMap[job.status],
                    s2=ReverseMap[status])
                print_line(msg)
                job.status = status

                if job.status == JobStatus.FAILED:
                    msg = f'Job has failed, check the job output here: {job.get_output_path()}\n'
                    print_line(msg, status='error')

                if status in [JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED]:
                    self._job_complete += 1

                    if not job.postvalidate(self.config):
                        job.status = JobStatus.FAILED
                        status = JobStatus.FAILED
                        msg = f'Job has failed, check the job output here: {job.get_output_path()}\n'
                        print_line(msg, status='error')
                    else:
                        job.handle_completion(
                            filemanager=self.filemanager,
                            config=self.config)
                    self.report_completed_job()
                    for_removal.append(item)
                    if status in [JobStatus.FAILED, JobStatus.CANCELLED]:
                        for depjob in self.get_jobs_that_depend(job.id):
                            depjob.status = JobStatus.FAILED
        if for_removal:
            self.running_jobs = [
                x for x in self.running_jobs if x not in for_removal]
        return
    # -----------------------------------------------

    def get_jobs_that_depend(self, job_id):
        """
        returns a list of all jobs that depend on the give job
        """
        jobs = list()
        for case in self.cases:
            for job in case['jobs']:
                for depid in job.depends_on:
                    if depid == job_id:
                        jobs.append(job)
        return jobs
    # -----------------------------------------------

    def is_all_done(self):
        """
        Check if all jobs are done, and all processing has been completed

        return -1 if still running
        return 0 if a job failed
        return 1 if all complete
        """
        if len(self.running_jobs) > 0:
            return -1

        failed = False
        for case in self.cases:
            for job in case['jobs']:
                if job.status in [JobStatus.VALID, JobStatus.PENDING, JobStatus.RUNNING]:
                    return -1
                if job.status in [JobStatus.FAILED, JobStatus.CANCELLED]:
                    failed = True
        if failed:
            return 0
        return 1
    # -----------------------------------------------
