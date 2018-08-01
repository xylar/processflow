import os
import logging
from time import sleep
from subprocess import Popen, PIPE


class PBS(object):
    """
    A Python class for interfacing with the PBS/TORQUE resource management system
    """

    def __init__(self):
        """
        Check if the system has PBS available, raise an exception if it cant be found
        """
        if not any(os.access(os.path.join(path, 'qstat'), os.X_OK) for path in os.environ["PATH"].split(os.pathsep)):
            raise Exception(
                'Unable to find qstat, is PBS installed on this sytem?')
    
    def batch(self, cmd, pargs=None):
        """
        Submit to the batch queue in non-interactive mode

        Parameters:
            cmd (str): The path to the run script that should be submitted
            sargs (str): The additional arguments to pass to pbs
        Returns:
            job id of the new job (int)
        """
        pass
    
    def showjob(self, jobid):
        """
        A wrapper around 'qstat -f JOBID' to get job info

        Parameters:
            jobid (str): the job id to get information about
        Returns:
            A dictionary of information provided by PBS about the job
        """
