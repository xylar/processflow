import json


class JobInfo(object):
    """
    A simple container class for slurm and pbs job information
    """
    def __init__(self, jobid=None, 
                       jobname=None, 
                       partition=None, 
                       state=None, 
                       time=None, 
                       user=None, 
                       command=None):
        self.jobid = jobid
        self.jobname = jobname
        self.partition = partition
        self.state = state
        self.time = time
        self.user = user
        self.command = command

    def __str__(self):
        return json.dumps({
            'JOBID': self.jobid,
            'JOBNAME': self.jobname,
            'PARTITION': self.partition,
            'STATE': self.state,
            'TIME': self.time,
            'USER': self.user
        })

    def set_attr(self, attr, val):
        """
        set the appropriate attribute with the value supplied
        """
        if attr == 'PARTITION':
            self.partition = val
        elif attr == 'COMMAND':
            self.command = val    
        elif attr == 'NAME':
            self.jobname = val
        elif attr == 'JOBID':
            self.jobid = val
        elif attr == 'STATE':
            self.state = val
        elif attr == 'RUNTIME':
            self.time = val
        elif attr == 'USER':
            self.user = val
        else:
            msg = '{} is not an allowed attribute'.format(attr)
            raise Exception(msg)
