from __future__ import absolute_import, division, print_function, unicode_literals
import logging
import os
from shutil import rmtree

from processflow.lib.jobstatus import JobStatus
from processflow.lib.mailer import Mailer
from processflow.lib.util import print_line, print_debug, ncrcat


def concat_cmor(config, runmanager):
    for case in runmanager.cases:
        msg = f"Starting CMOR file concatination for {case['case']}"
        print_line(msg)
        for job in case['jobs']:
            if job.status == JobStatus.COMPLETED and job.job_type == 'cmor':
                # ok, now that we've got a cmor job, walk the output dir and on each leaf run ncrcat
                for root, dirs, files in os.walk(job.output_path):
                    if dirs or not files:
                        continue
                    non_nc = False
                    for file in files:
                        if not file.endswith('.nc'):
                            non_nc = True
                            break
                    if non_nc:
                        continue
                    to_concat = [os.path.join(root, x) for x in files]
                    outpath = ncrcat(to_concat)
                    if not outpath or not Path(outpath).exists():
                        msg = f"Error running ncrcat for CMOR output files {root}"
                        print_line(msg, status='err')
                        continue
                    for file in files:
                        os.remove(file)



def finalize(config, status, runmanager):

    if config.get('post-processing'):
        cmor = config['post-processing'].get('cmor')
        if cmor and cmor.get('concatenate') in [1, '1', 'true', True]:
            concat_cmor(config, runmanager)

    if status == 1:
        msg = 'All processing complete'
        code = 'ok'
    else:
        msg = 'The following jobs encountered an error and were marked as failed:'
        code = 'error'
        for case in runmanager.cases:
            for job in case['jobs']:
                if job.status != JobStatus.COMPLETED:
                    msg += '\n        {}'.format(job.msg_prefix())
    print_line(msg, status=code)
    emailaddr = config['global'].get('email')
    if emailaddr:
        message = 'Sending notification email to {}'.format(emailaddr)
        print_line(message, status='ok')
        try:
            if status == 1:
                msg = 'Your processflow run has completed successfully\n'
                status = msg
            else:
                msg = 'One or more processflow jobs failed\n'
                status = msg
                msg += 'See log for additional details\n{}\n'.format(
                    config['global']['log_path'])

            for case in runmanager.cases:
                msg += '==' + '='*len(case['case']) + '==\n'
                msg += ' # ' + case['case'] + ' #\n'
                msg += '==' + '='*len(case['case']) + '==\n\n'
                for job in case['jobs']:
                    msg += '\t > ' + job.get_report_string() + '\n'
                msg += '\n'

            m = Mailer(src='processflowbot@llnl.gov', dst=emailaddr)
            m.send(
                status=status,
                msg=msg)
        except Exception as e:
            print_debug(e)

    logging.info("All processes complete")
# -----------------------------------------------
