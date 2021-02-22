#!/usr/bin/env python
from __future__ import absolute_import, division, print_function, unicode_literals

import logging
import os
import sys
import threading

from time import sleep

from processflow.lib.finalize import finalize
from processflow.lib.initialize import initialize
from processflow.lib.util import print_debug, print_line

os.environ['UVCDAT_ANONYMOUS_LOG'] = 'no'
os.environ['NCO_PATH_OVERRIDE'] = 'no'


def main(cl_args=None):
    """
    Processflow main

    Parameters:
        test (bool): turns on test mode. Simply stops the logger from reloading itself, which
            stops a crash when running from inside the test runner
        kwargs (dict): when running in test mode, arguments are passed directly through the kwargs
            which bypasses the argument parsing.
    """

    # The master configuration object
    config = {}

    # Read in parameters from config
    if cl_args is None and len(sys.argv) > 1:
        cl_args = sys.argv[1:]
    elif len(sys.argv) > 1:
        logging.debug("Both command line arguments and programmatic arguments "
                      "are present; defaulting to programmatic arguments.\n"
                      "    programmatic: main({})\n"
                      "    command line: {}".format(cl_args,
                                                    ' '.join(sys.argv[:])))

    config, runmanager = initialize(argv=cl_args)

    if isinstance(config, int):
        print_line("Error in setup, exiting", status='error')
        return -1
    logging.info('Config setup complete')
    debug = True if config['global'].get('debug') else False

    # Main loop
    loop_delay = 10
    state_path = os.path.join(
        config['global']['project_path'],
        'output',
        'job_state.txt')
    try:
        print("--------------------------")
        print(" Entering Main Loop ")
        print(" Status file: {}".format(state_path))
        print("--------------------------")
        while True:

            if debug:
                print_line(' -- checking data --')
            runmanager.check_data_ready()

            if debug:
                print_line(' -- starting ready jobs --')
            runmanager.start_ready_jobs()

            if debug:
                print_line(' -- monitoring running jobs --')
            runmanager.monitor_running_jobs(debug=debug)

            if debug:
                print_line(' -- writing out state --')
            runmanager.write_job_sets(state_path)

            status = runmanager.is_all_done()
            if status >= 0:
                msg = "Finishing up run"
                print_line(msg)
                finalize(
                    config=config,
                    status=status,
                    runmanager=runmanager)
                # SUCCESS EXIT
                return 0

            if debug:
                print_line(' -- sleeping')
            sleep(loop_delay)
    except KeyboardInterrupt as e:
        print_line('----- KEYBOARD INTERRUPT -----', status='err')
        if debug:
            import ipdb; ipdb.set_trace()
    except Exception as e:
        print_line('----- AN UNEXPECTED EXCEPTION OCCURED -----', status='err')
        print_debug(e)
    finally:
        runmanager.write_job_sets(state_path)
# -----------------------------------------------


if __name__ == "__main__":
    sys.exit(main())
# -----------------------------------------------
