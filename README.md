# E3SM Automated Workflow

The processflow performs post processing and diagnostics jobs automatically, removing many of the difficulties of performing 
post-run analysis on model data. 

[Documentation can be found here](https://e3sm-project.github.io/processflow/docs/html/index.html)

## Installation<a name="installation"></a>

```
conda create --name <YOUR_NEW_ENVIRONMENT> -c acme -c conda-forge -c uvcdat processflow
```

# Usage<a name="usage"></a>

        usage: processflow.py [-h] [-c CONFIG] [-v] [-l LOG] [-s] [-f]
                      [-r RESOURCE_PATH] [-a] [-d] [--dryrun] [-m MAX_JOBS]

        optional arguments:
        -h, --help            show this help message and exit
        -c CONFIG, --config CONFIG
                                Path to configuration file.
        -v, --version         Print version informat and exit.
        -l LOG, --log LOG     Path to logging output file.
        -s, --scripts         Copy the case_scripts directory from the remote
                                machine.
        -f, --file-list       Turn on debug output of the internal file_list so you
                                can see what the current state of the model files are
        -r RESOURCE_PATH, --resource-path RESOURCE_PATH
                                Path to custom resource directory
        -a, --always-copy     Always copy diagnostic output, even if the output
                                already exists in the host directory. This is much
                                slower but ensures old output will be overwritten
        -d, --debug           Set log level to debug
        --dryrun              Do everything up to starting the jobs, but dont start
                                any jobs
        -m MAX_JOBS, --max-jobs MAX_JOBS
                                maximum number of running jobs
