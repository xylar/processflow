# E3SM Automated Workflow

The processflow performs post processing and diagnostics jobs automatically, removing many of the difficulties of performing 
post-run analysis on model data. 

[Documentation can be found here](https://e3sm-project.github.io/processflow/docs/html/index.html)

## Installation<a name="installation"></a>

Latest stable build from the master branch:
```
conda create --name <YOUR_NEW_ENVIRONMENT> -c e3sm -c conda-forge -c cdat processflow
```

Latest (potentially unstable but with the latest bug fixes) build from the nightly branch:
```
conda create --name <YOUR_NEW_ENVIRONMENT> -c conda-forge -c e3sm/label/nightly -c e3sm -c cdat -c pcmdi processflow
```

Update your environment to the newest version
```
conda update -c e3sm -c conda-forge -c cdat processflow
```


# Usage<a name="usage"></a>

        usage: processflow [-h] [-m MAX_JOBS] [-l LOG] [-a] [-r RESOURCE_PATH]
                   [--debug] [--dryrun] [-v] [-s]
                   [config]

        positional arguments:
        config                Path to configuration file.

        optional arguments:
        -h, --help            show this help message and exit
        -m MAX_JOBS, --max-jobs MAX_JOBS
                                Maximum number of jobs to run at any given time
        -l LOG, --log LOG     Path to logging output file, defaults to
                                project_path/output/processflow.log
        -a, --always-copy     Always copy diagnostic output, even if the output
                                already exists in the host directory. This is much
                                slower but ensures old output will be overwritten
        -r RESOURCE_PATH, --resource-path RESOURCE_PATH
                                Path to custom resource directory
        --debug               Set log level to debug
        --dryrun              Do everything up to starting the jobs, but dont start
                                any jobs
        -v, --version         Print version information and exit.
        -s, --serial          Run in serial on systems without a resource manager
