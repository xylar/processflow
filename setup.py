import sys
from setuptools import find_packages, setup

data_files = [(sys.prefix + '/share/processflow/resources',
                ['resources/amwg_template_vs_model.csh',
                'resources/amwg_template_vs_obs.csh',
                'resources/aprime_template_vs_obs.bash',
                'resources/e3sm_diags_template_vs_model.py',
                'resources/e3sm_diags_template_vs_obs.py'])]

setup(
    name="acme_processflow",
    version="2.0.0",
    author="Sterling Baldwin",
    author_email="baldwin32@llnl.gov",
    description="ACME Automated Processflow for handling post processing jobs for raw model data",
    scripts=["processflow.py"],
    packages=find_packages(
        exclude=["*.test", "*.test.*", "test.*", "test", "*_template.py"]),
    data_files=data_files)
