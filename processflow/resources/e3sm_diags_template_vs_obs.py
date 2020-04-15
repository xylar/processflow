import os
from acme_diags.parameter.core_parameter import CoreParameter
from acme_diags.run import runner

param = CoreParameter()

machine_path_prefix = '{{ machine_path_prefix }}'

param.reference_data_path = os.path.join(machine_path_prefix, 'obs_for_e3sm_diags/climatology/')
param.results_dir = '{{ results_dir }}'
param.test_data_path = '{{ test_data_path }}'
param.test_name = '{{ test_name }}'

param.multiprocessing = True
param.num_workers =  {{ num_workers }}

{{ custom_params }}

runner.sets_to_run = ['{{ sets_to_run }}']
runner.run_diags([param])
