import os
from acme_diags.parameter.core_parameter import CoreParameter
from acme_diags.parameter.area_mean_time_series_parameter import AreaMeanTimeSeriesParameter
from acme_diags.run import runner

param = CoreParameter()

machine_path_prefix = '{{ machine_path_prefix }}'

param.reference_data_path = os.path.join(machine_path_prefix, 'obs_for_e3sm_diags/climatology/')
param.test_data_path = '{{ ts_test_data_path }}'
param.test_name = '{{ test_name }}'
param.results_dir = '{{ results_path }}'
param.test_timeseries_input = True
param.test_start_yr = '{{ ts_start }}'
param.test_end_yr = '{{ ts_end }}'
param.multiprocessing = True
param.num_workers =  {{ num_workers }}

ts_param = AreaMeanTimeSeriesParameter()
ts_param.ref_names = []   #This setting plot model data only
ts_param.start_yr = '{{ ts_start }}'
ts_param.end_yr = '{{ ts_end }}'
ts_param.reference_data_path = os.path.join(machine_path_prefix, 'obs_for_e3sm_diags/time-series/')
ts_param.test_data_path = '{{ ts_test_data_path }}'

{{ custom_params }}

runner.sets_to_run = ['{{ sets_to_run }}']
runner.run_diags([param, ts_param])


