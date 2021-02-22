"""
A module to verify that the user config is valid
"""
from __future__ import absolute_import, division, print_function, unicode_literals
import os
import socket

def verify_config(config):
    messages = list()
    # ------------------------------------------------------------------------
    # check that each mandatory section exists
    # ------------------------------------------------------------------------
    if not config.get('simulations'):
        msg = 'No simulations section found in config'
        messages.append(msg)
    if not config.get('global'):
        msg = 'No global section found in config'
        messages.append(msg)
    else:
        if not config['global'].get('project_path'):
            msg = 'no project_path in global options'
            messages.append(msg)
    if not config.get('data_types'):
        msg = 'No data_types section found in config'
        messages.append(msg)
    if messages:
        return messages
    # ------------------------------------------------------------------------
    # check simulations
    # ------------------------------------------------------------------------
    time_error = False
    if not config['simulations'].get('start_year'):
        msg = 'no start_year set for simulations'
        messages.append(msg)
        time_error = True
    else:
        config['simulations']['start_year'] = int(
            config['simulations']['start_year'])
    if not config['simulations'].get('end_year'):
        msg = 'no end_year set for simulations'
        messages.append(msg)
        time_error = True
    else:
        config['simulations']['end_year'] = int(
            config['simulations']['end_year'])
    if not time_error:
        if int(config['simulations'].get('end_year')) < int(config['simulations'].get('start_year')):
            msg = 'simulation end_year is less then start_year, is time going backwards!?'
            messages.append(msg)

    sim_names = []
    for sim in config.get('simulations'):
        if sim in ['start_year', 'end_year']:
            continue
        sim_names.append(sim)
        if config['simulations'][sim].get('comparisons'):
            if not isinstance(config['simulations'][sim]['comparisons'], list):
                config['simulations'][sim]['comparisons'] = [
                    config['simulations'][sim]['comparisons']]
        if not config['simulations'][sim].get('local_path'):
            config['simulations'][sim]['local_path'] = os.path.join(
                config['global']['project_path'],
                'input',
                sim)
        if not config['simulations'][sim].get('data_types'):
            msg = 'no data_types found for {}, set to \'all\' to select all types, or list only data_types desired'.format(
                sim)
            messages.append(msg)
            continue
        if not isinstance(config['simulations'][sim]['data_types'], list):
            config['simulations'][sim]['data_types'] = [
                config['simulations'][sim]['data_types']]
        for data_type in config['simulations'][sim]['data_types']:
            if data_type == 'all':
                continue
            if data_type not in config['data_types']:
                msg = '{} is set to use data_type {}, but this data type is not in the data_types config option'.format(
                    sim, data_type)
                messages.append(msg)
        if config['simulations'][sim].get('job_types'):
            if not isinstance(config['simulations'][sim]['job_types'], list):
                config['simulations'][sim]['job_types'] = [
                    config['simulations'][sim]['job_types']]
            for job_type in config['simulations'][sim]['job_types']:
                if job_type == 'all':
                    continue
                if job_type not in config.get('post-processing', []) and job_type not in config.get('diags', []):
                    msg = '{} is set to run job {}, but this run type is not in either the post-processing or diags config sections'.format(
                        sim, job_type)
                    messages.append(msg)
        else:
            msg = 'No job_types given for {}'.format(sim)
            messages.append(msg)
            return messages

    # ------------------------------------------------------------------------
    # check data_types
    # ------------------------------------------------------------------------
    for ftype in config.get('data_types'):
        if not config['data_types'][ftype].get('file_format'):
            msg = '{} has no file_format'.format(ftype)
            messages.append(msg)

        if not config['data_types'][ftype].get('local_path'):
            msg = '{} has no local_path'.format(ftype)
            messages.append(msg)
        if config['data_types'][ftype].get('monthly') == 'True':
            config['data_types'][ftype]['monthly'] = True
        if config['data_types'][ftype].get('monthly') == 'False':
            config['data_types'][ftype]['monthly'] = False
    # ------------------------------------------------------------------------
    # check img_hosting
    # ------------------------------------------------------------------------
    if config.get('img_hosting'):
        if not config['img_hosting'].get('img_host_server'):
            msg = 'image hosting is turned on, but no img_host_server specified'
            messages.append(msg)
        if not config['img_hosting'].get('host_directory'):
            msg = 'image hosting is turned on, but no host_directory specified'
            messages.append(msg)

    if config.get('post-processing'):
        # ------------------------------------------------------------------------
        # check regrid
        # ------------------------------------------------------------------------
        if config['post-processing'].get('regrid'):
            for item in config['post-processing']['regrid']:
                if item in ['custom_args']:
                    continue
                if item == 'lnd':
                    if not config['post-processing']['regrid'][item].get('source_grid_path'):
                        msg = 'no source_grid_path given for {} regrid'.format(
                            item)
                        messages.append(msg)
                    if not config['post-processing']['regrid'][item].get('destination_grid_path'):
                        msg = 'no destination_grid_path given for {} regrid'.format(
                            item)
                        messages.append(msg)
                    if not config['post-processing']['regrid'][item].get('destination_grid_name'):
                        msg = 'no destination_grid_name given for {} regrid'.format(
                            item)
                        messages.append(msg)
                else:
                    if not config['post-processing']['regrid'][item].get('regrid_map_path'):
                        msg = 'no regrid_map_path given for {} regrid'.format(
                            item)
                        messages.append(msg)
                for sim in config['simulations']:
                    if sim in ['start_year', 'end_year']:
                        continue
                    if 'all' not in config['simulations'][sim].get('data_types'):
                        if item not in config['simulations'][sim].get('data_types') and ('regrid' in config['simulations'][sim]['job_types'] or 'all' in config['simulations'][sim]['job_types']):
                            msg = 'regrid is set to run on data_type {}, but this type is not set in simulation {}'.format(
                                item, sim)
                            messages.append(msg)
        # ------------------------------------------------------------------------
        # check ncclimo
        # ------------------------------------------------------------------------
        if config['post-processing'].get('climo'):
            for sim in sim_names:

                if 'climo' not in config['simulations'][sim].get('job_types') and 'all' not in config['simulations'][sim].get('job_types'):
                    continue
                if 'all' not in config['simulations'][sim].get('data_types') and 'atm' not in config['simulations'][sim].get('data_types'):
                    msg = 'ncclimo is set to run for simulation {}, but this simulation does not have atm in its data_types'.format(
                        sim)
                    messages.append(msg)
                if not config['post-processing']['climo'].get('regrid_map_path'):
                    msg = 'no regrid_map_path given for climo'
                    messages.append(msg)
                if not config['post-processing']['climo'].get('destination_grid_name'):
                    msg = 'no destination_grid_name given for climo'
                    messages.append(msg)
                if not config['post-processing']['climo'].get('run_frequency'):
                    msg = 'no run_frequency given for ncclimo'
                    messages.append(msg)
                else:
                    if not isinstance(config['post-processing']['climo'].get('run_frequency'), list):
                        config['post-processing']['climo']['run_frequency'] = [
                            config['post-processing']['climo']['run_frequency']]

        # ------------------------------------------------------------------------
        # check timeseries
        # ------------------------------------------------------------------------
        if config['post-processing'].get('timeseries'):
            if not config['post-processing']['timeseries'].get('run_frequency'):
                msg = 'no run_frequency given for timeseries'
                messages.append(msg)
            else:
                if not isinstance(config['post-processing']['timeseries'].get('run_frequency'), list):
                    config['post-processing']['timeseries']['run_frequency'] = [
                        config['post-processing']['timeseries']['run_frequency']]
            for item in config['post-processing']['timeseries']:
                
                if item in ['run_frequency', 'regrid_map_path', 'destination_grid_name', 'custom_args', 'job_args']:
                    continue
                if item not in ['atm', 'lnd', 'ocn', 'cice']:
                    msg = '{} is an unsupported timeseries data type, processflow only supports time series regridding for data-types: atm, lnd, ocn, cice'.format(
                        item)
                    messages.append(msg)
                
                if not isinstance(config['post-processing']['timeseries'][item], list):
                    config['post-processing']['timeseries'][item] = [
                        config['post-processing']['timeseries'][item]]
                for sim in sim_names:
                    if 'all' not in config['simulations'][sim].get('data_types'):
                        if item not in config['simulations'][sim].get('data_types') and ('timeseries' in config['simulations'][sim]['job_types'] or 'all' in config['simulations'][sim]['job_types']):
                            msg = 'timeseries-{} is set to run for simulation {}, but this simulation does not have {} in its data_types'.format(
                                item, sim, item)
                            messages.append(msg)
        # ------------------------------------------------------------------------
        # check cmor
        # ------------------------------------------------------------------------
        if config['post-processing'].get('cmor'):
            # check that a valid run_frequency is set
            if not config['post-processing']['cmor'].get('run_frequency'):
                msg = 'no run_frequency given for cmor, make sure this matches the frequency for timeseries'
                messages.append(msg)
            else:
                # if its not a list, package it inside one
                if not isinstance(config['post-processing']['cmor'].get('run_frequency'), list):
                    config['post-processing']['cmor']['run_frequency'] = [
                        config['post-processing']['cmor']['run_frequency']]

            any_tables = False
            for table in ["Amon", "Lmon", "Omon", "SImon"]:
                if config['post-processing']['cmor'].get(table):
                    any_tables = True
                
                    # make sure the var list is packed inside a list
                    if not isinstance(config['post-processing']['cmor'][table]['variables'], list):
                        config['post-processing']['cmor'][table]['variables'] = [
                            config['post-processing']['cmor'][table]['variables']]
                
                if table == "Omon":
                    for extra in ['mpas_mesh_path', 'mpas_map_path', 'regions_path', 'mpaso-namelist']:
                        if not config['post-processing']['cmor'].get(extra):
                            msg = f"cmor set to run CMIP table {table} but {extra} was not included in the cmor config section"
                            messages.append(msg)
                
                if table == "SImon":
                    for extra in ['mpas_mesh_path', 'mpas_map_path']:
                        if not config['post-processing']['cmor'].get(extra):
                            msg = f"cmor set to run CMIP table {table} but {extra} was not included in the cmor config section"
                            messages.append(msg)

            if not any_tables:
                msg = "Please specify which tables to produce variables for"
                messages.append(msg)
            

            for extra in ['mpas_mesh_path', 'mpas_map_path', 'regions_path', 'mpaso-namelist']:
                if config['post-processing']['cmor'].get(extra) and not os.path.exists(config['post-processing']['cmor'][extra]) and not os.path.lexists(config['post-processing']['cmor'][extra]):
                    msg = f"{extra} was given for cmor, but the file {config['post-processing']['cmor'][extra]} doesnt appear to exist"
                    messages.append(msg)

            if not config['post-processing']['cmor'].get('cmor_tables_path'):
                msg = 'please provide a path to where to find the master cmor tables. A copy of the tables can be found here: https://github.com/PCMDI/cmor'
                messages.append(msg)
            else:
                if not os.path.exists(config['post-processing']['cmor']['cmor_tables_path']):
                    msg = 'provided cmor_tables_path {} points to a directory that doesnt exist. Download a copy from https://github.com/PCMDI/cmor'.format(
                        config['post-processing']['cmor']['cmor_tables_path'])
                    messages.append(msg)

    if config.get('diags'):
        # ------------------------------------------------------------------------
        # check e3sm_diags
        # ------------------------------------------------------------------------
        if config['diags'].get('e3sm_diags'):

            sets = config['diags']['e3sm_diags'].get('sets_to_run')
            if sets:
                if 'area_mean_time_series' in sets:
                    if 'timeseries' not in config['post-processing'].keys():
                        msg = 'e3sm_diags is set to run area_mean_time_series but no timeseries job speficied'
                        messages.append(msg)
            else:
                msg = 'please specify which plot sets to run for e3sm_diags using the sets_to_run config option (lon_lat is the most common set).'
                messages.append(msg)

            if not config['diags']['e3sm_diags'].get('machine_path_prefix'):
                hostname = socket.gethostname()
                if hostname == 'acme1.llnl.gov':
                    config['diags']['e3sm_diags']['machine_path_prefix'] = '/p/user_pub/e3sm/e3sm_diags_data/'
                elif 'cori' in hostname:
                    config['diags']['e3sm_diags']['machine_path_prefix'] = '/global/cfs/cdirs/e3sm/acme_diags/obs_for_e3sm_diags/'
                elif 'compy' in hostname:
                    config['diags']['e3sm_diags']['machine_path_prefix'] = '/compyfs/e3sm_diags_data/obs_for_e3sm_diags/'
                else:
                    msg = 'e3sm_diags requires the mechine_path_prefix for obs data'
                    messages.append(msg)

            frequency = config['diags']['e3sm_diags'].get('run_frequency')
            if not frequency:
                msg = 'No run frequency specified for e3sm_diags'
                messages.append(msg)
            else:

                if not isinstance(frequency, list):
                    config['diags']['e3sm_diags']['run_frequency'] = [frequency]
                    frequency = config['diags']['e3sm_diags'].get('run_frequency')

                for freq in frequency:
                    for sim in config['simulations']:
                        if sim in ['start_year', 'end_year']:
                            continue
                        if 'e3sm_diags' in config['simulations'][sim].get('job_types') \
                                and 'lat_lon' in config['diags']['e3sm_diags']['sets_to_run'] \
                                and 'climo' not in config['simulations'][sim].get('job_types') \
                                and ('climo' in config['post-processing'].keys() and 'all' not in config['simulations'][sim].get('job_types')):
                            msg = 'e3sm_diags is set to run lon_lat for case {case} at {freq}yr frequency, but no climo job is set in its config. Add "climo" to the cases job list, or set the jobs to "all" to run all defined jobs'.format(
                                case=sim,
                                freq=freq)
                            messages.append(msg)
                        if 'e3sm_diags' in config['simulations'][sim].get('job_types') \
                                and 'area_mean_time_series' in config['diags']['e3sm_diags']['sets_to_run'] \
                                and 'timeseries' not in config['simulations'][sim].get('job_types') \
                                and ('timeseries' in config['post-processing'].keys() and 'all' not in config['simulations'][sim].get('job_types')):
                            msg = 'e3sm_diags is set to run timeseries for case {case} at {freq}yr frequency, but no timeseries job is set in its config. Add "climo" to the cases job list, or set the jobs to "all" to run all defined jobs'.format(
                                case=sim,
                                freq=freq)
                            messages.append(msg)

        # ------------------------------------------------------------------------
        # check amwg
        # ------------------------------------------------------------------------
        if config['diags'].get('amwg'):
            if not config['diags']['amwg'].get('diag_home'):
                msg = 'no diag_home given for amwg'
                messages.append(msg)
            if not config['diags']['amwg'].get('run_frequency'):
                msg = 'no diag_home given for amwg'
                messages.append(msg)
            else:
                if not isinstance(config['diags']['amwg'].get('run_frequency'), list):
                    config['diags']['amwg']['run_frequency'] = [
                        config['diags']['amwg']['run_frequency']]
                for sim in sim_names:
                    if 'amwg' in config['simulations'][sim].get('job_types') \
                            and 'climo' not in config['simulations'][sim].get('job_types'):
                        msg = 'amwg is set to run for case {case} but no climo job is set in its config. Add "climo" to the cases job list, or set the jobs to "all" to run all defined jobs'.format(
                            case=sim,
                            freq=freq)
                        messages.append(msg)
                for freq in config['diags']['amwg']['run_frequency']:
                    if not config.get('post-processing') \
                            or not config['post-processing'].get('climo') \
                            or not freq in config['post-processing']['climo']['run_frequency']:
                        msg = 'amwg is set to run at frequency {} but no climo job for this frequency is set'.format(
                            freq)
                        messages.append(msg)
            if not config['diags']['amwg'].get('sets'):
                msg = 'no sets given for amwg'
                messages.append(msg)
            else:
                allowed_sets = [str(x) for x in range(1, 17)] + ['all', '4a']
                if not isinstance(config['diags']['amwg']['sets'], list):
                    config['diags']['amwg']['sets'] = [
                        config['diags']['amwg']['sets']]
                for idx, s in enumerate(config['diags']['amwg']['sets']):
                    if not isinstance(s, str):
                        config['diags']['amwg']['sets'][idx] = str(s)
                        s = str(s)
                    if s not in allowed_sets:
                        msg = '{} is not in the allowed sets for amwg, allowed sets are {}'.format(
                            s, allowed_sets)
                        messages.append(msg)
        # ------------------------------------------------------------------------
        # check aprime
        # ------------------------------------------------------------------------
        if config['diags'].get('aprime'):
            if not config['diags']['aprime'].get('run_frequency'):
                msg = 'no run_frequency given for aprime'
                messages.append(msg)
            else:
                if not isinstance(config['diags']['aprime']['run_frequency'], list):
                    config['diags']['aprime']['run_frequency'] = [
                        config['diags']['aprime']['run_frequency']]
            if not config['diags']['aprime'].get('aprime_code_path'):
                msg = 'no aprime_code_path given for aprime'
                messages.append(msg)

        # ------------------------------------------------------------------------
        # check MPAS-Analysis
        # ------------------------------------------------------------------------
        if config['diags'].get('mpas_analysis'):
            if not config['diags']['mpas_analysis'].get('run_frequency'):
                msg = 'no run_frequency given for mpas_analysis'
                messages.append(msg)
            else:
                if not isinstance(config['diags']['mpas_analysis']['run_frequency'], list):
                    config['diags']['mpas_analysis']['run_frequency'] = [
                        config['diags']['mpas_analysis']['run_frequency']]
            required_parameters = ['diagnostics_path', 'generate_plots', 'start_year_offset',
                                   'ocn_obs_data_path', 'seaice_obs_data_path', 'region_mask_path', 'run_MOC']
            for param in required_parameters:
                if not config['diags']['mpas_analysis'].get(param):
                    msg = 'Missing parameter {p} is required for MPAS-Analysis'.format(
                        p=param)
                    messages.append(msg)
            if not isinstance(config['diags']['mpas_analysis'].get('generate_plots', ''), list):
                config['diags']['mpas_analysis']['generate_plots'] = [
                    config['diags']['mpas_analysis'].get('generate_plots', '')]

            required_datatypes = ['ocn', 'cice', 'ocn_restart', 'cice_restart',
                                  'ocn_streams', 'cice_streams', 'ocn_in', 'cice_in', 'meridionalHeatTransport']
            for reqtype in required_datatypes:
                for sim in sim_names:
                    if 'mpas_analysis' in config['simulations'][sim].get('job_types') \
                            and reqtype not in config['simulations'][sim].get('data_types'):
                        if 'all' in config['simulations'][sim].get('data_types') and reqtype in config['data_types'].keys():
                            continue
                        msg = 'mpas_analysis is set to run for case {case}, but {reqtype} is not in the cases data_types'.format(
                            case=sim,
                            reqtype=reqtype)
                        messages.append(msg)

        # ------------------------------------------------------------------------
        # check ILAMB
        # ------------------------------------------------------------------------                        
        if config['diags'].get('ilamb'):
            # make sure there's a run_frequency and its packed inside a list
            if not config['diags']['ilamb'].get('run_frequency'):
                msg = 'no run_frequency given for ilamb'
                messages.append(msg)
            else:
                if not isinstance(config['diags']['ilamb']['run_frequency'], list):
                    config['diags']['ilamb']['run_frequency'] = [
                        config['diags']['ilamb']['run_frequency']]
            # make sure there's a cmor job
            if not config['post-processing'].get('cmor'):
                msg = f"ILAMB requires that variables be provided in the CMIP6 format, please configure a CMOR job"
                messages.append(msg)
            else:
                # make sure all the variables are going to be created by CMOR
                if not config['diags']['ilamb'].get('variables'):
                    msg = f'Please give one or more variables for ilamb to run'
                    messages.append(msg)
                else:
                    for var in config['diags']['ilamb']['variables']:
                        found_var = False
                        for table in ["Amon", "Lmon"]:
                            if var in config['post-processing']['cmor'][table]['variables']:
                                found_var = True
                                break
                        if not found_var:
                            msg = f"ILAMB is set to run on variable {var}, but the CMOR job isnt set to generate it"
                            messages.append(msg)
            # make sure there's an ILAMB data root
            if not config['diags']['ilamb'].get('obs_data_root'):
                msg = "Please specify the ilamb_root for the ilamb obs data"
                messages.append(msg)
    return messages
# ------------------------------------------------------------------------


def check_config_white_space(filepath):
    line_index = 0
    found = False
    with open(filepath, 'r') as infile:
        for line in infile.readlines():
            line_index += 1
            index = line.find('=')
            if index == -1:
                found = False
                continue
            if line[index + 1] != ' ':
                found = True
                break
    if found:
        return line_index
    else:
        return 0
# ------------------------------------------------------------------------
