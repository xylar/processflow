import cmor
import cdms2
import pdb
pdb.set_trace()

# evspsbl:water_evaporation_flux:longitude latitude time:atmos:1:QFLX:QFLX no change


# extract spacio temp reference system
filename = "/p/user_pub/e3sm/baldwin32/ACME_simulations/20180129.DECKv1b_piControl.ne30_oEC.edison/output/pp/fv129x256/ts/monthly/20yr/QFLX_000101_002012.nc"
filename2 = "/p/user_pub/e3sm/baldwin32/ACME_simulations/20180129.DECKv1b_piControl.ne30_oEC.edison/output/pp/fv129x256/ts/monthly/20yr/QFLX_002101_004012.nc"

filenames = [filename, filename2]


f = cdms2.open(filenames[0])
data = f['QFLX']
lat = data.getLatitude()[:]
lon = data.getLongitude()[:]
lat_bnds = f('lat_bnds')
lon_bnds = f('lon_bnds')
time = data.getTime()
time_bnds = f('time_bnds')
f.close()



cmor.setup(inpath='/export/baldwin32/projects/cmor/Tables', netcdf_file_action=cmor.CMOR_REPLACE)

cmor.dataset_json("/export/baldwin32/projects/cmor/Test/common_user_input.json")

table = 'CMIP6_Amon.json'

cmor.load_table(table)


axes = [{
    'table_entry': 'time',
    'units': time.units
}, {
    'table_entry': 'latitude',
    'units': 'degrees_north',
    'coord_vals': lat[:],
    'cell_bounds': lat_bnds[:]
}, {
    'table_entry': 'longitude',
    'units': 'degrees_east',
    'coord_vals': lon[:],
    'cell_bounds': lon_bnds[:]
}]


axis_ids = list()

for axis in axes:
    axis_id = cmor.axis(**axis)
    axis_ids.append(axis_id)

varid = cmor.variable('evspsbl', 'kg m-2 s-1', axis_ids, positive='up')

for name in filenames:
    print 'opening ' + name
    f = cdms2.open(name)
    data = f['QFLX']
    time_bnds = f('time_bnds')
    for index, val in enumerate(data.getTime()[:]):
        print val, time_bnds[index, :]
        cmor.write(varid, data[index, :], time_vals=val, time_bnds=[
                time_bnds[index, :]])
    f.close()

# make sure to close each file after writing

cmor.close(varid)
