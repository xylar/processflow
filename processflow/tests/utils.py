import os

def touch(fname):

    if os.path.exists(fname):
        return
    tail, _ = os.path.split(fname)
    if not os.path.exists(tail):
        os.makedirs(tail)
    with open(fname, 'w') as fp:
        fp.write('\n')

# generate mock files that match the expected ncclimo output
def mock_climos(output_path, regrid_path):
    for month in range(1, 13):
        name = '20180129.DECKv1b_piControl.ne30_oEC.edison_{month:02d}_0001{month:02d}_0002{month:02d}_climo.nc'.format(month=month)
        outpath = os.path.join(output_path, name)
        touch(outpath)

        outpath = os.path.join(regrid_path, name)
        touch(outpath)
    
    for season in ['ANN_000101_000212', 'DJF_000101_000212', 'JJA_000106_000208', 'MAM_000103_000205', 'SON_000109_000211']:
        name = '20180129.DECKv1b_piControl.ne30_oEC.edison_{season}_climo.nc'.format(season=season)
        outpath = os.path.join(output_path, name)
        touch(outpath)

        outpath = os.path.join(regrid_path, name)
        touch(outpath)

def mock_ts(variables, output_path, regrid_path, start_year, end_year):
    # create some dummy files for the post validator
    for var in variables:
        file_name = "{var}_{start:04d}01_{end:04d}12.nc".format(
            var=var,
            start=start_year,
            end=end_year)
        file_path = os.path.join(output_path, file_name)
        touch(file_path)
        file_path = os.path.join(regrid_path, file_name)
        touch(file_path)
