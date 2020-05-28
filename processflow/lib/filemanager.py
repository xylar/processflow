from __future__ import absolute_import, division, print_function, unicode_literals
import logging
import os
import threading
from tqdm import tqdm

from threading import Thread
from enum import IntEnum

from .models import DataFile
from processflow.lib.util import print_debug, print_line


class FileStatus(IntEnum):
    PRESENT = 0
    NOT_PRESENT = 1
    IN_TRANSIT = 2
# -----------------------------------------------


class FileManager(object):
    """
    Manage all files required by jobs
    """

    def __init__(self, event_list, config, database='processflow.db'):
        """
        Parameters:
            database (str): the path to where to create the sqlite database file
            config (dict): the global configuration dict
        """
        self._event_list = event_list
        self._db_path = database
        self._config = config

        if os.path.exists(database):
            os.remove(database)

        DataFile._meta.database.init(database)
        if DataFile.table_exists():
            DataFile.drop_table()

        DataFile.create_table()
    # -----------------------------------------------

    def __str__(self):
        # TODO: make this better
        return str({
            'db_path': self._db_path,
        })
    # -----------------------------------------------

    def write_database(self):
        """
        Write out a human readable version of the database for debug purposes
        """
        file_list_path = os.path.join(
            self._config['global']['project_path'],
            'output',
            'file_list.txt')
        with open(file_list_path, 'w') as fp:
            try:
                for case in self._config['simulations']:
                    if case in ['start_year', 'end_year']:
                        continue
                    fp.write('+++++++++++++++++++++++++++++++++++++++++++++')
                    fp.write('\n\t{case}\t\n'.format(case=case))
                    fp.write('+++++++++++++++++++++++++++++++++++++++++++++\n')
                    q = (DataFile
                         .select(DataFile.datatype)
                         .where(DataFile.case == case)
                         .distinct())
                    for df_type in q.execute():
                        _type = df_type.datatype
                        fp.write('===================================\n')
                        fp.write('\t' + _type + ':\n')
                        datafiles = (DataFile
                                     .select()
                                     .where(
                                            (DataFile.datatype == _type) &
                                            (DataFile.case == case)))
                        for datafile in datafiles.execute():
                            filestr = '-------------------------------------'
                            filestr += '\n\t     name: ' + datafile.name + '\n\t     local_status: '
                            if datafile.local_status == 0:
                                filestr += ' present, '
                            else:
                                filestr += ' missing, '

                            filestr += '\n\t     local_size: ' + \
                                str(datafile.local_size)
                            filestr += '\n\t     local_path: ' + datafile.local_path
                            filestr += '\n\t     year: ' + str(datafile.year)
                            filestr += '\n\t     month: ' + \
                                str(datafile.month) + '\n'
                            fp.write(filestr)
            except Exception as e:
                print_debug(e)
    # -----------------------------------------------

    def check_data_ready(self, data_required, case, start_year=None, end_year=None):
        try:
            for datatype in data_required:
                if not self._config['data_types'].get(datatype):
                    return False
                monthly = self._config['data_types'][datatype].get('monthly')
                if start_year and end_year and monthly:
                    q = (DataFile
                         .select()
                         .where(
                                (DataFile.year >= start_year) &
                                (DataFile.year <= end_year) &
                                (DataFile.case == case) &
                                (DataFile.datatype == datatype)))
                else:
                    q = (DataFile
                         .select()
                         .where(
                                (DataFile.case == case) &
                                (DataFile.datatype == datatype)))
                datafiles = q.execute()
                if not datafiles:
                    return False
                for df in datafiles:
                    if df.local_status != FileStatus.PRESENT.value:
                        return False
            return True
        except Exception as e:
            print_debug(e)
    # -----------------------------------------------

    def render_file_string(self, data_type, data_type_option, case, year=None, month=None):
        """
        Takes strings from the data_types dict and replaces the keywords with the appropriate values
        """
        # setup the replacement dict
        start_year = int(self._config['simulations']['start_year'])
        end_year = int(self._config['simulations']['end_year'])
        replace = {
            'PROJECT_PATH': self._config['global']['project_path'],
            'CASEID': case,
            'REST_YR': f'{start_year + 1:04d}',
            'START_YR': f'{start_year:04d}',
            'END_YR': f'{end_year:04d}',
            'LOCAL_PATH': self._config['simulations'][case].get('local_path', '')
        }
        if year is not None:
            replace['YEAR'] = f'{year:04d}'
        if month is not None:
            replace['MONTH'] = f'{month:02d}'

        # if this datatype definition also includes a specific entry for the given case
        if self._config['data_types'][data_type].get(case):
            if self._config['data_types'][data_type][case].get(data_type_option):
                instring = self._config['data_types'][data_type][case][data_type_option]
                for item in self._config['simulations'][case]:
                    if item.upper() in self._config['data_types'][data_type][case][data_type_option]:
                        instring = instring.replace(
                            item.upper(), self._config['simulations'][case][item])
                        break
                return instring

        instring = self._config['data_types'][data_type].get(data_type_option)
        if not instring:
            return ""

        for string, val in list(replace.items()):
            if string in instring:
                instring = instring.replace(string, val)
        return instring
    # -----------------------------------------------

    def populate_file_list(self):
        """
        Populate the database with the required DataFile entries
        """
        msg = 'Creating file table'
        print_line(msg)

        start_year = int(self._config['simulations']['start_year'])
        end_year = int(self._config['simulations']['end_year'])
        with DataFile._meta.database.atomic():
            # for each case
            new_files = list()
            for case in self._config['simulations']:
                if case in ['start_year', 'end_year']:
                    continue

                # for each data type included in this case
                for _type in self._config['simulations'][case]['data_types']:

                    # if the type isnt defined in the config, throw an exception
                    if _type not in self._config['data_types'] and _type != 'all':
                        raise ValueError(f'{_type} from case {case} is not defined as a valid data type')                    
                    elif _type == 'all':
                        pass
                    else:
                        msg = f'Checking files for data type: {_type} for case: {case}'
                        print_line(msg, 'ok')

                    # setup the base local_path
                    local_root = self.render_file_string(
                        data_type=_type,
                        data_type_option='local_path',
                        case=case)

                    
                    if self._config['data_types'][_type].get('monthly') and self._config['data_types'][_type]['monthly'] in ['True', 'true', '1', 1]:
                        # handle monthly data
                        for year in range(start_year, end_year + 1):
                            for month in range(1, 13):
                                filename = self.render_file_string(
                                    data_type=_type,
                                    data_type_option='file_format',
                                    case=case,
                                    year=year,
                                    month=month)
                                
                                local_path = os.path.join(local_root, filename)
                                new_files.append({
                                    'name': filename,
                                    'local_path': local_path,
                                    'local_status': FileStatus.PRESENT.value,
                                    'case': case,
                                    'year': year,
                                    'month': month,
                                    'datatype': _type,
                                    'super_type': 'raw_output',
                                    'local_size': 0
                                })
                    else:
                        # handle one-off data
                        filename = self.render_file_string(
                            data_type=_type,
                            data_type_option='file_format',
                            case=case)
                        local_path = os.path.join(local_path, filename)
                        if not os.path.exists(local_path):
                            raise ValueError(f'File {local_path} not found for case {case}')
                        new_files.append({
                            'name': filename,
                            'local_path': os.path.join(local_path, filename),
                            'local_status': FileStatus.PRESENT.value,
                            'case': case,
                            'year': 0,
                            'month': 0,
                            'datatype': _type,
                            'super_type': 'raw_output',
                            'local_size': 0
                        })

            step = 500
            for idx in range(0, len(new_files), step):
                with DataFile._meta.database.atomic():
                    DataFile.insert_many(
                        new_files[idx: idx + step]).execute()

            msg = 'Database initialization complete'
            print_line(msg)
    # -----------------------------------------------

    def print_db(self):
        for df in DataFile.select():
            print({
                'case': df.case,
                'type': df.datatype,
                'name': df.name,
                'local_path': df.local_path
            })
    # -----------------------------------------------

    def add_files(self, data_type, file_list, super_type="raw_output"):
        """
        Add files to the database

        Parameters:
            data_type (str): the data_type of the new files
            file_list (list): a list of dictionaries in the format
                local_path (str): path to the file,
                case (str): the case these files belong to
                name (str): the filename
                year (int): the year of the file, optional
                month (int): the month of the file, optional
        """
        try:
            new_files = list()
            for file in file_list:
                new_files.append({
                    'name': file['name'],
                    'local_path': file['local_path'],
                    'local_status': file.get('local_status', FileStatus.NOT_PRESENT.value),
                    'datatype': data_type,
                    'super_type': super_type,
                    'case': file['case'],
                    'year': file.get('year', 0),
                    'month': file.get('month', 0),
                    'local_size': 0,
                })
            step = 500
            for idx in range(0, len(new_files), step):
                with DataFile._meta.database.atomic():
                    DataFile.insert_many(
                        new_files[idx: idx + step]).execute()
        except Exception as e:
            print_debug(e)
    # -----------------------------------------------

    def file_status_check(self):
        """
        Update the database with the local status of the expected files

        Return True if there was new local data found, False othewise
        """
        # try:
        query = (DataFile
                    .select()
                    .where(DataFile.local_status == FileStatus.PRESENT.value))
        to_update = list()

        for datafile in tqdm(query.execute(), desc="Checking local files"):
            if not os.path.exists(datafile.local_path):
                raise ValueError(f'File {datafile.local_path} not found')
            
    # -----------------------------------------------

    def all_data_local(self):
        """
        Returns True if all data is local, False otherwise
        """
        try:
            query = (DataFile
                     .select()
                     .where(DataFile.local_status == FileStatus.NOT_PRESENT.value))
            missing_data = query.execute()
            if missing_data:
                return False
        except Exception as e:
            print_debug(e)
        logging.debug('All data is local')
        return True
    # -----------------------------------------------

    def get_file_paths_by_year(self, datatype, case, start_year=None, end_year=None):
        """
        Return paths to files that match the given type, start, and end year

        Parameters:
            datatype (str): the type of data
            case (str): the name of the case to return files for
            monthly (bool): is this datatype monthly frequency
            start_year (int): the first year to return data for
            end_year (int): the last year to return data for
        """
        try:
            if start_year and end_year:
                if datatype in ['climo_regrid', 'climo_native', 'ts_regrid', 'ts_native']:
                    query = (DataFile
                             .select()
                             .where(
                                 (DataFile.month == end_year) &
                                 (DataFile.year == start_year) &
                                 (DataFile.case == case) &
                                 (DataFile.datatype == datatype) &
                                 (DataFile.local_status == FileStatus.PRESENT.value)))
                else:
                    query = (DataFile
                             .select()
                             .where(
                                 (DataFile.year <= end_year) &
                                 (DataFile.year >= start_year) &
                                 (DataFile.case == case) &
                                 (DataFile.datatype == datatype) &
                                 (DataFile.local_status == FileStatus.PRESENT.value)))
            else:
                query = (DataFile
                         .select()
                         .where(
                                (DataFile.case == case) &
                                (DataFile.datatype == datatype) &
                                (DataFile.local_status == FileStatus.PRESENT.value)))
            datafiles = query.execute()
            if datafiles is None or len(datafiles) == 0:
                return None
            return [x.local_path for x in datafiles]
        except Exception as e:
            print_debug(e)
    # -----------------------------------------------
