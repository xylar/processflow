from __future__ import absolute_import, division, print_function, unicode_literals
import logging
import os
import re
import sys
import traceback
import jinja2

from pathlib import Path
from datetime import datetime
from subprocess import Popen, PIPE



def print_line(line, ignore_text=False, newline=True, status='ok'):
    """
    Prints a message to log file and the console

    Parameters:
        line (str): The message to print
        ignore_text (bool): should this be printed to the console if in text mode
    """
    logging.info(line)
    if not ignore_text:
        now = datetime.now()
        if status == 'ok':
            start_color = colors.OKGREEN
            start_icon = '[+]'
        else:
            start_color = colors.FAIL
            start_icon = '[-]'
        hour=now.strftime('%H')
        minutes=now.strftime('%M')
        sec=now.strftime('%S')
        timestr = f'{start_color}{start_icon}{colors.ENDC} {hour}:{minutes}:{sec}'
        msg = f'{timestr}: {line}'
        if newline:
            print(msg, flush=True)
        else:
            print(msg, end=' ', flush=True)
# -----------------------------------------------


def get_climo_output_files(input_path, start_year, end_year):
    """
    Return a list of ncclimo climatologies from start_year to end_year

    Parameters:
        input_path (str): the directory to look in
        start_year (int): the first year of climos to add to the list
        end_year (int): the last year
    Returns:
        file_list (list(str)): A list of the climo files in the directory
    """
    if not os.path.exists(input_path):
        return None
    contents = [s for s in os.listdir(input_path) if not os.path.isdir(s)]
    pattern = r'_{start:04d}\d\d_{end:04d}\d\d_climo\.nc'.format(
        start=start_year,
        end=end_year)
    return [x for x in contents if re.search(pattern=pattern, string=x)]
# -----------------------------------------------


def get_cmip_file_info(filename):
    """
    From a CMIP6 filename, return the variable name as well as the start and end year
    """

    if filename[-3:] != '.nc':
        return False, False, False

    attrs = filename.split('_')
    var = attrs[0]

    pattern = r'\d{6}-\d{6}'
    match = re.match(pattern, attrs[-1])
    if not match:
        # this variable doesnt have time
        return var, False, False
    
    start = int(attrs[-1][:4])
    end = int(attrs[-1][7:11])

    return var, start, end
# -----------------------------------------------

def get_cmor_output_files(input_path, start_year, end_year):
    """
    Return a list of CMORize output files from start_year to end_year
    Parameters:
        input_path (str): the directory to look in
        start_year (int): the first year of climos to add to the list
        end_year (int): the last year
    Returns:
        cmor_list (list): A list of the cmor files
    """
    if not os.path.exists(input_path):
        return None
    cmor_list = list()

    pattern = r'_{start:04d}01-{end:04d}12\.nc'.format(
        start=start_year, end=end_year)

    for root, dirs, files in os.walk(input_path):
        for file_ in files:
            if re.search(pattern, file_):
                cmor_list.append(os.path.join(root, file_))

    return cmor_list
# -----------------------------------------------

def get_ts_output_files(input_path, var_list, start_year, end_year):
    """
    Return a list of ncclimo timeseries files from a list of variables, start_year to end_year

    Parameters:
        input_path (str): the directory to look in
        var_list (list): a list of strings of variable names
        start_year (int): the first year of climos to add to the list
        end_year (int): the last year
    Returns:
        ts_list (list): A list of the ts files
    """
    if not os.path.exists(input_path):
        return None
    contents = [s for s in os.listdir(input_path) if not os.path.isdir(s)]
    ts_list = list()
    for var in var_list:
        pattern = r'{var}_{start:04d}01_{end:04d}12\.nc'.format(
            var=var,
            start=start_year,
            end=end_year)
        for item in contents:
            if re.search(pattern, item):
                ts_list.append(item)
                break
    return ts_list
# -----------------------------------------------


def get_data_output_files(input_path, case, start_year, end_year):
    if not os.path.exists(input_path):
        return None
    contents = [s for s in os.listdir(input_path) if not os.path.isdir(s)]
    contents.sort()
    data_list = list()
    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            pattern = r'%s.*\.%04d-%02d.nc' % (case, year, month)
            for item in contents:
                if re.match(pattern, item):
                    data_list.append(item)
                    break
    return data_list
# -----------------------------------------------


def print_debug(e):
    """
    Print an exceptions relavent information
    """
    print('1', e.__doc__)
    print('2', sys.exc_info())
    print('3', sys.exc_info()[0])
    print('4', sys.exc_info()[1])
    _, _, tb = sys.exc_info()
    print('5', traceback.print_tb(tb))
# -----------------------------------------------


def format_debug(e):
    """
    Return a string of an exceptions relavent information
    """
    _, _, tb = sys.exc_info()
    return """
1: {doc}
2: {exec_info}
3: {exec_0}
4: {exec_1}
5: {lineno}
6: {stack}
""".format(
        doc=e.__doc__,
        exec_info=sys.exc_info(),
        exec_0=sys.exc_info()[0],
        exec_1=sys.exc_info()[1],
        stack=traceback.print_tb(tb))
# -----------------------------------------------


class colors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
# -----------------------------------------------


def render(variables, input_path, output_path):
    """
    Renders the jinja2 template from the input_path into the output_path
    using the variables from variables
    """
    try:
        tail, head = os.path.split(input_path)

        template_path = os.path.abspath(tail)
        loader = jinja2.FileSystemLoader(searchpath=template_path)
        env = jinja2.Environment(loader=loader)
        template = env.get_template(head)
        outstr = template.render(variables)

        with open(output_path, 'a+') as outfile:
            outfile.write(outstr)
    except Exception:
        return False
    else:
        return True
# -----------------------------------------------


def create_symlink_dir(src_dir, src_list, dst):
    """
    Create a directory, and fill it with symlinks to all the items in src_list

    Parameters:
        src_dir (str): the path to the source directory
        src_list (list): a list of strings of filenames
        dst (str): the path to the directory that should hold the symlinks
    """
    if not src_list:
        return
    if not os.path.exists(dst):
        os.makedirs(dst)
    for src_file in src_list:
        if not src_file:
            continue
        source = os.path.join(src_dir, src_file)
        destination = os.path.join(dst, src_file)
        if os.path.lexists(destination):
            os.remove(destination)
        try:
            os.symlink(source, destination)
        except Exception as e:
            msg = format_debug(e)
            logging.error(msg)
# -----------------------------------------------

def ncrcat(inpath, files):
    
    _, start, start_end = get_cmip_file_info(files[0])
    _, _, end = get_cmip_file_info(files[-1])
    
    outname = files[0]
    outname.replace(f"{start_end:04d}12", f"{end:04d}12")
    outpath = Path(outname)

    print_line(f"Concatinating CMOR output for {files[0]:-16}")
    cmd = f"ncrcat {' '.join(files)} {outpath}".split()
    proc = Popen(cmd, stdout=PIPE, stderr=PIPE)
    out, err = proc.communicate()
    if proc.returncode != 0 or err:
        print_line("Error running ncrcat", status='err')
        print(err)
        return None
    else:
        return outpath
# -----------------------------------------------