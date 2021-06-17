"""
File Merger Utility
Author: Abhimanyu Mitra
Version 1.0
"""

import configparser
import os
from concurrent.futures import ThreadPoolExecutor
from glob import glob

config = configparser.ConfigParser()
config.read('config.ini')
input_file_path = config.get('FILE_PATH', 'input_file_path')
output_file_path = config.get('FILE_PATH', 'output_file_path')


def get_file_names():
    file_names = os.listdir(path=input_file_path)
    unique_file_names = set()
    for file_name in file_names:
        split_index = file_name.find('_')
        unique_file_names.add(file_name[:split_index])

    return list(unique_file_names)


def get_completed_file_names(file_name):
    # not all files will be completed in the list, this method gets the file names which have yet not completed the
    # processing
    abs_files = glob(f'{input_file_path}{os.sep}{file_name}*')
    files = []
    for file in abs_files:
        files.append(file[file.rfind(os.sep) + 1:])
    part_file_count = files[0].split('_')[-2]
    if int(part_file_count) != len(files):
        files.clear()

    return files


def merge_files(unique_file):
    completed_files = get_completed_file_names(unique_file)
    if len(completed_files) > 0:
        merged_file_name = completed_files[0][:completed_files[0].find('_')] + '_merged.csv'
        with open(f'{output_file_path}{os.sep}{merged_file_name}', 'a') as merged_file:
            for file in completed_files:
                for line in open(f'{input_file_path}{os.sep}{file}', 'r'):
                    merged_file.write(line)
        future = f'{merged_file_name} is merged'
    else:
        future = f'All part files not yet generated for {unique_file}'

    return future


if __name__ == '__main__':
    unq_files = get_file_names()
    max_workers = int(config.get('PARAMS', 'max_workers'))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = executor.map(merge_files, unq_files)

    for result in results:
        print(result)
