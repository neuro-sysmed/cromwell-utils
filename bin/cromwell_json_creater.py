#!/usr/bin/env python3

import os
import re
import sys
import argparse
from datetime import datetime, timedelta
import time
import json
import pprint as pp
pp.PrettyPrinter(indent=4)


import kbr.args_utils as args_utils
import kbr.version_utils as version_utils
import kbr.string_utils as string_utils
import kbr.datetime_utils as datetime_utils


sys.path.append('.')

import whittle.cromwell_api as cromwell_api
import whittle.cromwell as cromwell



version = version_utils.as_string('whittle')

def _get_keys(data:dict, level:int) -> list:

    if level <= 1:
        return data.items()
    else:
        data_joined = {}
        for key in data.keys():
            if isinstance(data[key], dict):
                for sub_key, sub_value in _get_keys(data[key], level - 1):
                    data_joined[ f"{key}.{sub_key}" ] = sub_value
            else:
                data_joined[key] = data[key]

        return data_joined.items()


def get_keys(data:dict, level:int) -> list:
    ''' recursive joining of keys & subkeys into dot sep keys '''
    if level <= 1:
        return data
    else:
        return dict(_get_keys(data, level))



def build_json(entries:list, workflow:str) -> dict:

    data = {workflow:{}}
    for entry in entries:
        path,value = entry.split("=")
        path_parts = path.split(".")
        sub_data = data[workflow]

        for path_part in path_parts:
            if path_part not in sub_data:
                sub_data[ path_part ] = {}

            if path_part != path_parts[-1]:
                sub_data  = sub_data[path_part]

        if path_part in sub_data and not isinstance(sub_data[path_part], dict):
            tmp_value = sub_data[path_part]
            sub_data[path_part] = [tmp_value]

        if isinstance(sub_data[path_part], list):
            sub_data[path_part].append(value)    
        else:
            sub_data[path_part] = value

    return data


def main():

    commands = [ 'workflow', 'workflows', 'cromwell', 'monitor', 'help']

    parser = argparse.ArgumentParser(description=f'nga_cli: command line tool for the NGA ({version})')

    parser.add_argument('-w', '--workflow', help="Workflow name", required=True)
    parser.add_argument('-p', '--pack-level', help="pack values under workflow", default=2, type=int)
    parser.add_argument('-j', '--jsons-add', help="jsons to add under workflow")

    parser.add_argument('-v', '--verbose', default=0, action="count", help="Increase the verbosity of logging output")
    parser.add_argument('entries', nargs='*', help="{}".format(",".join(commands)))

    args = parser.parse_args()

    workflow = args.workflow
    data= build_json(args.entries, workflow)

    pp.pprint(get_keys(data, args.pack_level))


    sys.exit()


if __name__ == "__main__":
    main()