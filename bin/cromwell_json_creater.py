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


def main():

    commands = [ 'workflow', 'workflows', 'cromwell', 'monitor', 'help']

    parser = argparse.ArgumentParser(description=f'nga_cli: command line tool for the NGA ({version})')

    parser.add_argument('-W', '--workflow', help="Workflow name", required=True)
    parser.add_argument('-N', '--nested-values', help="Nest values under workflow", default=True, action="store_false")
    parser.add_argument('-j', '--jsons-add', help="jsons to add under workflow")

    parser.add_argument('-v', '--verbose', default=0, action="count", help="Increase the verbosity of logging output")
    parser.add_argument('entries', nargs='*', help="{}".format(",".join(commands)))

    args = parser.parse_args()

    workflow = args.workflow
    data = {workflow:{}}

    for entry in args.entries:
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


    data_fixed = {}
    sub_data = data

    for sub_part in data:
        key_one = list(sub_data.keys())[0]
        if len(sub_data.keys()) == 1 and len(sub_data[key_one].keys()) == 1:
            print("Found one key")
            print(sub_data.keys())
            subkey_one = list(sub_data[key_one].keys())[0]
            print(sub_data[ key_one ].keys()) 
            new_key = f"{key_one}.{subkey_one}"
            print(new_key)
            data_fixed[ new_key ] = sub_data[ key_one ][ subkey_one ]
        



    pp.pprint(data)
    pp.pprint(data_fixed)

    print(json.dumps( data ))


if __name__ == "__main__":
    main()