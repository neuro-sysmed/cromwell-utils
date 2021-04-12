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
        if '=' in entry:
            path,value = entry.split("=")
        else:
            value = entry

        try:
            path_parts = path.split(".")
        except:
            print(f"Key not defined for {value}, eg: method.ref=ref_file1 ref_file2 etc")
            sys.exit(1)
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


def add_jsons(data:dict, jsons:list, workflow:str) -> dict:

    for json_file in jsons:

        with open(json_file) as json_fh:
            js = json.load(json_fh)
            for k in js.keys():
                if k not in data[workflow]:
                    data[workflow][k] = {}
                data[workflow][k] = js[k]
        json_fh.close()



    return data

def serialise_jsons(jsons:[]) -> None:

    data = []
    for json_file in jsons:

        with open(json_file) as json_fh:
            js = json.load(json_fh)
            data.append(js)

    return data


def main():

    parser = argparse.ArgumentParser(description=f'nga_cli: command line tool for the NGA ({version})')

    parser.add_argument('-c', '--create', action='store_true', help="create json")
    parser.add_argument('-w', '--workflow', help="Workflow name")
    parser.add_argument('-p', '--pack-level', help="pack values under workflow", default=2, type=int)
    parser.add_argument('-j', '--jsons', action='append', help="jsons to add under workflow")

    parser.add_argument('-S', '--serialise', action='store_true', help="serialise jsons")

    parser.add_argument('-v', '--verbose', default=0, action="count", help="Increase the verbosity of logging output")
    parser.add_argument('-P', '--pretty-print', default=False, action="store_true", help="Pretty print of the json")
    parser.add_argument('entries', nargs='*', help="Entries to build from or join")

    args = parser.parse_args()

    if args.serialise:
        data = serialise_jsons(args.entries)
    else:
        workflow = args.workflow

        data = build_json(args.entries, workflow)
        data = add_jsons(data, args.jsons, workflow )
        data = get_keys(data, args.pack_level)


    if args.pretty_print:
        pp.pprint(data)
    else:
        print(json.dumps(data))

if __name__ == "__main__":
    main()