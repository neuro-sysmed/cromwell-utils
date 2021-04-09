#!/usr/bin/env python3

import os
import re
import sys
import argparse
from datetime import datetime, timedelta
import time

import pytz


import kbr.args_utils as args_utils
import kbr.version_utils as version_utils
import kbr.string_utils as string_utils
import kbr.datetime_utils as datetime_utils


sys.path.append('.')

import whittle.cromwell_api as cromwell_api
import whittle.cromwell as cromwell


version = version_utils.as_string('whittle')
as_json = False

def group_args(args) -> {}:

    res = {'':[]}
    for arg in args:
        m = re.match(r'(\w):(.+)', arg)
        if m is not None:
            k, v = m.group(1), m.group(2)
            if k not in res:
                res[ k ] = []
            res[ k ].append(v)
        else:
            res[ 'rest' ].append(v)
            
    return res

def cromwell_info() -> None:
    c_version = cromwell_api.get_version()
    c_status = cromwell_api.get_status()
    print(f'Cromwell server (v:{c_version}), status: {c_status}')


def workflow_subcmd(commands) -> None:
    sub_commands = ['submit', 'batch', 'status', 'abort', 'logs', 'outputs', 'timing', 'meta', 'labels', 'help']

    if len(commands) == 0:
        commands.append('help')

    sub_command = commands.pop(0)

    if sub_command == 'submit':
        cromwell.submit_workflow(commands, as_json=as_json)
    elif sub_command == 'batch':
        cromwell.batch_submit_workflow(commands, as_json=as_json)
    elif sub_command == 'status':
        cromwell.workflow_status(commands, as_json=as_json)
    elif sub_command == 'abort':
        cromwell.workflow_abort(commands, as_json=as_json)
    elif sub_command == 'logs':
        cromwell.workflow_logs(commands, as_json=as_json)
    elif sub_command == 'outputs':
        cromwell.workflow_outputs(commands, as_json=as_json)
    elif sub_command == 'timing':
        sys.exit(10)
        print('not done yet, do we need this? outputs a html page')
        cromwell.workflow_timing(commands, as_json=as_json)
    elif sub_command == 'meta':
        cromwell.workflow_meta(commands, as_json=as_json)
    elif sub_command == 'labels':
        print('not done yet, do we need this?')
        sys.exit(10)
        cromwell.labels(commands, as_json=as_json)
    else:
        if sub_command != 'help':
            print(f"Error: Unknown command '{sub_command}'\n")

        print("Help:")
        print("==========================")
        print("workflow submit [wdl-file] i:[input-jsonfile(s)] o:[options-jsonfile] d:[dependency-zipfile] l:[label-jsonfile] ")
        print("workflow batch  [wdl-file] i:[input-jsonfile] o:[options-jsonfile] d:[dependency-zipfile] l:[label-jsonfile]")
        print("workflow status [job-ids]")
        print("workflow abort [job-ids]")
        print("workflow logs [job-ids]")
        print("workflow outputs [job-ids]")
#        print("workflow timing [job-id]")
        print("workflow meta [job-ids]")
#        print("workflow labels get [job-id]")
#        print("workflow labels set [job-id] [label-jsonfile]")
        sys.exit(1)



def workflows_subcmd(commands) -> None:
    sub_commands = ['days', 'status', 'name', 'id', 'label', 'help']

    if len(commands) == 0:
        commands.append('all')

    sub_command = commands.pop(0)

    if sub_command == 'all':
        cromwell.workflows(as_json=as_json)
    elif sub_command == 'date':
        from_date = args_utils.get_or_fail(commands, "from date is required")
        to_date   = args_utils.get_or_default(commands, None)

        cromwell.workflows(from_date=from_date, to_date=to_date, as_json=as_json)
    elif sub_command == 'days':
        days   = args_utils.get_or_default(commands, 7)
        from_date = datetime_utils.to_string( datetime.now(pytz.utc) - timedelta(days=int(days)) )
        cromwell.workflows(from_date=from_date, as_json=as_json, query=True)
    elif sub_command == 'status':
        cromwell.workflows(status=commands, as_json=as_json)
    elif sub_command == 'name':
        cromwell.workflows(names=commands, as_json=as_json)
    elif sub_command == 'id':
        cromwell.workflows(ids=commands, as_json=as_json)
    elif sub_command == 'query' or sub_command == 'q':
        args = group_args(commands)

        cromwell.workflows( from_date=args.get("f", None),
                            to_date=args.get("t", None),
                            status=args.get("s", None), 
                            names=args.get("n", None), 
                            ids=args.get("i", None), query=True, as_json=as_json)
    else:
        
        if sub_command != 'help':
            print(f"Error: Unknown command '{sub_command}'\n")

        print("Help:")
        print("==========================")
        print("workflows (all, default)")
        print("workflows date [from-date] <end-date>  ")
        print("workflows days [days from now]")
        print("workflows status [status1, status2, ...]  ")
        print("workflows name [name1, name2, ...]")
        print("workflows id [id1, id2, ...]")
        print("workflows days [nr of days, default=7]")
        print("workflows query f:[from-date] t:[to-date] s:[status] n:[name] i:[ids] l:[labels]")
        sys.exit(1)



def monitor_subcmd(commands, interval:int=60) -> None:
    sub_commands = ['all', 'days', 'status', 'name', 'id', 'label', 'help']

    if len(commands) == 0 :
        commands.append('all')

    if commands[0] not in sub_commands or 'help' in commands:
        if 'help' not in commands:
            print(f"Error: Unknown command '{commands[0]}'\n")

        print("Help:")
        print("==========================")
        print("monitor (all, default)")
        print("monitor days [days from now]")
        print("monitor status [status1, status2, ...]  ")
        print("monitor name [name1, name2, ...]")
        print("monitor id [id1, id2, ...]")
        print("monitor label [label1, label2, ...]")
        print("monitor query s:[status] n:[name] i:[ids] l:[labels]")
        sys.exit(1)

    global as_json
    as_json = False

    tmp_commands = commands.copy()

    while True:
        os.system('clear')
        print(datetime.now())
        commands = tmp_commands.copy()
        workflows_subcmd( commands )
        time.sleep(int(interval))
        continue

def main():

    commands = [ 'workflow', 'workflows', 'cromwell', 'monitor', 'help']

    parser = argparse.ArgumentParser(description=f'nga_cli: command line tool for the NGA ({version})')

    parser.add_argument('-c', '--config', help="NGA config file, or set env CROMWELL",
                        default=args_utils.get_env_var('CROMWELL'))
    parser.add_argument('-j', '--json-output', help="print the outputs in json format",
                        action="store_true", default=False)
    parser.add_argument('-i', '--interval', help="update interval when monitoring", default=60)
    parser.add_argument('-v', '--verbose', default=0, action="count", help="Increase the verbosity of logging output")
    parser.add_argument('command', nargs='*', help="{}".format(",".join(commands)))

    args = parser.parse_args()

    if args.json_output:
        global as_json
        as_json = True

    args_utils.min_count(1, len(args.command),
                         msg="nga-cli takes one of the following commands: {}".format(string_utils.comma_sep(commands)))

    command = args.command.pop(0)


    if command == 'workflow':
        workflow_subcmd(args.command)
    elif command == 'workflows':
        workflows_subcmd(args.command)
    elif command == 'monitor':
        monitor_subcmd(args.command, args.interval)
    elif command == 'cromwell':
        cromwell_info()
    else:
        print("The tool support the following commands: {}\n".format(string_utils.comma_sep(commands)))
        parser.print_usage()
        parser.add_argument('command', nargs='+', help="{}".format(",".join(commands)))
        sys.exit(1)

if __name__ == "__main__":
    main()