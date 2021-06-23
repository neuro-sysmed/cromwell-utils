
import re
import os
import shutil
import sys
import json
from datetime import datetime, timedelta
import tabulate
import pytz

import kbr.args_utils as args_utils
import kbr.datetime_utils as datetime_utils

import cromwell.api as cromwell_api



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


def first_element_or_default(values, default=None) -> any:

    if isinstance(values, list):
        if len(values):
            return values[0]

    return default


def submit_workflow(args, as_json:bool=False) -> None:

    wdl_file = args_utils.get_or_fail(args, "workflow file is required")
    args = group_args(args)
    inputs   = args.get('i', [])
    options = args.get('o', None)

    if options is not None:
        options = options[0]
    deps    = args.get('d', None)
    if deps is not None:
        deps = deps[0]
    labels  = args.get('l', None)
    if labels is not None:
        labels = labels[0]

    st = cromwell_api.submit_workflow(wdl_file, inputs, options, deps, labels)
    
    if as_json:
        print(json.dumps(st))
    else:
        print(f'{st["id"]}\t{st["status"]}\t')

def batch_submit_workflow(args, as_json:bool=False) -> None:

    wdl_file = args_utils.get_or_fail(args, "workflow file is required")
    inputs = args_utils.get_or_fail(args, "inputs file is required")
    args = group_args(args)
    options = args.get('o', None)
    deps    = args.get('d', None)
    labels  = args.get('l', None)

    st = cromwell_api.batch_submit_workflow(wdl_file, inputs, options, deps, labels)

    if as_json:
        print(json.dumps(st))
    else:
        for s in st:
            print(f'{s["id"]}\t{s["status"]}\t')



def workflow_status(args, as_json:bool=False) -> None:

    args_utils.min_count(1, len(args), 1, msg="one or more workflow id is required")
    jsons = []
    for wf_id in args:
        st = cromwell_api.workflow_status(wf_id)
        if as_json:
            jsons.append( st )
        else:
            print(f'{st["id"]}\t{st["status"]}\t')

    if as_json:
        print(json.dumps(jsons))

def workflow_abort(args, as_json:bool=False) -> None:

    args_utils.min_count(1, len(args), 1, msg="one or more workflow id is required")
    for wf_id in args:
        st = cromwell_api.workflow_abort(wf_id)
        print(f'{st["id"]}\t{st["status"]}')


def workflow_labels_get(args, as_json:bool=False) -> None:

    args_utils.min_count(1, len(args), 1, msg="one or more workflow id is required")
    jsons = []

    for wf_id in args:
        st = cromwell_api.workflow_labels_get(wf_id)
        if as_json:
            jsons.append( st )
        else:
            for label in st['labels']:
                if label != 'cromwell-workflow-id':
                    print(f"{wf_id}\t{label}:{st['labels'][label]}")

    if as_json:
        print(json.dumps(jsons))

def workflow_labels_set(wf_id:str, args:[], as_json:bool=False) -> None:

    data  = {}
    for label in args:
        key, value = label.split(":")        
        data[key] = value

    st = cromwell_api.workflow_labels_set(wf_id, data)
    if as_json:
        jsons.append( st )
    else:
        for label in st['labels']:
            if label != 'cromwell-workflow-id':
                print(f"{wf_id}\t{label}:{st['labels'][label]}")

def workflow_logs(args, as_json:bool=False) -> None:

    args_utils.min_count(1, len(args), 1, msg="one or more workflow id is required")
    jsons = []
    for wf_id in args:
        st = cromwell_api.workflow_logs(wf_id)
        if as_json:
            jsons.append(st)
        elif 'status' in st:
            print(f'{st["id"]}\t{st["status"]}')
        else:
            for stp in st['calls']:
                for attempt in st['calls'][stp]:
                    print(f"{wf_id}\t{stp}/{attempt['attempt']}\t{attempt['stderr']}")
                    print(f"{wf_id}\t{stp}/{attempt['attempt']}\t{attempt['stderr']}")

    if as_json:
        print(json.dumps(jsons))

def workflow_outputs(args, as_json:bool=False) -> None:

    args_utils.min_count(1, len(args), 1, msg="one or more workflow id is required")
    jsons = []
    for wf_id in args:
        st = cromwell_api.workflow_outputs(wf_id)
        if as_json:
            jsons.append( st )
        elif 'status' in st:
            print(f'{st["id"]}\t{st["status"]}')
        else:
            for output in st['outputs']:
                print(f"{wf_id}\t{output}\t{st['outputs'][output]}")

    if as_json:
        print(json.dumps(jsons))

def workflow_meta(args, as_json:bool=False) -> None:

    args_utils.min_count(1, len(args), 1, msg="one or more workflow id is required")
    jsons = []
    for wf_id in args:
        st = cromwell_api.workflow_meta(wf_id)
        if as_json:
            jsons.append(st)
        else:
            print(f"{wf_id}\tworkflow: {st['workflowName']}")

            print("\n---------------------------------------------------------------------------")
            status = []
            for k in ['status', 'submission', 'start', 'end']:
                if k in st:
                    status.append(f'{k}:{st[k]}')
            print("\t".join(status))
            if 'workflowRoot' in st:
                print(f"Workflow root dir: {st['workflowRoot']}")

            print("\n---------------------------------------------------------------------------")
            print('outputs:')
            for output in st['outputs']:
                print(f"\t{output}\t{st['outputs'][output]}")


            print ('\n\n             and a lot more info if needed (run with -j)...\n\n')

    if as_json:
        print(json.dumps(jsons))

def workflows(from_date:str=None, to_date:str=None, status:[]=None, names:[]=None, ids:[]=None, labels:[]=None, 
              query:bool=False, as_json:bool=False, count:int=-1) -> None:
    data = []

    filter = {}

    if from_date is not None:
        if isinstance(from_date, list) and len(from_date):
            from_date = from_date[0]
        filter['start'] = from_date

    if to_date is not None:
        if isinstance(to_date, list) and len(to_date):
            to_date = to_date[0]
        filter['end'] = to_date

    if status is not None:
        if query:
            filter['status'] = first_element_or_default(status)
        else:
            for st in status:
                data.append({'status': st})

    if names is not None:
        if query:
            filter['name'] = first_element_or_default(names)
        else:
            for nm in names:
                data.append({'name': nm})

    if labels is not None:
        if query:
            filter['label'] = first_element_or_default(labels)
        else:
            for label in labels:
                data.append({'label': label})

    if ids is not None:
        if query:
            filter['id'] = first_element_or_default(ids)
        else:
            for i in ids:
                data.append({'id': i})

    if query and filter != {}:
        data = [filter]
    else:
        data.append(filter)

#    print(data)

    st = cromwell_api.workflows(data)

    res = [["id", "name", "status", "submitted", "started", "ended"]]
    jsons = []

    if 'results' in st:
        for r in st['results']:
            jsons.append( r )

            if 'parentWorkflowId' in r:
                continue

            res.append([ r['id'], r.get('name','NA'), r['status'], r.get('submission', 'NA'), r.get('start', 'NA'), r.get('end', 'NA')])
            count -= 1
            if count == 0:
                break

    else:
        if as_json:
            print(json.dumps(st))
        else:
            print(f"Query error: {st['status']}")

        sys.exit(10)

    return jsons

    if as_json:
        print(json.dumps(jsons))
    else:
        print( tabulate.tabulate(res, headers="firstrow", tablefmt='psql'))
        

def cleanup_workflow(action:str, wf_id:str, done_only:bool=True, hours_ago:int=0) -> None:
    st = cromwell_api.workflow_outputs(wf_id)
    outputs = {}

    meta = cromwell_api.workflow_meta(wf_id)
    rootdir = meta.get('workflowRoot', None)
    status  = meta.get('status', None)
    start   = meta.get('start', None)
    end     = meta.get('end', None)


    if action == 'nuke':
        try:
            shutil.rmtree(rootdir)
        except OSError as e:
            print("Error: %s : %s" % (dir_path, e.strerror))

        print(f"Deleted everything in {rootdir}")
        return

    for output in meta['outputs']:
        outputs[ output ] = meta['outputs'][output]

    wf_keep_files = ["rc","stdout.submit", "stderr.submit", "script",
                     "stdout", "stderr", "script.submit" ]


    for call in meta['calls']:
        for shard in meta['calls'][call]:
            shard_status = shard.get('executionStatus', None)
            shard_start  = shard.get('start', None)
            shard_end  = shard.get('start', None)
            shard_rootdir = shard.get('callRoot', None)
            shard_outputs = shard.get('outputs', {})


            if done_only and  shard_status != 'Done':
                print(f"keeping {shard_rootdir} as status is {shard_status} ")
                continue

            if shard_end_ts < datetime.now()- timedelta(hours=24):
                print( "Keeping call folder, not old enough!")
                continue

            if action == 'tmpfiles':
                delete_workflow_files( shard_rootdir, list(shard_outputs.values()) + wf_keep_files)
            elif action == 'files':
                delete_workflow_files( shard_rootdir, wf_keep_files)
            else:
                raise RuntimeError(f'{action} is an unknown cleanup action, allowed: tmpfiles, files or nuke')


def delete_workflow_files(root_dir:str, keep_list:list) -> None:
    if root_dir is None:
        return 

    for root, dirs, files in os.walk(root_dir):
        for filename in files:
            filepath = f"{root}/{filename}"
            if filename in keep_list or filepath in keep_list:
                print(f"Keeping {filename}")
                continue

            print(f"Deleting {filename}")
            os.unlink(filepath)


def cleanup(action:str, ids:list=None, time_type:str=None, time_span:str=None) -> None:

    if ids is None:
        if time_type == 'days':
            from_date = datetime_utils.to_string( datetime.now(pytz.utc) - timedelta(days=int(time_span)) )
        elif sub_command == 'hours':
            from_date = datetime_utils.to_string( datetime.now(pytz.utc) - timedelta(hours=int(time_span)) )
        workflows = cromwell_facade.workflows(from_date=from_date, as_json=as_json, query=True)
        ids = []
        for workflow in workflows:
            ids.append(workflow['id'])


    for id in ids:
        cleanup_workflow(action, id)
