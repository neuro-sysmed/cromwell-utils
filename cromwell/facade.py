
import re
import os
import shutil
import sys
import json
from datetime import datetime, timedelta
import tabulate
import tempfile
import pytz

import kbr.args_utils as args_utils
import kbr.datetime_utils as datetime_utils
import kbr.file_utils as file_utils
import kbr.string_utils as string_utils

import cromwell.api as cromwell_api



def group_args(args) -> dict:

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


def write_to_tmpfile(data) -> str:

    if data is None:
        return None

    tmpfile = tempfile.NamedTemporaryFile(mode="w", delete=False)
    tmpfile.write( data)
#    json.dump(data, tmpfile.file)
    tmpfile.close()

    return tmpfile.name

def del_files(*files) -> None:
    for f in files:
        if f is not None and os.path.isfile( f ):
            os.remove(f)


def resubmit_workflows(args:list, wdl_zip:str=None, as_json:bool=False) -> None:

    for wf_id in args:
        wf_meta = cromwell_api.workflow_meta(wf_id=wf_id)

        options  = wf_meta['submittedFiles'].get('options', None)
        labels   = wf_meta['submittedFiles'].get('labels', None)
        inputs   = wf_meta['submittedFiles'].get('inputs', None)
        workflow = wf_meta['submittedFiles'].get('workflow' , None)

        tmp_options = write_to_tmpfile( options )
        tmp_labels  = write_to_tmpfile( labels )
        tmp_inputs  = write_to_tmpfile( inputs )
        tmp_wf_file = write_to_tmpfile( workflow )

        st = cromwell_api.submit_workflow(wdl_file=tmp_wf_file, inputs=[tmp_inputs], options=tmp_options, labels=tmp_labels, dependency=wdl_zip)
        print(f"{st['id']}: {st['status']}")
        del_files( tmp_inputs, tmp_options, tmp_labels, tmp_wf_file)
    



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

def workflow_labels_set(wf_id:str, args:list, as_json:bool=False, quiet:bool=False) -> None:

    data  = {}
    for label in args:
        key, value = label.split(":")        
        data[key] = value

    st = cromwell_api.workflow_labels_set(wf_id, data)

    if quiet:
        return

    if as_json:
        print(json.dumps(st))
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

def workflow_outputs(args:list, as_json:bool=False) -> None:

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





def export_workflow_outputs(args:list, outdir:str=".") -> None:

    args_utils.min_count(1, len(args), 1, msg="one or more workflow id is required")
    for wf_id in args:
        st = cromwell_api.workflow_outputs(wf_id)
        if outdir is None:

            labels = cromwell_api.workflow_labels_get(wf_id)
            if 'outdir' in st['labels']:
                print('taking outdir from label')
                outdir = st['labels']['outdir']
            else:
                print('Exporting to cwd')
                outdir = os.getcwd()

        if 'status' in st:
            print(f'Cannot export output files for {st["id"]} as status is {st["status"]}')
        else:
            for output in st['outputs']:
                _, name = output.split(".")
                if not isinstance(st['outputs'][output], list):
                    st['outputs'][output] = [st['outputs'][output]]

                for of in st['outputs'][output]:
                    if of is None:
                        continue
                    
                    if not os.path.isfile( of ):
                        print(f"{of} no longer on disk...")
                        continue

                    print( of )
                    if of is None:
                        continue
                    outfile = re.sub(r'.*\/execution/', '', of)
                    outfile = re.sub(r'^./', '', outfile)
                    if os.path.isfile( f"{outdir}/{outfile}" ):
                        if file_utils.size(of) == file_utils.size(f"{outdir}/{outfile}" ):
                            print(f"{outfile} is already present in {outdir} and files have the same size")
                        else:
                            print(f"{outfile} is already present in {outdir} and files have differnt sizes")
                        continue

                    print (f"Moving {of} -- > {outdir}/{outfile}")
                    shutil.move(of, f"{outdir}/{outfile}")
                    cromwell_api.workflow_labels_set(wf_id=wf_id, data={'exported': True})


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

def workflows(from_date:str=None, to_date:str=None, status:list=None, names:list=None, ids:list=None, labels:list=None, 
              query:bool=False, as_json:bool=False) -> None:
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

            if 'parentWorkflowId' in r:
                continue

            jsons.append( r )
    else:
        if as_json:
            print(json.dumps(st))
        else:
            print(f"Query error: {st['status']}")

        sys.exit(10)

    return jsons
        

def cleanup_workflow(action:str, wf_id:str, keep_running_wfs:bool=True) -> None:
    outputs = {}

    meta = cromwell_api.workflow_meta(wf_id)
    rootdir = meta.get('workflowRoot', None)
    status  = meta.get('status', None)
    start   = meta.get('start', None)
    end     = meta.get('end', None)

    workflow_stats = ['Submitted', 'Running', 'Aborting', 'Failed', 'Succeeded', 'Aborted']


    if action == 'nuke':
        try:
            shutil.rmtree(rootdir)
        except OSError as e:
            print("Error: %s : %s" % (rootdir, e.strerror))

        print(f"Deleted everything in {rootdir}")
        return

    # collect output files
    if 'outputs' in meta:
        for output in meta['outputs']:
            outputs[ output ] = meta['outputs'][output]

    # worth keeping these for long term tracking
    wf_keep_files = ["rc","stdout.submit", "stderr.submit", "script",
                     "stdout", "stderr", "script.submit" ]

    kf = cromwell_api.workflow_outputs(wf_id)
    output_files = []
    for kf in list(kf['outputs'].values()):
        if isinstance(kf, list):
            output_files += list(kf)
        elif kf is not None:
            output_files.append( kf )


    for call in meta['calls']:
        for shard in meta['calls'][call]:
            shard_status = shard.get('executionStatus', None)
            shard_rootdir = shard.get('callRoot', None)

            if keep_running_wfs and shard_status not in ['Submitted', 'Running', 'Aborting']:
                print(f"keeping {shard_rootdir} as status is {shard_status} ")
                continue

            if action == 'tmpfiles':
                delete_workflow_files( shard_rootdir, keep_list=list(output_files + wf_keep_files))
            elif action == 'files':
                delete_workflow_files( shard_rootdir, keep_list=wf_keep_files)
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


def cleanup(action:str, ids:list=None, time_type:str=None, time_span:str=None, ) -> None:

    keep_running = False

    if ids is None:
        keep_running = True
        if time_type == 'days':
            to_date = datetime_utils.to_string( datetime.now(pytz.utc) - timedelta(days=int(time_span)) )
        elif time_type == 'hours':
            to_date = datetime_utils.to_string( datetime.now(pytz.utc) - timedelta(hours=int(time_span)) )

        workflows_data = workflows(to_date=to_date, as_json=True, query=True)
        ids = []
        for workflow in workflows_data:
            ids.append(workflow['id'])


    for id in ids:
        cleanup_workflow(action=action, wf_id=id, keep_running_wfs=keep_running)
        workflow_labels_set(id, [f"cleanup:{action}"])


def directory_size(start_path:str = '.') -> int:
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(start_path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            total_size += os.path.getsize(fp)
    return total_size

def wf_dirsizes(ids:list=None, time_type:str=None, time_span:str=None, ) -> None:


    if ids is None and time_type:
        if time_type == 'days':
            to_date = datetime_utils.to_string( datetime.now(pytz.utc) - timedelta(days=int(time_span)) )
        elif time_type == 'hours':
            to_date = datetime_utils.to_string( datetime.now(pytz.utc) - timedelta(hours=int(time_span)) )

        workflows_data = workflows(to_date=to_date, as_json=True, query=True)
        ids = []
        for workflow in workflows_data:
            ids.append(workflow['id'])

    elif ids is None:
        workflows_data = workflows(as_json=True, query=True)

        ids = []
        for workflow in workflows_data:
            ids.append(workflow['id'])

    res = [["id", "name", "size", "path"]]
    for id in ids:
        workflow = cromwell_api.workflow_meta(wf_id = id )
        print( workflow )
        wf_rootdir = workflow['workflowRoot']
        size = directory_size( wf_rootdir )
        res.append([id, string_utils.readable_bytes(size), 
                    workflow['workflowName'], wf_rootdir])


    print( tabulate.tabulate(res, headers="firstrow", tablefmt='psql'))


def workflow_fails(args:list, as_json:bool=False) -> None:

    args_utils.min_count(1, len(args), 1, msg="one or more workflow id is required")
    jsons = []
    for wf_id in args:
        st = cromwell_api.workflow_meta(wf_id)

        if 'calls' in st:
            for call in st['calls']:
                for shard in st['calls'][call]:
                    if "executionStatus" in shard and shard['executionStatus'] == 'Failed':                        
                        print(f"{wf_id}\t{call}")

        if False and 'failures' in st:
            for failure in st['failures']:
                if "causedBy" in failure:
                    for cb in failure[ 'causedBy' ]:
                        print(cb['message'])


#    if as_json:
#        print(json.dumps(jsons))



def workflow_overview(args:list, as_json:bool=False) -> None:

    args_utils.min_count(1, len(args), 1, msg="one or more workflow id is required")
    jsons = []
    res = [["id", "name", "status", ]]
    for wf_id in args:
        st = cromwell_api.workflow_meta(wf_id)

        if 'calls' in st:
            for call in st['calls']:
                for shard in st['calls'][call]:
                    res.append([wf_id, call,shard['executionStatus']])
#                   print(f"{wf_id}\t{call}\t{shard['executionStatus']}")

                    jsons.append( { 'id':wf_id, "call":call,"status":shard['executionStatus']})



    if as_json:
        print(json.dumps(jsons))
    else:
        print( tabulate.tabulate(res, headers="firstrow", tablefmt='psql'))


