
import re
import sys
import json
import tabulate

import kbr.args_utils as args_utils

import whittle.cromwell_api as cromwell_api



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

def workflows(from_date:str=None, to_date:str=None, status:[]=None, names:[]=None, ids:[]=None, query:bool=False, as_json:bool=False) -> None:
    data = []

    filter = {}

    if from_date is not None:
        from_date = first_element_or_default(from_date)
        filter['start'] = from_date

    if to_date is not None:
        to_date = first_element_or_default(to_date)
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

    if ids is not None:
        if query:
            filter['id'] = first_element_or_default(ids)
        else:
            for i in ids:
                data.append({'id': i})

    if query and filter != {}:
        data = [filter]

    st = cromwell_api.workflows(data)

    res = [["id", "name", "status", "submitted", "started", "ended"]]
    jsons = []

    if 'results' in st:
        for r in st['results']:
            res.append([ r['id'], r.get('name','NA'), r['status'], r['submission'], r.get('start', 'NA'), r.get('end', 'NA')])
            jsons.append( r )

    else:
        if as_json:
            print(json.dumps(st))
        else:
            print(f"Query error: {st['status']}")

        sys.exit(10)

    if as_json:
        print(json.dumps(jsons))
    else:
        print( tabulate.tabulate(res, headers="firstrow", tablefmt='psql'))
        
