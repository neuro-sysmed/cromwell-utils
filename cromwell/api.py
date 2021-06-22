import sys
from requests import Request, Session, HTTPError

import json
import kbr.requests_utils as requests_utils

base_url = "localhost:8000"
protocol = 'http'


def init(host:str="localhost:8000", p='http'):
    global base_url, protocol
    base_url = host
    protocol = p
    


#def get_user_history_imports(self, user_id:str) -> []:
#    return self._request_get(f"{self._base_url}/user/{user_id}/imports")

#def get_history_exports(self, filter:{}={}) -> []:
#    return self._request_get(f"{self._base_url}/history/exports", data=filter)


def handle_exception(wf_id:str, error_code:int) -> dict:
        if error_code == 400:
            return {'id':wf_id, 'status': 'malformed id'}
        if error_code == 403:
            return {'id':wf_id, 'status': 'in terminal state'}
        if error_code == 404:
            return {'id':wf_id, 'status': 'id not found'}
        if error_code == 405:
            print("Method now allowed check method/url")
            sys.exit(1)
        if error_code == 500:
            return {'id':wf_id, 'status': 'Internal error'}



def get_version() -> str:
    r, _ = requests_utils.get(f"{protocol}://{base_url}/engine/v1/version")
    return r.get("cromwell", None)


def get_status() -> str:
    r, _ = requests_utils.get(f"{protocol}://{base_url}/engine/v1/status")
    if r == {}:
        return "Unknown"
    return r['serviceName']['ok'], r['serviceName']['messages']



def submit_workflow(wdl_file:str, inputs:list=[], options:str=None, dependency:str=None, labels:str=None) ->list:

    data = {'version':'v1',
            }

    files = {'workflowSource': (wdl_file, open(wdl_file, 'rb'), 'application/octet-stream')}

    if inputs != []:
        files[f'workflowInputs'] = (inputs[0], open(inputs[0], 'rb'), 'application/json')
        for i, v in enumerate( inputs ):
            if i == 0:
                continue
            files[f'workflowInputs_{i+1}'] = (v, open(v, 'rb'), 'application/json')

    if options is not None:
        files['workflowOptions'] = (options, open(options, 'rb'), 'application/json')

    if dependency is not None:
        files['workflowDependencies'] = (dependency, open(dependency, 'rb'), 'application/zip')

    if labels is not None:
        files['labels'] = (labels, open(labels, 'rb'), 'application/json')

    try:
        r, _ = requests_utils.post(f"{protocol}://{base_url}/api/workflows/v1", data=data, files=files)
        return r
    except HTTPError as e:
        return handle_exception("wf_id", e.response.status_code)


def batch_submit_workflow(wdl_file:str, inputs:str, options:str=None, dependency:str=None, labels:str=None) -> list:

    data = {'version':'v1',
            'workflowSource': wdl_file,
            'workflowInputs': inputs}

    files = {'workflowSource': (wdl_file, open(wdl_file, 'rb'), 'application/octet-stream'),
             'workflowInputs': (inputs,   open(inputs, 'rb'),'application/json')}

    if options is not None:
        files['workflowOptions'] = (options, open(options, 'rb'), 'application/json')

    if dependency is not None:
        files['workflowDependencies'] = (dependency, open(dependency, 'rb'), 'application/zip')

    if labels is not None:
        files['labels'] = (labels, open(labels, 'rb'), 'application/json')

    try:
        r, _ = requests_utils.post(f"{protocol}://{base_url}/api/workflows/v1/batch", data=data, files=files)
        return r
    except HTTPError as e:
        return handle_exception("wf_id", e.response.status_code)


def workflow_status(wf_id) -> list:
    try:
        r, _ = requests_utils.get(f"{protocol}://{base_url}/api/workflows/v1/{wf_id}/status")
        return r
    except HTTPError as e:
        return handle_exception(wf_id, e.response.status_code)


def workflows_status(wf_id) -> list:
    try:
        r, _ = requests_utils.get(f"{protocol}://{base_url}/api/workflows/v1/query")
        return r
    except HTTPError as e:
        return handle_exception(wf_id, e.response.status_code)


def workflow_abort(wf_id) -> list:
    try:
        r, _ = requests_utils.post(f"{protocol}://{base_url}/api/workflows/v1/{wf_id}/abort")
        return r
    except HTTPError as e:
        return handle_exception(wf_id, e.response.status_code)


def workflow_labels_get(wf_id) -> list:
    try:
        r, _ = requests_utils.get(f"{protocol}://{base_url}/api/workflows/v1/{wf_id}/labels")
        return r
    except HTTPError as e:
        return handle_exception(wf_id, e.response.status_code)

def workflow_labels_set(wf_id, data:dict={}) -> list:
    try:
        r, _ = requests_utils.patch(f"{protocol}://{base_url}/api/workflows/v1/{wf_id}/labels", data=data)
        return r
    except HTTPError as e:
        return handle_exception(wf_id, e.response.status_code)


def workflow_logs(wf_id) -> list:
    try:
        r, _ = requests_utils.get(f"{protocol}://{base_url}/api/workflows/v1/{wf_id}/logs")
        return r
    except HTTPError as e:
        return handle_exception(wf_id, e.response.status_code)

def workflow_outputs(wf_id) -> list:
    try:
        r, _ = requests_utils.get(f"{protocol}://{base_url}/api/workflows/v1/{wf_id}/outputs")
        return r
    except HTTPError as e:
        return handle_exception(wf_id, e.response.status_code)

def workflow_meta(wf_id) -> list:
    try:
        r, _ = requests_utils.get(f"{protocol}://{base_url}/api/workflows/v1/{wf_id}/metadata")
        return r
    except HTTPError as e:
        return handle_exception(wf_id, e.response.status_code)


def workflows(data={}) -> list:
    try:
        r, _ = requests_utils.post(f"{protocol}://{base_url}/api/workflows/v1/query", data=data)
        return r
    except HTTPError as e:
        return handle_exception("na", e.response.status_code)
