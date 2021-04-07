from requests import Request, Session

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



def get_version() -> []:
    r, _ = requests_utils.get(f"{protocol}://{base_url}/engine/v1/version")
    return r.get("cromwell", None)


def get_status() -> []:
    r, _ = requests_utils.get(f"{protocol}://{base_url}/engine/v1/status")
    if r == {}:
        return "Unknown"
    return r['serviceName']['ok'], r['serviceName']['messages']



