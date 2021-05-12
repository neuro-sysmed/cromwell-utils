import json
import pprint as pp


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


def pack(data:dict, level:int) -> list:
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

    if jsons is None:
        return data

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

def print_data(data:dict, pretty_print:bool=False, outfile:str=None):
    if pretty_print:
        if outfile:
            with open(outfile, "w") as outfile:
                outfile.write(pp.pformat(data))
        else:
            pp.pprint(data)
    else:
        if outfile:
            with open(outfile, 'w') as outfile:
                json.dump(data, outfile)
        else:
            print(json.dumps(data))

