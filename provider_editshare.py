import subprocess
import os
import shutil
from datetime import datetime
import xml.etree.ElementTree as ET
import argparse
from action_functions import *
import requests
from urllib import request
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def get_download_id(params,file_id):
    json_body = {
    "file_id": file_id,
    "offset": 0
    }

    # response = requests.post(f"https://{params["hostname"]}:8006/api/v2/transfer",json=json_body)
    # if response.status_code != 201:
    #     print(f"Response error. Status - {response.status_code}, Error - {response.text}")
    #     return False
    response = [
        {
            "expires": "2021-08-18T15:21:57Z",
            "file_id": "null",
            "file_path": "folder/Kittens.mov",
            "file_size_bytes": 8589934592,
            "mediaspace": "Test",
            "name": "Kittens",
            "offset": 0,
            "transfer": "c838cd8f-93bf-49d4-ab4c-5ff745b01468"
        }
    ]

    return response[0]["transfer"]

def download_file(download_id,params):
    para = {
        "transfer_id" : download_id
    }

    response = requests.get(f"https://{params["hostname"]}:8006/api/v2/transfer",params=para)
    if response.status_code != 200:
        print(f"Response error. Status - {response.status_code}, Error - {response.text}")
        return False
    return True

def GetObjectDict(data_list,params):
    scanned_files = 0
    selected_count = 0
    output = {}
    file_object_list = []
    extensions = []
    total_size = 0

    if "filtertype" in params:
        filter_type = params["filtertype"]
    else:
        filter_type = None

    if "filterfile" in params:
        filter_file = params["filterfile"]
    else:
        filter_file = None

    if "policyfile" in params:
        policy_file = params["policyfile"]
    else:
        policy_file = None

    policy_dict = None

    if filter_type is None or filter_file is None:
       filter_type = 'none'

    if filter_type.lower() != 'none':
        if not os.path.isfile(filter_file):
            print(f"Filter file given: {filter_file} not found.")
            return output

        with open(filter_file, 'r') as f:
            extensions = [ext.strip() for ext in f.readlines()]

    if not policy_file is None:
        policy_dict = load_policies_from_file(policy_file)

    
    for data in data_list:
        mtime_struct = datetime.strptime(data["last_modification_date"].split("+")[0], "%Y-%m-%dT%H:%M:%S")
        mtime_epoch_seconds = int(mtime_struct.timestamp())
        file_path = data["file_path"]
        file_name = get_filename(file_path)
        file_size = data["file_size"]
        file_type = "file" if file_size != "0" else "dir"

        if file_type.lower() != 'file'  or filter_type == 'none':
            include_file = True
        elif len(extensions) == 0:
            continue
        else:
            file_in_list = isFilenameInFilterList(file_name, extensions)
            include_file = (file_in_list and filter_type == "include") or  (not file_in_list and filter_type == "exclude") or (filter_type == "none")

        if include_file == False:
            continue

        if not policy_dict is None:
            policy_type = policy_dict["type"]
            policy_entries = policy_dict["entries"]
            
            if policy_type == "ERROR":
                continue

            if policy_type == "NOFILE":
                include_file = True

            elif include_file and len(policy_entries) > 0:
                include_file = file_in_policy(policy_dict, file_name, file_path, file_size, mtime_epoch_seconds)

        file_object = {}
        if include_file == True:
            file_object["name"] = file_path
            file_object["size"] = file_size
            file_object["mode"] = "0"
            file_object["checksum"] = data["checksum"]
            file_object["type"] = "F_REG" if file_type == "file" else "F_DIR"
            file_object["mtime"] = f'{mtime_epoch_seconds}'
            file_object["atime"] = f'{mtime_epoch_seconds}'
            file_object["owner"] = "0"
            file_object["group"] = "0"
            file_object["index"] = "0"
            
            if file_object["type"] == "F_REG":
                scanned_files += 1
                selected_count += 1
                total_size += int(file_object["size"])
            file_object_list.append(file_object)

    output["scanned_count"] = scanned_files
    output["selected_count"] = selected_count
    output["total_size"] = total_size
    output["filelist"] = file_object_list

    return output


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', required = True, help = 'Configuration name')
    parser.add_argument('-m', '--mode', required = True, help = 'upload,browse,download,list,actions')
    parser.add_argument('-s','--source',help='source file')
    parser.add_argument('-t','--target',help='target_path')
    parser.add_argument('-f','--foldername',help='folder_name_to_create')
    parser.add_argument('-tmp','--tmp_id',help='tmp_id')
    parser.add_argument('-ft', '--filtertype', required=False, choices=['none', 'include', 'exclude'], help='Filter type')
    parser.add_argument('-ff', '--filterfile', required=False, help='Extension file')
    parser.add_argument('-pf', '--policyfile', required=False, help='Policy file')

    args = parser.parse_args()
    mode = args.mode
    target_path = args.target
    folder_name = args.foldername

    # config_map = loadConfigurationMap(args.config)
    config_map = {'hostname': '192.168.1.172',
                         'port': 8000 
                }

    params_map = {}
    params_map["foldername"] = args.foldername
    params_map["target"] = args.target
    params_map["filtertype"] = args.filtertype
    params_map["filterfile"] = args.filterfile
    params_map["policyfile"] = args.policyfile


    file_id = 111
    for key in config_map:
        if key in params_map:
            print(f'Skipping existing key {key}')
        else:
            params_map[key] = config_map[key]

    if mode == 'actions':
        print('upload,browse,download,list')
        exit(0)

    if mode == 'list':
        pass

    elif mode == 'upload':
        pass
        
    elif mode == 'browse':
        pass

    elif mode == "download":
        download_id = get_download_id(params_map,file_id)
        if download_file(download_id,params_map):
            print(f"File Downloaded {target_path}")
        

    else:
        print(f'Unsupported mode {mode}')
        exit(1)