import os
import xml.etree.ElementTree as ET
import argparse
import requests
from datetime import datetime
from urllib import request
import urllib3
import time
from action_functions import * 

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

VALID_MODES = ['upload', 'download', 'list','browse']

END_STATES = ['Success', 'Failed']


def GetRequestStatus(cloudConfigDetails, requestId):
    url = f"http://{cloudConfigDetails['hostname']}:{cloudConfigDetails['port']}/xen/export/{requestId}"
    headers = { 'Content-Type': 'application/json; charset=utf-8'}
    print (f'In GetRequestStatus - URL = {url}, headers = {headers}')
    response = requests.get(url, headers=headers).json()
    if response.status_code != 200:
        return f"Response error. Status - {response.status_code}, Error - {response.text}"
    response = response.json()
    return response


def GetAllObjects(cloudConfigDetails,recursive=None):
    url = f"http://{cloudConfigDetails['hostname']}:{cloudConfigDetails['port']}/xen/export"
    params = {
            'path': cloudConfigDetails["foldername"],
            'recursive': recursive
            }
    
    print (f'In GetAllObjects - URL = {url}, params = {params}')
    response = requests.get(url, params=params).json()
    if response.status_code != 200:
        return f"Response error. Status - {response.status_code}, Error - {response.text}"
    response = response.json()
    return response


def GetObjectDict(data : dict,params):
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

    for result in data['requestResults']:
        mtime_struct = datetime.strptime(result['Creation'], "%m/%d/%Y %H:%M:%S") 
        atime_struct = datetime.strptime(result['Last-Accessed'], "%m/%d/%Y %H:%M:%S")
        mtime_epoch_seconds = int(mtime_struct.timestamp())
        atime_epoch_seconds = int(atime_struct.timestamp())
        file_path = result["File-Path"]
        file_name = get_filename(file_path)
        file_size = result["File-Size"] if result["File-Type"] == "File" else "0"
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
            file_object["type"] = "F_REG" if file_type == "file" else "F_DIR"
            file_object["mtime"] = f'{mtime_epoch_seconds}'
            file_object["atime"] = f'{atime_epoch_seconds}'
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


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', required = True, help = 'Configuration name')
    parser.add_argument('-m', '--mode', required = True, help = 'upload,browse,download,list,actions')
    parser.add_argument('-t','--target',help='target_path')
    parser.add_argument('-f','--foldername',help='folder_name_to_create')
    parser.add_argument('-ft', '--filtertype', required=False, choices=['none', 'include', 'exclude'], help='Filter type')
    parser.add_argument('-ff', '--filterfile', required=False, help='Extension file')
    parser.add_argument('-pf', '--policyfile', required=False, help='Policy file')
    
    args = parser.parse_args()
    mode = args.mode
    target_path = args.target
    folder_name = args.foldername

    config_map = loadConfigurationMap(args.config)
    # config_map = {'hostname': '192.168.1.172',
    #                      'port': 8000 
    #             }

    params_map = {}
    params_map["foldername"] = args.foldername
    params_map["target"] = args.target
    params_map["filtertype"] = args.filtertype
    params_map["filterfile"] = args.filterfile
    params_map["policyfile"] = args.policyfile

    for key in config_map:
        if key in params_map:
            print(f'Skipping existing key {key}')
        else:
            params_map[key] = config_map[key]

    if mode == 'actions':
        print('browse,list')
        exit(0)

    if mode == 'list':
        if target_path is None or folder_name is None:
            print('Target path (-t <targetpath> ) and folder name (-f <foldername> ) options are required for list')
            exit(1)

        all_objs_list = GetAllObjects(params_map,recursive='true')
        print(all_objs_list)
        if not all_objs_list['requestId']:
            exit(-1)

        requestId = all_objs_list['requestId'] 
        state_name = ""
        while state_name not in END_STATES:
            listing_json_responce = GetRequestStatus(params_map,requestId)
            state_name = listing_json_responce['requestStatus']
            print(state_name)
            time.sleep(5)

        objects_dict = GetObjectDict(listing_json_responce,params_map)
        if objects_dict and target_path:
            generate_xml_from_file_objects(objects_dict, target_path)
            print(f"Generated XML file: {target_path}")
            exit(0)
        else:
            print("Failed to generate XML file.")
            exit(1)


    elif mode == 'browse':
        if folder_name is None:
            print('Folder name (-f <foldername> ) options are required for browse.')
            exit(1)

        folders = set()
        all_objs_list = GetAllObjects(params_map,recursive='false')
        requestId = all_objs_list['requestId'] 
        state_name = ""
        while state_name not in END_STATES:
            data = GetRequestStatus(params_map,requestId)
            state_name = data['requestStatus']
            print(state_name)
            time.sleep(1)
            
        if data['requestStatus'] == "Success":
            for item in data['requestResults']:
                path =os.path.normpath(folder_name)  
                File_Path = os.path.normpath(item['File-Path'])

                if item['File-Type'] == 'Folder' and File_Path.startswith(path):
                    difference = File_Path[len(path):]
                    split_values  = difference.split("\\")
                    split_values = [value for value in split_values if value]
                    if split_values:
                        folders.add(split_values[0])

        folders = list(folders)
        xml_output = add_CDATA_tags(folders)
        print(xml_output)
        exit(0)


        

