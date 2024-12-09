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

def strdata_to_logging_file(str_data, filename):
    f = open(filename, "a")
    f.write(f'{str_data}\n')
    f.close()


def GetRequestStatus(requestId):
    url = f"http://{params_map['hostname']}:{params_map['port']}/xen/export/{requestId}"
    headers = { 'Content-Type': 'application/json; charset=utf-8'}
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        if logging_dict["logging_level"] > 0:
            strdata_to_logging_file(url, logging_dict["logging_error_filename"])
        print(f"Response error. Status - {response.status_code}, Error - {response.text}")
        return False
    elif logging_dict["logging_level"] > 1:
        strdata_to_logging_file(url, logging_dict["logging_filename"])
    response = response.json()
    return response


def GetAllObjects(folder_name,recursive=None):
    url = f"http://{params_map['hostname']}:{params_map['port']}/xen/export"
    params = {
            'path': folder_name,
            'recursive': recursive
            }
    response = requests.get(url, params=params)
    if response.status_code != 200:
        if logging_dict["logging_level"] > 0:
            strdata_to_logging_file(url, logging_dict["logging_error_filename"])
        print(f"Response error. Status - {response.status_code}, Error - {response.text}")
        return False
    elif logging_dict["logging_level"] > 1:
        strdata_to_logging_file(url, logging_dict["logging_filename"])
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
            file_object["index"] = params["indexid"]
            
            if file_object["type"] == "F_REG":
                scanned_files += 1
                selected_count += 1
                total_size += int(file_object["size"])
            file_object_list.append(file_object)
            
        output["filelist"] = file_object_list

    output["scanned_count"] = scanned_files
    output["selected_count"] = selected_count
    output["total_size"] = total_size
    output["filelist"] = file_object_list

    return output


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', required = True, help = 'Configuration name')
    parser.add_argument('-m', '--mode', required = True, help = 'browse,list,actions')
    parser.add_argument('-t','--target',help='target_path')
    parser.add_argument('-f','--foldername',help='folder_name_to_create')
    parser.add_argument('-in', '--indexid', required=False, help = 'REQUIRED if list')
    parser.add_argument('-jg', '--jobguid', required=False, help = 'REQUIRED if list')
    parser.add_argument('-ji', '--jobid', required=False, help = 'REQUIRED if bulk restore.')
    parser.add_argument('-p', "--projectname", required=False, help = 'Project name')
    
    args = parser.parse_args()
    mode = args.mode
    target_path = args.target
    folder_name = args.foldername

    logging_dict = loadLoggingDict(os.path.basename(__file__), args.jobguid)
    config_map = loadConfigurationMap(args.config)
    filter_file_dict = loadFilterPolicyFiles (args.jobguid)
    

    params_map = {}
    params_map["foldername"] = args.foldername
    params_map["target"] = args.target
    params_map["indexid" ] = args.indexid
    params_map["jobguid"] = args.jobguid
    params_map["jobid"] = args.jobid
    
    params_map["filtertype"] = filter_file_dict["type"]
    params_map["filterfile"] = filter_file_dict["filterfile"]
    params_map["policyfile"] = filter_file_dict["policyfile"]

    for key in config_map:
        if key in params_map:
            print(f'Skipping existing key {key}')
        else:
            params_map[key] = config_map[key]

    if mode == 'actions':
        try:
            if params_map["actions"]:
                print(params_map["actions"])
                exit(0)
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            exit(1)
            
    if folder_name:
        folder = os.path.join(params_map["default_list_path"],folder_name)
    else:
        folder = params_map["default_list_path"]
        
    if mode == 'list':
        if target_path is None or args.indexid is None:
            print('Target path (-t <targetpath> ) and -in <index>  options are required for list')
            exit(1)
        all_objs_list = GetAllObjects(folder,recursive='true')
        if not all_objs_list['requestId']:
            exit(1)

        requestId = all_objs_list['requestId'] 
        state_name = ""
        while state_name not in END_STATES:
            listing_json_responce = GetRequestStatus(requestId)
            state_name = listing_json_responce['requestStatus']
            time.sleep(5)

        objects_dict = GetObjectDict(listing_json_responce,params_map)
        if len(listing_json_responce["requestResults"]) == 0:
            objects_dict = {}
        
        if objects_dict and target_path:
            generate_xml_from_file_objects(objects_dict, target_path)
            print(f"Generated XML file: {target_path}")
            exit(0)
        else:
            print("Failed to generate XML file.")
            exit(1)


    elif mode == 'browse':
        folders = []
        all_objs_list = GetAllObjects(folder,recursive='false')
        requestId = all_objs_list['requestId'] 
        state_name = ""
        while state_name not in END_STATES:
            data = GetRequestStatus(requestId)
            state_name = data['requestStatus']
            time.sleep(1)
            
        if data['requestStatus'] == "Success":
            for item in data['requestResults']:
                if item['File-Type'] == 'Folder':
                    folders.append(get_filename(item['File-Path']))

        xml_output = add_CDATA_tags(folders)
        print(xml_output)
        exit(0)

    else:
        print(f'Unsupported mode {mode}')
        exit(1)

        

