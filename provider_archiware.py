import json
import os
import csv
import xml.etree.ElementTree as ET
from http.server import BaseHTTPRequestHandler, HTTPServer
import argparse
import requests
import sys
from configparser import ConfigParser
import plistlib
import urllib.parse
from urllib import request
import urllib3
import time
import xml.etree.ElementTree as ET
from action_functions import * 

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

VALID_MODES = ['upload', 'download', 'list']

VALID_END_STATES = ['success', 'exception', 'failure']

def strdata_to_logging_file(str_data, filename):
    f = open(filename, "a")
    f.write(f'{str_data}\n')
    f.close()

def csv_to_json(csv_file_path):
    entries = []

    # Read the CSV file
    with open(csv_file_path, mode='r', newline='') as csvfile:
        csv_reader = csv.reader(csvfile)
        
        # Iterate over each row in the CSV reader
        for row in csv_reader:

            entry = {
                "ID": f"{row[0]}",
                "targetPath": f"{row[1]}"
            }
            entries.append(entry)

    # Create the final JSON structure
    json_data = {
        "entries": entries
    }

    # Convert the JSON data to a JSON string with proper formatting
    json_output = json.dumps(json_data,indent=4)
    return json_output


def archiware_to_object_array(txt_file,params):
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

    with open(txt_file, 'r') as file:
        for line in file:
            parts = line.strip().split('\t')

            file_size = parts.pop()
            parts.pop().strip() #we don't want this field, so we pop and skip it.
            mtime_epoch_seconds = parts.pop()
            tmp_id = parts.pop()
            #checksum = id.replace(f"{indexId}#","").strip()
            file_path = parts.pop()
            file_name = get_filename(file_path)
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
            if include_file:
                file_object["name"] = file_path
                file_object["size"] = file_size
                file_object["mode"] = "0"
                file_object["tmpid"] = tmp_id
                #file_object["checksum"] = checksum
                file_object["type"] = "F_REG" if file_type == "file" else "F_DIR"
                file_object["mtime"] = f'{mtime_epoch_seconds}'
                file_object["atime"] = f'{mtime_epoch_seconds}'
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
    

def list_index():
    url = f"http://{params_map['hostname']}:{params_map['port']}/rest/v1/archive/indexes/{params_map["indexid"]}"
    
    file_name = f"{params_map["jobguid"]}-archiware.txt"
        
    default_path = params_map['default_list_path']
    default_path = default_path.replace('"','')
    default_path = default_path.strip()
    filepath = f"{default_path}/{file_name}"
    filepath = filepath.replace("//","/")

    filename = f'{params_map["client_machine"]}:{filepath}'

    attributes = ""
    if not params_map['attributes'] is None and params_map['attributes'] != "":
        attributes = params_map['attributes']
        attributes = attributes.replace('"','')
        attributes = attributes.strip()

    if attributes != "":
        headers = {
            "filename": filename,
            "attributes": attributes,
            "Content-Type": "application/json"
        }
    else:
        headers = {
            "filename": f"{filename}",
            "Content-Type": "application/json"
        }

    response = requests.put(url, headers=headers, auth=(f"{params_map['username']}", f"{params_map['password']}"))
    
    if response.status_code != 200:
        if logging_dict["logging_level"] > 0:
            strdata_to_logging_file(url, logging_dict["logging_error_filename"])
        print(f"Response error. Status - {response.status_code}, Error - {response.text}")
        return False
    elif logging_dict["logging_level"] > 1:
        strdata_to_logging_file(url, logging_dict["logging_filename"])

    filepath = filepath.replace("/host_dir","")
    return filepath

def restore_request_call(csv_file):
    url = f"http://{params_map['hostname']}:{params_map['port']}/rest/v1/restore/restoreselections"
    body = csv_to_json(csv_file)
    
    headers = {
        "client" : f"{params_map['client_machine']}",
        "Content-Type": "application/json"
    }
    
    response = requests.post(url, headers=headers, params=body, auth=(params_map['username'], params_map['password']))
    if response.status_code != 200:
        if logging_dict["logging_level"] > 0:
            strdata_to_logging_file(url, logging_dict["logging_error_filename"])
        print(f"Response error. Status - {response.status_code}, Error - {response.text}")
        return False
    elif logging_dict["logging_level"] > 1:
        strdata_to_logging_file(url, logging_dict["logging_filename"])

    json_resp = response.json()
    return_id = json_resp['ID']
    links_list = json_resp['links']

    return return_id


def get_progress_status(job_id : str):
    url = f"http://{params_map['hostname']}:{params_map['port']}/rest/v1/general/jobs/{job_id}"
    header = {'Content-Type': 'application/json'}

    response = requests.get(url, headers=header, auth=(params_map['username'], params_map['password']))
    if response.status_code != 200:
        if logging_dict["logging_level"] > 0:
            strdata_to_logging_file(url, logging_dict["logging_error_filename"])
        print(f"Response error. Status - {response.status_code}, Error - {response.text}")
        return False
    elif logging_dict["logging_level"] > 1:
        strdata_to_logging_file(url, logging_dict["logging_filename"])
    
    json_resp = response.json()
    return json_resp


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', required = True, help = 'Configuration name')
    parser.add_argument('-m', '--mode', required = True, help = 'upload,browse,download,list,actions')
    parser.add_argument('-s','--source',help='source file')
    parser.add_argument('-t','--target',help='target_path')
    parser.add_argument('-id','--collection_id',help='collection_id')
    parser.add_argument('-tmp','--tmp_id',help='tmp_id')
    parser.add_argument('-in', '--indexid', required=False, help = 'REQUIRED if list')
    parser.add_argument('-jg', '--jobguid', required=False, help = 'REQUIRED if list')
    parser.add_argument('-ji', '--jobid', required=False, help = 'REQUIRED if bulk restore.')
    parser.add_argument('--progressfile', required=False, help='Progress file for restore')
    parser.add_argument('--restoreticketpath', required=False, help='Restore ticket')
    parser.add_argument('-p', "--projectname", required=False, help = 'Project name')
        
    args = parser.parse_args()
    mode = args.mode
    target_path = args.target
    restore_ticket_path = args.restoreticketpath
    progress_file = args.progressfile
    job_guid = args.jobguid

    
    logging_dict = loadLoggingDict(os.path.basename(__file__), args.jobguid)
    config_map = loadConfigurationMap(args.config)
    filter_file_dict = loadFilterPolicyFiles (args.jobguid)

    params_map = {}
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

    if mode == "list":
        if target_path is None or args.indexid is None:
            print('Target path (-t <targetpath> ) -in <index>  options are required for list')
            exit(1)
            
        txt_file_path = list_index()
        if os.path.exists(txt_file_path):
            objects_dict = archiware_to_object_array(txt_file_path,params_map)
        else:
            objects_dict ={}
            
        if objects_dict and target_path:
            generate_xml_from_file_objects(objects_dict, target_path)
            print(f"Generated XML file: {target_path}")
            exit(0)
        else:
            print("Failed to generate XML file.")
            exit(1)
    
    elif mode == "bulkrestore":
        if args.restoreticketpath is None or args.progressfile is None:
            print('Restore ticket path (--restoreticketpath ) and Progress file path (--progressfile) options are required for bulkrestore')
            exit(1)

        current_time = int(time.time())
        restore_csv = restore_ticket_to_csv(restore_ticket_path, current_time)
        return_id = restore_request_call(restore_csv)

        if return_id:
            job_stat = ""
            progress_dict = {}
            progress_dict["duration"] = current_time
            progress_dict["run_id"] = job_guid
            progress_dict["job_id"] = job_guid
            progress_dict["progress_path"] = progress_file
            progress_dict["processedBytes"] = 0
            progress_dict["processedFiles"] = 0
            current_record_index = 0
            progress_resp = {}
            while job_stat not in VALID_END_STATES:
                current_record_index = current_record_index+1
                progress_resp = get_progress_status(return_id)
                job_stat = progress_resp["completion"]
                progress_dict["totalFiles"] = int(progress_resp["totalfiles"])
                progress_dict["totalSize"] = int(progress_resp["totalkbytes"]) * 1024
                progress_dict["processedFiles"] = int(progress_resp["totalfiles"])
                progress_dict["processedBytes"] = int(progress_resp["totalkbytes"]) * 1024
                progress_dict["status"] = f'{progress_dict["status"]} : {progress_dict["description"]}'

                if (current_record_index % 2) == 0:
                    send_progress(progress_dict, current_record_index)

            if job_stat == "exception" or job_stat == "failure":
                progress_dict["totalFiles"] = int(progress_resp["totalfiles"])
                progress_dict["totalSize"] = int(progress_resp["totalkbytes"]) * 1024
                progress_dict["processedFiles"] = int(progress_resp["totalfiles"])
                progress_dict["processedBytes"] = int(progress_resp["totalkbytes"]) * 1024
                progress_dict["status"] = f'{progress_dict["status"]} : {progress_dict["description"]}'
                send_progress(progress_dict, 1)
                exit(-1)
        else:
            print("Restore Files request Failed.")
            exit(1)
        
    else:
        print(f'Unsupported mode {mode}')
        exit(1)