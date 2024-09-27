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

linux_dir = "/opt/sdna/bin"
is_linux = 0
if os.path.isdir(linux_dir):
    is_linux = 1
DNA_CLIENT_SERVICES = ''
if is_linux == 1:    
    DNA_CLIENT_SERVICES = '/etc/StorageDNA/DNAClientServices.conf'
    SERVERS_CONF_FILE = "/etc/StorageDNA/Servers.conf"
else:
    DNA_CLIENT_SERVICES = '/Library/Preferences/com.storagedna.DNAClientServices.plist'
    SERVERS_CONF_FILE = "/Library/Preferences/com.storagedna.Servers.plist"


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


def archiware_to_object_array(indexId, txt_file, filter_type, filter_file, policy_file):
    scanned_files = 0
    selected_count = 0
    output = {}
    file_object_list = []
    extensions = []
    total_size = 0

    if filter_type.lower() != 'none':
        if not os.path.isfile(filter_file):
            print(f"Filter file given: {filter_file} not found.")
            return output

        with open(filter_file, 'r') as f:
            extensions = [ext.strip() for ext in f.readlines()]

    policy_dict = load_policies_from_file(policy_file)

    with open(txt_file, 'r') as file:
        for line in file:
            parts = line.strip().split('\t')

            file_size = parts.pop()
            parts.pop().strip() #we don't want this field, so we pop and skip it.
            mtime_epoch_seconds = parts.pop()
            id = parts.pop()
            #checksum = id.replace(f"{indexId}#","").strip()
            file_path = parts.pop()
            file_name = file_path.split('/')[-1]
            file_parent_path = os.path.sep.join(file_path.split('/')[:-1])
            is_dir = file_size == '0'

            if is_dir or filter_type == 'none':
                include_file = True
            elif len(extensions) == 0:
                continue
            else:
                file_in_list = isFilenameInFilterList(file_name, extensions)
                include_file = (file_in_list and filter_type == "include") or (not file_in_list and filter_type == "exclude") or (filter_type == "none")

            if not include_file:
                continue

            policy_type = policy_dict.get("type")
            policy_entries = policy_dict.get("entries")

            if policy_type == "ERROR":
                continue

            if policy_type == "NOFILE":
                include_file = True

            elif include_file and len(policy_entries) > 0:
                include_file = file_in_policy(policy_dict, file_name, file_parent_path, file_size, mtime_epoch_seconds)

            file_object = {}
            if include_file:
                file_object["name"] = file_path
                file_object["size"] = file_size
                file_object["mode"] = "0"
                file_object["tmpid"] = id
                #file_object["checksum"] = checksum
                file_object["type"] = "F_DIR" if file_object["size"] == '0' else "F_REG"
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
    

def list_index(indexId : str, jobGuid : str, cloudConfigDetails, targetScanFile : str, filterType : str, filterFile : str, policyFile : str):
    url = f"http://{cloudConfigDetails['hostname']}:{cloudConfigDetails['port']}/rest/v1/archive/indexes/{indexId}"
    
    file_name = f"{jobGuid}-archiware.txt"
        
    default_path = cloudConfigDetails['default_list_path']
    default_path = default_path.replace('"','')
    default_path = default_path.strip()
    filepath = f"{default_path}/{file_name}"
    filepath = filepath.replace("//","/")

    filename = f'{cloudConfigDetails["client_machine"]}:{filepath}'

    attributes = ""
    if not cloudConfigDetails['attributes'] is None and cloudConfigDetails['attributes'] != "":
        attributes = cloudConfigDetails['attributes']
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

    #print(f"URL = {url}, FILENAME = {filename}, ATTR = {attributes}, HEADER = {headers}, USER = {cloudConfigDetails['username']}, PASS = {cloudConfigDetails['password']}")
    #print(f"URL = {url}, HEADER = {headers}")

    response = requests.put(url, headers=headers, auth=(f"{cloudConfigDetails['username']}", f"{cloudConfigDetails['password']}"))
    if response.status_code != 200:
        return f"Response error. Status - {response.status_code}. Failed to get index inventory"
    
    if not os.path.exists(filepath):
        return "File path not found. Failed to get index inventory"
    
    ''' -------- THIS IS USED FOR TESTING ONLY
    filepath = "/Users/tejkonganda/Downloads/provider_rsync/4000.35.archiware.txt"

    filter_type = "none"
    filter_file = ""
    policy_file = "/Users/tejkonganda/Downloads/provider_rsync/policy_sample_all.txt"
    filter_type = "none"
    filter_file = ""
    #policy_file = "/Users/tejkonganda/Downloads/provider_rsync/policy_sample_all.txt"
    policy_file = "/tmp/policy_sample_all.txt"
    -------------- '''

    parse_result = archiware_to_object_array(indexId, filepath, filterType, filterFile, policyFile)

    if len(parse_result) == 0:
        print("Failed to get index inventory")
        return False

    generate_xml_from_file_objects(parse_result, targetScanFile)
    #print(f"Generated XML file: {targetScanFile}")
    return True


def restore_request_call(csv_file, cloudConfigDetails):
    url = f"http://{cloudConfigDetails['hostname']}:{cloudConfigDetails['port']}/rest/v1/restore/restoreselections"

    body = csv_to_json(csv_file)
    
    headers = {
        "client" : f"{cloudConfigDetails['client_machine']}",
        "Content-Type": "application/json"
    }
    
    response = requests.post(url, headers=headers, params=body, auth=(cloudConfigDetails['username'], cloudConfigDetails['password']))

    if response.status_code != 200:
        return "Failed to get restore request response."
    
    json_resp = response.json()
    return_id = json_resp['ID']
    links_list = json_resp['links']

    return return_id


def get_progress_status(job_id : str, cloudConfigDetails):
    url = f"http://{cloudConfigDetails['hostname']}:{cloudConfigDetails['port']}/rest/v1/general/jobs/{job_id}"
    header = {'Content-Type': 'application/json'}
    response = requests.get(url, headers=header, auth=(cloudConfigDetails['username'], cloudConfigDetails['password']))
    if response.status_code != 200:
        return "Failed to get job progress status."
    
    json_resp = response.json()
    return json_resp


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Archiware functions.")
    parser.add_argument('-m', '--mode', required = True, help = 'upload, download, list')
    parser.add_argument('-c', '--configname', required = True, help = 'Archiware config name to work with in cloud_targets.conf')
    parser.add_argument('-s', '--source', help = 'REQUIRED if upload or download')
    parser.add_argument('-t', '--target', help = 'REQUIRED if upload or download or list. For list, it will be the final scan file.')
    parser.add_argument('-in', '--indexid', help = 'REQUIRED if list')
    parser.add_argument('-ji', '--jobguid', help = 'REQUIRED if list')
    parser.add_argument('-ft', '--filtertype', required=False, choices=['none', 'include', 'exclude'], help='Filter type')
    parser.add_argument('-ff', '--filterfile', required=False, help='Extension file')
    parser.add_argument('-pf', '--policyfile', required=False, help='Policy file')
    parser.add_argument('--progressfile', required=False, help='Progress file for restore')
    parser.add_argument('--restoreticket', required=False, help='Restore ticket')
    
    args = parser.parse_args()

    mode = args.mode
    config_name = args.configname
    job_guid = args.jobguid
    
    cloudTargetPath = ''
    
    if is_linux == 1:
        config_parser = ConfigParser()
        config_parser.read(DNA_CLIENT_SERVICES)
        if config_parser.has_section('General') and config_parser.has_option('General','cloudconfigfolder'):
            section_info = config_parser['General']
            cloudTargetPath = section_info['cloudconfigfolder'] + "/cloud_targets.conf"
    else:
        with open(DNA_CLIENT_SERVICES, 'rb') as fp:
            my_plist = plistlib.load(fp)
            cloudTargetPath = my_plist["CloudConfigFolder"] + "/cloud_targets.conf"
            
    if not os.path.exists(cloudTargetPath):
        err= "Unable to find cloud target file: " + cloudTargetPath
        sys.exit(err)

    config_parser = ConfigParser()
    config_parser.read(cloudTargetPath)
    if not config_name in config_parser.sections():
        err = 'Unable to find cloud configuration: ' + config_name
        sys.exit(err)
        
    cloud_config_info = config_parser[config_name]
    
    if mode == "list":
        index_id = args.indexid
        target_scan_file = args.target
        filter_type = 'none'
        if args.filtertype is not None:
            filter_type = args.filtertype
        filter_file = ''
        if filter_type != 'none':
            if args.filterfile is not None:
                filter_file = args.filterfile
        policy_file = ''
        if args.policyfile is not None:
            policy_file = args.policyfile

        if not list_index(index_id, job_guid, cloud_config_info, target_scan_file, filter_type, filter_file, policy_file):
            exit(-1)
        else:
            exit(0)
    
    elif mode == "bulkrestore":
        restore_ticket = args.restoreticket
        progress_file = args.progressfile

        restore_csv = restore_ticket_to_csv(restore_ticket)

        return_id = restore_request_call(restore_csv, cloud_config_info)

        if return_id == "" or "Failed" in return_id:
            exit(-1)
        else:
            job_stat = ""
            progress_dict = {}
            progress_dict["duration"] = int(time.time())
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