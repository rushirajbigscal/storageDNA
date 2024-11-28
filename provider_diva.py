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
from action_functions import * 

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

VALID_MODES = ['upload', 'download', 'list']

END_STATES = ['COMPLETED', 'ABORTED', 'CANCELLED']

def strdata_to_logging_file(str_data, filename):
    f = open(filename, "a")
    f.write(f'{str_data}\n')
    f.close()

def GenerateBearerToken():
    url = f"http://{params_map['hostname']}:{params_map['port']}/manager/users/login"
    headers = { 'Content-Type': 'application/json; charset=utf-8' }
    body = ('{"username":"%s", "password":"%s"}' % (params_map['username'], params_map['password']))
    response = requests.post(url, headers=headers, data=body.encode("utf-8"), verify=False)
    if response.status_code != 200:
        if logging_dict["logging_level"] > 0:
            strdata_to_logging_file(url, logging_dict["logging_error_filename"])
        print(f"Response error. Status - {response.status_code}, Error - {response.text}")
        return False
    elif logging_dict["logging_level"] > 1:
        strdata_to_logging_file(url, logging_dict["logging_filename"])
    response = response.json()
    return response['token']


def ArchiveFileRequest(bearerToken, sourceFile):
    url = f"http://{params_map['hostname']}:{params_map['port']}/manager/requests/archive"
    headers = { 'Content-Type': 'application/json; charset=utf-8', 'Authorization': f"{bearerToken}" }
    body = ('{"objectName":"%s", "collectionName":"%s", "sourceServer":"%s", "media":"%s", "components":["%s"]}' % (get_filename(sourceFile), params_map['collection_name'], params_map['source_server'], params_map['media_name'], sourceFile))
    response = requests.post(url, headers=headers, data=body.encode("utf-8"), verify=False)
    if response.status_code != 200:
        if logging_dict["logging_level"] > 0:
            strdata_to_logging_file(url, logging_dict["logging_error_filename"])
        print(f"Response error. Status - {response.status_code}, Error - {response.text}")
    elif logging_dict["logging_level"] > 1:
        strdata_to_logging_file(url, logging_dict["logging_filename"])
    response = response.json()
    return response


def GetRequestStatus(bearerToken, requestId):
    url = f"http://{params_map['hostname']}:{params_map['port']}/manager/requests/{requestId}"
    headers = { 'Content-Type': 'application/json; charset=utf-8', 'Authorization': f"{bearerToken}" }
    response = requests.get(url, headers=headers, verify=False)
    if response.status_code != 200:
        if logging_dict["logging_level"] > 0:
            strdata_to_logging_file(url, logging_dict["logging_error_filename"])
        print(f"Response error. Status - {response.status_code}, Error - {response.text}")
    elif logging_dict["logging_level"] > 1:
        strdata_to_logging_file(url, logging_dict["logging_filename"])
    response = response.json()
    return response


def RestoreFileRequest(bearerToken, targetFile):
    url = f"http://{params_map['hostname']}:{params_map['port']}/manager/requests/restore"
    headers = { 'Content-Type': 'application/json; charset=utf-8', 'Authorization': f"{bearerToken}" }
    body = ('{"objectName":"%s", "collectionName":"%s", "destinationServer":"%s", "filePathRoot":"%s"}' % (get_filename(targetFile), params_map['collection_name'], params_map['destination_server'], os.path.dirname(targetFile)))
    response = requests.post(url, headers=headers, data=body.encode("utf-8"), verify=False)
    if response.status_code != 200:
        if logging_dict["logging_level"] > 0:
            strdata_to_logging_file(url, logging_dict["logging_error_filename"])
        print(f"Response error. Status - {response.status_code}, Error - {response.text}")
    elif logging_dict["logging_level"] > 1:
        strdata_to_logging_file(url, logging_dict["logging_filename"])
    response = response.json()
    return response


def GetAllObjects(bearerToken : str, listPosition : list, fullObjList : list):
    if not listPosition:
        url = f"http://{params_map['hostname']}:{params_map['port']}/manager/objects/list?initialTime=0&listType=1&objectsListType=2&maxListSize=200&collectionName={params_map['collection_name']}&mediaName=*&objectName=*&levelOfDetail=3"
    else:
        url = f"http://{params_map['hostname']}:{params_map['port']}/manager/objects/list?initialTime=0&listType=1&objectsListType=2&maxListSize=200&collectionName={params_map['collection_name']}&mediaName=*&levelOfDetail=3&objectName=*&listPosition={(',').join(listPosition)}"
    headers = { 'Content-Type': 'application/json; charset=utf-8', 'Authorization': f"{bearerToken}" }
    response = requests.get(url, headers=headers, verify=False)
    if response.status_code != 200:
        if logging_dict["logging_level"] > 0:
            strdata_to_logging_file(url, logging_dict["logging_error_filename"])
        print(f"Response error. Status - {response.status_code}, Error - {response.text}")
    elif logging_dict["logging_level"] > 1:
        strdata_to_logging_file(url, logging_dict["logging_filename"])
    response = response.json()
    if response['statusDescription'] != 'Success':
        if response['statusName'] != "WARN_NO_MORE_OBJECTS":
            full_object_list = ['ERROR']
            return full_object_list
    
    listPosition = []
    if response['statusCode'] != 1019:
        listPosition = response['listPosition']

    if not listPosition: #check if list is empty
        return fullObjList + response['objectInfoList']
    else:
        return GetAllObjects(bearerToken, params_map, listPosition, fullObjList + response['objectInfoList'])
    

def GetObjectDict(objList : list,params):
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
        
    for object_dict in objList:
        mtime_epoch_seconds = object_dict['archiveDate']
        file_size = object_dict['sizeInBytes']
        file_path = ""
        if not object_dict['path'] is None and object_dict['path'] != "":
            file_path = object_dict['path']
        file_name = object_dict['files'][0]
        file_uuid = ""
        if not object_dict['uuid'] is None:
            file_uuid = object_dict['uuid']
        if file_path != "":
            full_file = f"{file_path}/{file_name}"
        else:
            full_file = f"{file_name}"
            
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
                include_file = file_in_policy(policy_dict, file_name, full_file, file_size, mtime_epoch_seconds)

        file_object = {}
        if include_file:
            file_object["name"] = full_file
            file_object["size"] = file_size
            file_object["mode"] = "0"
            file_object["tmpid"] = file_uuid
            file_object["checksum"] = file_uuid
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


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', required = True, help = 'Configuration name')
    parser.add_argument('-m', '--mode', required = True, help = 'upload,browse,download,list,actions')
    parser.add_argument('-s','--source',help='source file')
    parser.add_argument('-t','--target',help='target_path')
    parser.add_argument('-in', '--indexid', required=False, help = 'REQUIRED if list')
    parser.add_argument('-jg', '--jobguid', required=False, help = 'REQUIRED if list')
    parser.add_argument('-ji', '--jobid', required=False, help = 'REQUIRED if bulk restore.')
    parser.add_argument('-p', "--projectname", required=False, help = 'Project name')
    
    args = parser.parse_args()
    
    mode = args.mode
    file_path = args.source
    target_path = args.target
    
    logging_dict = loadLoggingDict(os.path.basename(__file__), args.jobguid)
    config_map = loadConfigurationMap(args.config)
    filter_file_dict = loadFilterPolicyFiles (args.jobguid)
    
    params_map = {}
    params_map["source"] = args.source
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
            
    bearer_token = GenerateBearerToken()#host_name, port_no, user_name, pass_word)
    #bearer_token = bearer_token.replace("Bearer ", "").strip()
            
    if mode == 'actions':
        try:
            if params_map["actions"]:
                print(params_map["actions"])
                exit(0)
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            exit(1)
    
    if mode == 'list':
        if target_path is None or args.indexid is None:
            print('Target path (-t <targetpath> ) -in <index> options are required for list')
            exit(1)
            
        initial_list_position = []
        obj_list = []
        all_objs_list = GetAllObjects(bearer_token,initial_list_position,obj_list)
        if "ERROR" in all_objs_list:
            exit(-1)
        
        objects_dict = GetObjectDict(all_objs_list,params_map)
        if len(all_objs_list) == 0:
            objects_dict = {}

        if target_path:
            generate_xml_from_file_objects(objects_dict, target_path)
            print(f"Generated XML file: {target_path}")
            exit(0)
        else:
            print("Failed to generate XML file.")
            exit(1)
    
    elif mode == 'upload':
        if file_path is None:
            print(f'File path (-s <source> ) option is required for upload')
            exit(1)

        archive_request_json = ArchiveFileRequest(bearer_token,file_path)
        archive_request_id = ""
        if archive_request_json['statusDescription'] == "Success":
            archive_request_id = archive_request_json['requestId']
        else:
            exit(-1)

        state_name = ""
        issue_found = False
        if archive_request_id != "":
            while state_name not in END_STATES:
                archive_status_json = GetRequestStatus(bearer_token,archive_request_id)
                state_name = archive_status_json['stateName']
                print(f"STATE = {state_name}")
        else:
            exit(-1)

        print(f"FINAL STATE = {state_name}")
        if state_name == "ABORTED" or state_name == "CANCELLED":
            exit(-1)
        else:
            exit(0)
    
    elif mode == 'download':
        if target_path is None:
            print('Target path (-t <targetpath> )option are required for download')
            exit(1)

        restore_request_json = RestoreFileRequest(bearer_token,target_path)
        
        restore_request_id = ""
        if restore_request_json['statusDescription'] == "Success":
            restore_request_id = restore_request_json['requestId']
        else:
            exit(-1)

        state_name = ""
        issue_found = False
        if restore_request_id != "":
            while state_name not in END_STATES:
                archive_status_json = GetRequestStatus(bearer_token,restore_request_id)
                state_name = archive_status_json['stateName']
        else:
            exit(-1)

        if state_name == "ABORTED" or state_name == "CANCELLED":
            exit(-1)
    
    else:
        print(f'Unsupported mode {mode}')
        exit(1)
