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
  

def GenerateBearerToken(cloudConfigDetails):
    url = f"http://{cloudConfigDetails['hostname']}:{cloudConfigDetails['port']}/manager/users/login"
    headers = { 'Content-Type': 'application/json; charset=utf-8' }
    body = ('{"username":"%s", "password":"%s"}' % (cloudConfigDetails['username'], cloudConfigDetails['password']))
    print (f'In GenerateBearerToken - URL = {url}, body = {body}')
    resp_JSON = requests.post(url, headers=headers, data=body.encode("utf-8"), verify=False).json()
    #print (f'In GenerateBearerToken - URL = {url}, body = {body}, JSON resp = {resp_JSON}')
    return resp_JSON['token']


def ArchiveFileRequest(bearerToken, cloudConfigDetails, sourceFile):
    url = f"http://{cloudConfigDetails['hostname']}:{cloudConfigDetails['port']}/manager/requests/archive"
    headers = { 'Content-Type': 'application/json; charset=utf-8', 'Authorization': f"{bearerToken}" }
    body = ('{"objectName":"%s", "collectionName":"%s", "sourceServer":"%s", "media":"%s", "components":["%s"]}' % (get_filename(sourceFile), cloudConfigDetails['collection_name'], cloudConfigDetails['source_server'], cloudConfigDetails['media_name'], sourceFile))
    print (f'In ArchiveFileRequest - URL = {url}, headers = {headers}, body = {body}')
    resp_JSON = requests.post(url, headers=headers, data=body.encode("utf-8"), verify=False).json()
    return resp_JSON


def GetRequestStatus(bearerToken, cloudConfigDetails, requestId):
    url = f"http://{cloudConfigDetails['hostname']}:{cloudConfigDetails['port']}/manager/requests/{requestId}"
    headers = { 'Content-Type': 'application/json; charset=utf-8', 'Authorization': f"{bearerToken}" }
    print (f'In GetRequestStatus - URL = {url}, headers = {headers}')
    resp_JSON = requests.get(url, headers=headers, verify=False).json()
    return resp_JSON


def RestoreFileRequest(bearerToken, cloudConfigDetails, targetFile):
    url = f"http://{cloudConfigDetails['hostname']}:{cloudConfigDetails['port']}/manager/requests/restore"
    headers = { 'Content-Type': 'application/json; charset=utf-8', 'Authorization': f"{bearerToken}" }
    body = ('{"objectName":"%s", "collectionName":"%s", "destinationServer":"%s", "filePathRoot":"%s"}' % (get_filename(targetFile), cloudConfigDetails['collection_name'], cloudConfigDetails['destination_server'], os.path.dirname(targetFile)))
    print (f'In RestoreFileRequest - URL = {url}, headers = {headers}, body = {body}')
    resp_JSON = requests.post(url, headers=headers, data=body.encode("utf-8"), verify=False).json()
    return resp_JSON


def GetAllObjects(bearerToken : str, cloudConfigDetails, listPosition : list, fullObjList : list):

    url = f"http://{cloudConfigDetails['hostname']}:{cloudConfigDetails['port']}/manager/objects/list"


    # if not listPosition:
    #     url = f"http://{cloudConfigDetails['hostname']}:{cloudConfigDetails['port']}/manager/objects/list?initialTime=0&listType=1&objectsListType=2&maxListSize=2&collectionName={cloudConfigDetails['collection_name']}&mediaName=*&objectName=*&levelOfDetail=3"
    # else:
    #     url = f"http://{cloudConfigDetails['hostname']}:{cloudConfigDetails['port']}/manager/objects/list?initialTime=0&listType=1&objectsListType=2&maxListSize=2&collectionName={cloudConfigDetails['collection_name']}&mediaName=*&levelOfDetail=3&objectName=*&listPosition={(',').join(listPosition)}"

    headers = { 'Content-Type': 'application/json; charset=utf-8', 'Authorization': f"{bearerToken}" }

    if not listPosition:
        params = {  
                    "initialTime": 0,
                    "listType": 1,
                    "objectsListType": 2, 
                    "maxListSize": 2,
                    "objectName" : '*' ,
                    "collectionName": cloudConfigDetails['collection_name'],
                    "mediaName": '*',
                    "levelOfDetail": 3
                }
    else:
        params = {  
                    "initialTime": 0,
                    "listType": 1,
                    "objectsListType": 2, 
                    "maxListSize": 2,
                    "objectName" : '*' ,
                    "collectionName": cloudConfigDetails['collection_name'],
                    "mediaName": '*',
                    "levelOfDetail": 3,
                    "listPosition": {(',').join(listPosition)}
                }

    print (f'In GetAllObjects - URL = {url}, headers = {headers}')
    resp_JSON = requests.get(url, headers=headers, params=params, verify=False).json()
    with open('api_response.json', 'w') as file:
        json.dump(resp_JSON, file, indent=4)
    print (f"RESP JSON = {resp_JSON}")
    if resp_JSON['statusDescription'] != 'success':
        full_object_list = ['ERROR']
        return full_object_list
    
    if resp_JSON['statusCode'] != 1019:
        listPosition = resp_JSON['listPosition']

    if not listPosition: #check if list is empty
        return fullObjList + resp_JSON['objectInfoList']
    else:
        return GetAllObjects(bearerToken, cloudConfigDetails, listPosition, fullObjList)
    

def GetObjectDict(objList : list):
    scanned_files = 0
    selected_count = 0
    output = {}
    file_object_list = []
    extensions = []
    total_size = 0

    '''
    if filter_type.lower() != 'none':
        if not os.path.isfile(filter_file):
            print(f"Filter file given: {filter_file} not found.")
            return output

        with open(filter_file, 'r') as f:
            extensions = [ext.strip() for ext in f.readlines()]

    policy_dict = load_policies_from_file(policy_file)
    '''
    for object_dict in objList:
        mtime_epoch_seconds = object_dict['archiveDate']
        file_size = object_dict['sizeInBytes']
        file_path = object_dict['path']
        file_name = object_dict['files'][0]
        file_uuid = object_dict['uuid']
        full_file = f"{file_path}/{file_name}"
        is_dir = file_size == '0'
        '''
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
        '''
        include_file = True
        file_object = {}
        if include_file:
            file_object["name"] = full_file
            file_object["size"] = file_size
            file_object["mode"] = "0"
            file_object["id"] = file_uuid
            file_object["checksum"] = file_uuid
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


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    
    parser.add_argument('-m', '--mode', required = True, help = 'upload, download, list')
    parser.add_argument('-x','--xml_filename',help='xml_filename')

    # parser.add_argument('-c', '--configname', required = True, help = 'Blackpearl config name to work with in cloud_targets.conf')
    parser.add_argument('-s', '--source', help = 'REQUIRED if upload or download')
    parser.add_argument('-t', '--target', help = 'REQUIRED if upload or download')

    #parser.add_argument('-cn', '--collectionname', help = 'REQUIRED if upload or download')
    #parser.add_argument('-ss', '--sourceserver', help = 'REQUIRED if upload or download')
    
    args = parser.parse_args()
    
    mode = args.mode

    #config_name = args.configname
    
    cloudTargetPath = ''
    '''
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
    '''
    cloud_config_info = { 'hostname': '3.144.242.95',
                         'port': 9443,
                         'username': 'admin',
                         'password': 'admin',
                         'source_server': 'TestCifs',
                         'destination_server': 'TestCifs',
                         'collection_name': 'TEST',
                         'media_name': 'NLD_DISK' }
    
    bearer_token = GenerateBearerToken(cloud_config_info)#host_name, port_no, user_name, pass_word)
    bearer_token = bearer_token.replace("Bearer ", "").strip()
    print(f'Bearer token = {bearer_token}')
    
    if mode == 'upload':
        source_file = args.source
        # source_file = "APIDNATest2.mxf"
        #target_media = args.target
        #source_server = args.sourceserver
        
        archive_request_json = ArchiveFileRequest(bearer_token, cloud_config_info, source_file)#host_name, port_no, bearer_token, source_file, collection_name, source_server)
        print(f"ARCHIVE REQ JSON = {archive_request_json}")
        archive_request_id = ""
        if archive_request_json['statusDescription'] == "Success":
            archive_request_id = archive_request_json['requestId']
        else:
            exit(-1)

        state_name = ""
        issue_found = False
        if archive_request_id != "":
            while state_name not in END_STATES:
                archive_status_json = GetRequestStatus(bearer_token, cloud_config_info, archive_request_id)#host_name, port_no, bearer_token, archive_request_id)
                print(f"REQ STATUS JSON = {archive_status_json}")
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
        target_file = args.target

        restore_request_json = RestoreFileRequest(bearer_token, cloud_config_info, target_file)#host_name, port_no, bearer_token, target_file, collection_name, dest_server)
        
        restore_request_id = ""
        if restore_request_json['statusDescription'] == "Success":
            restore_request_id = restore_request_json['requestId']
        else:
            exit(-1)

        state_name = ""
        issue_found = False
        if restore_request_id != "":
            while state_name not in END_STATES:
                archive_status_json = GetRequestStatus(bearer_token, cloud_config_info, restore_request_id)
                state_name = archive_status_json['stateName']
        else:
            exit(-1)

        if state_name == "ABORTED" or state_name == "CANCELLED":
            exit(-1)
    
    elif mode == 'list':
        initial_list_position = []
        obj_list = []

        all_objs_list = GetAllObjects(bearer_token, cloud_config_info, initial_list_position, obj_list)
        print(all_objs_list)
        if "ERROR" in all_objs_list:
            exit(-1)

        objects_dict = GetObjectDict(all_objs_list)
        xml_filename = args.xml_filename
        print('***********************************')
        print(objects_dict)

        generate_xml_from_file_objects(objects_dict, xml_filename)
        print(f"Generated XML file: {xml_filename}")
        #os.remove(directory)
        print ("GOOD")

