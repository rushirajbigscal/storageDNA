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

def GetRequestStatus(cloudConfigDetails, requestId):
    url = f"http://{cloudConfigDetails['hostname']}:{cloudConfigDetails['port']}/xen/export/{requestId}"
    headers = { 'Content-Type': 'application/json; charset=utf-8'}
    print (f'In GetRequestStatus - URL = {url}, headers = {headers}')
    resp_JSON = requests.get(url, headers=headers, verify=False,timeout=3600).json()
    return resp_JSON


def GetAllObjects(cloudConfigDetails,path,recursive=None):
    url = f"http://{cloudConfigDetails['hostname']}:{cloudConfigDetails['port']}/xen/export"
    params = {
            'path': path,
            'recursive': recursive
            }
    
    print (f'In GetAllObjects - URL = {url}, params = {params}')
    resp_JSON = requests.get(url, params=params, verify=False).json()
    return resp_JSON


def GetObjectDict(data : dict):
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

    for result in data['requestResults']:
        mtime_struct = datetime.strptime(result['Creation'], "%m/%d/%Y %H:%M:%S") 
        atime_struct = datetime.strptime(result['Last-Accessed'], "%m/%d/%Y %H:%M:%S")
        mtime_epoch_seconds = int(mtime_struct.timestamp())
        atime_epoch_seconds = int(atime_struct.timestamp())
        
        '''
        if is_dir or filter_type == 'none':
            include_file = True
        elif len(extensions) == 0:
            continue
        else:
            file_in_list = isFilenameInFilterList(file_name, extensions)
            include_file = (file_in_list and filter_type == "include") or  (not file_in_list and filter_type == "exclude") or (filter_type == "none")

        if include_file == False:
            continue

        policy_type = policy_dict["type"]
        policy_entries = policy_dict["entries"]
        
        if policy_type == "ERROR":
            continue

        if policy_type == "NOFILE":
            include_file = True

        elif include_file and len(policy_entries) > 0:
            include_file = file_in_policy(policy_dict, file_name, file_parent_path, file_size, mtime_epoch_seconds)
        '''

        include_file = True
        file_object = {}
        if include_file == True:
            file_object["name"] = result["File-Path"]
            file_object["size"] = result["File-Size"] if result["File-Type"] == "File" else "0"
            file_object["mode"] = "0"
            file_object["type"] = "F_DIR" if result["File-Type"] == "Folder" else "F_REG"
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
    
    parser.add_argument('-m', '--mode', required = True, help = 'upload, download, list,browse')
    parser.add_argument('-x','--xml_filename',required=False,help='xml_filename')
    # parser.add_argument('-f','--filter',required=False,help='Filter in reg expresion')
    parser.add_argument('-p', '--path', required=False, default='V:\\', help="Path of storage disk (default: V:\\)")
    
    args = parser.parse_args()
    
    mode = args.mode
    # filter = args.filter
    path = args.path
    xml_filename = args.xml_filename

    '''
    config_name = args.configname
    
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
    '''
    cloud_config_info = {'hostname': '192.168.1.172',
                         'port': 8080 
                        }
    

    if mode == 'list':
        all_objs_list = GetAllObjects(cloud_config_info,path,recursive='true')
        print(all_objs_list)
        if not all_objs_list['requestId']:
            exit(-1)

        requestId = all_objs_list['requestId'] 
        state_name = ""
        while state_name not in END_STATES:
            listing_json_responce = GetRequestStatus(cloud_config_info,requestId)
            state_name = listing_json_responce['requestStatus']
            print(state_name)
            time.sleep(5)

        objects_dict = GetObjectDict(listing_json_responce)
        generate_xml_from_file_objects(objects_dict, xml_filename)
        print(f"Generated XML file: {xml_filename}")
        #os.remove(directory)
        print ("GOOD")

    elif mode == 'browse':
        folders = set()
        all_objs_list = GetAllObjects(cloud_config_info,path,recursive='false')
        requestId = all_objs_list['requestId'] 
        state_name = ""
        while state_name not in END_STATES:
            data = GetRequestStatus(cloud_config_info,requestId)
            state_name = data['requestStatus']
            print(state_name)
            time.sleep(1)
            
        if data['requestStatus'] == "Success":
            for item in data['requestResults']:
                path =os.path.normpath(path)  
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
        generate_xml(xml_filename,xml_output)


        

