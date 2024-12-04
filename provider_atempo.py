import argparse
import os
import urllib
import urllib.parse
import urllib3
import datetime
import sys
import xml.etree.ElementTree as ET
from time import sleep
from urllib.request import urlopen, Request
from action_functions import *

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def get_list_folder_xml(url : str, archive : str, path : str):
    archive = urllib.parse.quote(archive)
    path = urllib.parse.quote(path)
    src_path = f'{archive}@{path}'
    #print(f'GET LS SRC PATH = {src_path}')
    url += f"?src={src_path}"
    #print(f'GET LS FINAL URL = {url}')
    httprequest = Request(url)
    with urlopen(httprequest) as response:
        #print(response.status)
        #print(response.read().decode())
        return response.read().decode()
   

def get_archived_file_details_xml(url : str, archive : str, path : str):
    archive = urllib.parse.quote(archive)
    path = urllib.parse.quote(path)
    src_path = f'{archive}@{path}'
    #print(f'GET FILESTAT SRC PATH = {src_path}')
    url += f"?src={src_path}"
    #print(f'GET FILESTAT FINAL URL = {url}')
    httprequest = Request(url)
    with urlopen(httprequest) as response:
        #print(response.status)
        #print(response.read().decode())
        return response.read().decode()
    

def search_for_files(folder_path ,files_list):
    atempo_url = params_map["url"]
    atempo_archivename = params_map["project_name"]
    #print(f'FOLDER PATH = {folder_path}')
    url = f'{atempo_url}/ls'
    #print(f"LS URL = {url}")
    folder_xml_resp = get_list_folder_xml(url, atempo_archivename, folder_path)
    if folder_xml_resp is None or folder_xml_resp == "":
        err = 'No xml response obtained for folder listing.'
        sys.exit(err)
    #print(f'FOLDER RESP XML = {folder_xml_resp}')
    root_elem = ET.fromstring(folder_xml_resp)
    #print(f'FOLDER LISTING ROOT ELEM = {root_elem.tag}')
    #print("Object XML tag found.")
    return_code = root_elem.find('ReturnCode').attrib['ADARetCode']
    #print(f'RETURN CODE = {return_code}')
    if return_code != "1":
        err = 'Issue with GET request on folders.'
        sys.exit(err)
    for obj in root_elem.findall('object'):
        attrib_dict = obj.attrib
        obj_type = attrib_dict['type']
        obj_name = attrib_dict['name']
        #print(f"OBJ TYPE = {obj_type}, NAME = {obj_name}")
        if obj_type == 'folder' or obj_type == 'directory':
            #print("Folder/Dir found.")
            new_folder_path = f'{folder_path}/{obj_name}'
            new_folder_path = new_folder_path.replace("//","/")
            search_for_files(atempo_url, atempo_archivename, new_folder_path, files_list)
        else:
            file_path = f'{folder_path}/{obj_name}'
            file_path = file_path.replace("//","/")
            files_list.append(file_path)
    return files_list

def get_file_data(file_path):
    file_attrib_dict = {}
    atempo_url = params_map["url"]
    atempo_archivename = params_map["project_name"]
    
    file_url = f'{atempo_url}/getFileStatus'
    found_result = False
    count=0
    while not found_result:
        count = count + 1
        if count > 10:
            #print (f"Exiting after 10 tries. Failed to find file details for - {file_path}")
            # break
            return False
        file_xml_resp = get_archived_file_details_xml(file_url, atempo_archivename, file_path)
        #print(f'FILE RESP XML = {file_xml_resp}')
        file_root_elem = ET.fromstring(file_xml_resp)
        #print(f'FILE DETAILS ROOT ELEM = {file_root_elem.tag}')
        if file_root_elem.find('instance') == None or len(file_root_elem.findall('instance')) == 0:
            #print ("ISSUE FINDING FILE DETAILS. RETRYING...")
            sleep(0.5)
            continue
        else:
            found_result = True
            file_attrib_dict = file_root_elem.find('instance').attrib

    return file_attrib_dict

def GetObjectDict(files_list : list,params):
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

    for file in files_list:
        file_attrib_dict = get_file_data(file)
        if file_attrib_dict:
            if 'last_update' in file_attrib_dict:
                mtime_struct = datetime.strptime(file_attrib_dict['last_update'].split(".")[0], "%Y/%m/%d-%H:%M:%S") 
                mtime_epoch_seconds = int(mtime_struct.timestamp())
            if mtime_epoch_seconds == "NOT_FOUND" and 'last_access' in file_attrib_dict:
                mtime_struct = datetime.strptime(file_attrib_dict['last_update'].split(".")[0], "%Y/%m/%d-%H:%M:%S")
                mtime_epoch_seconds = int(mtime_struct.timestamp())
            file_size = file_attrib_dict['file_size']
            file_type = "file" if file_size != "0" else "dir"
            file_path = file
            file_name = get_filename(file_path)
            
            if mtime_epoch_seconds == "NOT_FOUND":
                continue
            
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

            file_path = replace_file_path(file_path)
            
            file_object = {}
            if include_file == True:
                file_object["name"] = file_path
                file_object["size"] = file_size
                file_object["mode"] = "0"
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
    parser.add_argument('-f','--foldername',help='folder_name_to_create')
    parser.add_argument('-in', '--indexid', required=False, help = 'REQUIRED if list')
    parser.add_argument('-jg', '--jobguid', required=False, help = 'REQUIRED if list')
    parser.add_argument('-ji', '--jobid', required=False, help = 'REQUIRED if bulk restore.')
    parser.add_argument('-p', "--projectname", required=False, help = 'Project name')
    
    args = parser.parse_args()
    mode = args.mode
    file_path = args.source
    target_path = args.target
    folder_path = args.foldername
    project_name = args.projectname
    
    logging_dict = loadLoggingDict(os.path.basename(__file__), args.jobguid)
    config_map = loadConfigurationMap(args.config)
    filter_file_dict = loadFilterPolicyFiles (args.jobguid)

    params_map = {}
    params_map["foldername"] = args.foldername
    params_map["source"] = args.source
    params_map["target"] = args.target
    params_map["indexid" ] = args.indexid
    params_map["jobguid"] = args.jobguid
    params_map["jobid"] = args.jobid
    params_map["project_name"] = args.projectname


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

    if mode == 'list':
        if target_path is None or folder_path is None or args.indexid is None:
            print('Target path (-t <targetpath> ),Folder path (-f <folderpath>) and -in <index> options are required for list')
            exit(1)
        
        file_list = []
        files_list = search_for_files(folder_path,file_list)
        objects_dict = GetObjectDict(files_list,params_map)
        if len(files_list) == 0:
            objects_dict = {}
        if target_path:
            generate_xml_from_file_objects(objects_dict, target_path)
            print(f"Generated XML file: {target_path}")
            exit(0)
        else:
            print("Failed to generate XML file.")
            exit(1)
