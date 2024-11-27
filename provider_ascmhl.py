import subprocess
import os
import shutil
from datetime import datetime
import xml.etree.ElementTree as ET
import argparse
from action_functions import *

def aoc_command_to_str(command, filename):
    command_listed = '\" \"'.join(command)
    command_exec=f'\"{command_listed}\"'
    f = open(filename, "a")
    f.write(f'{command_listed}\n')
    f.close()

def generate_mhl_file(source_folder,logging_dict):
    command = [
        'ascmhl','create',
        f"{source_folder}"
    ]
    result = subprocess.run(command,capture_output=True)
    if result.returncode != 0:
        if logging_dict["logging_level"] > 0:
            aoc_command_to_str(command, logging_dict["logging_error_filename"])
        print(f"Error while generate mhl file: {result.stderr}")
        return False
    elif logging_dict["logging_level"] > 1:
        aoc_command_to_str(command, logging_dict["logging_filename"])
    return True

def get_mhl_file_path(file_path):
    namespace = {'mhl': 'urn:ASC:MHL:DIRECTORY:v2.0'}
    tree = ET.parse(file_path)
    root = tree.getroot()
    mhl_path = root.find('.//mhl:path', namespace)
    if mhl_path.text:
        return f"{os.path.join('ascmhl',mhl_path.text)}"
    else:
        return False

def GetObjectDict(mhl_file_path,params):
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

    data_list = []
    namespace = {'mhl': 'urn:ASC:MHL:v2.0'}
    tree = ET.parse(mhl_file_path)
    root = tree.getroot()
    for data in root.findall(".//mhl:hash", namespace):
        file_path = data.find("mhl:path", namespace).text
        file_size = data.find("mhl:path", namespace).get("size")
        file_size = file_size if file_size else "0"
        last_modification_date = data.find("mhl:path", namespace).get("lastmodificationdate")
        checksum = data.find("mhl:xxh128", namespace).text
        data_list.append({"file_path":file_path,"file_size":file_size,"last_modification_date":last_modification_date,"checksum":checksum})

    for data in root.findall(".//mhl:directoryhash", namespace):
        file_path = data.find("mhl:path", namespace).text
        file_size = "0"
        last_modification_date = data.find("mhl:path", namespace).get("lastmodificationdate")
        checksum = "0"
        data_list.append({"file_path":file_path,"file_size":file_size,"last_modification_date":last_modification_date,"checksum":checksum})
    
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


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', required = True, help = 'Configuration name')
    parser.add_argument('-m', '--mode', required = True, help = 'list,actions')
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

    if mode == 'list':
        if target_path is None or folder_name is None or args.indexid is None:
            print('Target path (-t <targetpath> ) and folder name (-f <foldername> ) -in <index>  options are required for list')
            exit(1)
        if os.path.isdir(folder_name):
            mhl_path = os.path.join(folder_name,"ascmhl")
            if os.path.exists(mhl_path):
                shutil.rmtree(mhl_path)
            if generate_mhl_file(folder_name,logging_dict):
                file_path = "ascmhl/ascmhl_chain.xml"
                file_path = os.path.join(folder_name,file_path)
            if os.path.exists(file_path):
                if get_mhl_file_path(file_path):
                    mhl_file_path = os.path.join(folder_name,get_mhl_file_path(file_path))
                    objects_dict = GetObjectDict(mhl_file_path,params_map)
            else:
                objects_dict = {}
                
            if objects_dict and target_path:
                generate_xml_from_file_objects(objects_dict, target_path)
                print(f"Generated XML file: {target_path}")
                exit(0)
            else:
                print("Failed to generate XML file.")
                exit(1)
    else:
        print(f'Unsupported mode {mode}')
        exit(1)