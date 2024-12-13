import subprocess
import csv
import os
import sys
from datetime import datetime
import argparse
from action_functions import *


def aoc_command_to_str(command, filename):
    
    command_listed = '\" \"'.join(command)
    command_exec=f'\"{command_listed}\"'
    f = open(filename, "a")
    f.write(f'{command_listed}\n')
    f.close()

def browse_folder(workspace,source_folder, logging_dict):
    command = [
        'ascli', 'aoc', 'files',
        f'--workspace={workspace}',
        'browse', f'{source_folder}',
        '--fields=name,type',
        '--format=csv'
    ]
    result = subprocess.run(command, capture_output=True)
    if result.returncode != 0:
        if logging_dict["logging_level"] > 0:
            aoc_command_to_str(command, logging_dict["logging_error_filename"])
    elif logging_dict["logging_level"] > 1:
            aoc_command_to_str(command, logging_dict["logging_filename"])
    csv_output = result.stdout.decode('utf-8').strip().split('\n')
    return csv_output

def create_folder(target_folder):

    mkdir_command = [
        'ascli', 'aoc', 'files',
        f'--workspace={workspace}',
        'mkdir', target_path
    ]
    
    mkdir_process = subprocess.run(mkdir_command, capture_output=True)
    if mkdir_process.returncode != 0:
        result_err = str(mkdir_process.stderr)
        if not "already exists" in result_err:
            if logging_dict["logging_level"] > 0:
                aoc_command_to_str(mkdir_command, logging_dict["logging_error_filename"])
            print(f"Error while creating remote folder: {mkdir_process.stderr} {mkdir_process.returncode}")
            return False

    if logging_dict["logging_level"] > 1:
        aoc_command_to_str(mkdir_command, logging_dict["logging_filename"])

    return True

def scan_files(param_map):
    workspace = get_parameter_value("workspace")
    source_folder = get_parameter_value("foldername")
    command = [
        'ascli', 'aoc', 'files',
        f'--workspace={workspace}',
        'find', f'{source_folder}',
        '@ruby:->(f){f["type"].eql?("file")}',
        '--fields=size,path,modified_time,type',
        '--format=csv'
    ]
    result = subprocess.run(command, capture_output=True)
    if result.returncode != 0:
        if logging_dict["logging_level"] > 0:
            aoc_command_to_str(command, logging_dict["logging_error_filename"])
        print(f'Scan failed with exit code: {result.returncode}')
        exit(1)
    elif logging_dict["logging_level"] > 1:
            aoc_command_to_str(command, logging_dict["logging_filename"])
                
    result_text = result.stdout.decode('utf-8')
    csv_output = result_text.strip().split('\n')
    return csv_output

def upload_file(workspace,target_path,file_path):
    if not os.path.exists(file_path):
        print(f'Source path does not exist: {file_path}')
        return False

    mkdir_command = [
        'ascli', 'aoc', 'files',
        f'--workspace={workspace}',
        'mkdir', target_path
    ]
    upload_command = [
        'ascli', 'aoc', 'files',
        f'--workspace={workspace}',
        'upload',
        f'--to-folder={target_path}',
        file_path
    ]
    mkdir_process = subprocess.run(mkdir_command, capture_output=True)
    if mkdir_process.returncode != 0:
        result_err = str(mkdir_process.stderr)
        if not "already exists" in result_err:
            print(f"Error while creating remote folder: {mkdir_process.stderr} {mkdir_process.returncode}")
            return False

    upload_process = subprocess.run(upload_command, capture_output=True)
    if upload_process.returncode != 0:
        if logging_dict["logging_level"] > 0:
            aoc_command_to_str(upload_command, logging_dict["logging_error_filename"])
        result_err = str(upload_process.stderr)
        print(f"Error while uploading file to remote folder: {result_err}")
        return False
    elif logging_dict["logging_level"] > 1:
        aoc_command_to_str(upload_command, logging_dict["logging_filename"])

    print(f"File '{file_path}' uploaded to '{target_path}' successfully.")
    return True


def download_file(workspace, file_path, target_path):
    command = [
        'ascli', 'aoc', 'files',
        f'--workspace={workspace}',
        'download',
        f'--to-folder={target_path}/',
        file_path
    ]

    download_process = subprocess.run(command, capture_output = True)
    if download_process.returncode != 0:
        if logging_dict["logging_level"] > 0:
            aoc_command_to_str(command, logging_dict["logging_error_filename"])
        result_err = str(download_process.stderr)
        print(f"Error while downloading file to local folder: {result_err}")
        return False
    elif logging_dict["logging_level"] > 1:
        aoc_command_to_str(command, logging_dict["logging_filename"])

    print(f"File '{file_path}' downloaded to '{target_path}' successfully.")
    return True

  
def get_parameter_value(key):
    if not key in params_map or params_map[key] is None:
        print(f'No {key} key found in parameter map')
        exit(1)
    return params_map[key]

def GetObjectDict(files_list : list, params):
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
   
    for file_data in files_list:
        file_list = file_data.strip().split(",")
        if len(file_list) == 1:
            continue
        file_type = file_list.pop()
        modified_time = file_list.pop()
        if modified_time:
            mtime_struct = datetime.strptime(modified_time.split(".")[0], "%Y-%m-%dT%H:%M:%SZ")
        else:
            mtime_struct = datetime.now()
        mtime_epoch_seconds = int(mtime_struct.timestamp())
        file_path = file_list.pop()
        file_name = get_filename(file_path)
        file_size = file_list.pop()
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
    folder_name = args.foldername

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

    workspace = get_parameter_value("workspace")

    if mode == 'list':
        if target_path is None or folder_name is None or args.indexid is None:
             print('Target path (-t <targetpath> ) and folder name (-f <foldername> ) -in <index>  options are required for list')
             exit(1)
        files_list = scan_files(params_map)
        objects_dict = GetObjectDict(files_list, params_map)
        if len(files_list) == 0:
            objects_dict ={}
        
        if objects_dict and target_path:
            generate_xml_from_file_objects(objects_dict, target_path)
            print(f"Generated XML file: {target_path}")
            exit(0)
        else:
            print("Failed to generate XML file.")
            exit(1)

    elif mode == 'browse':
        if args.foldername is None:
             print(f'Folder name (-f <foldername> ) option is required for browse')
             exit(2)

        folders_list = []
        folders = browse_folder(workspace, folder_name,logging_dict)
        for folder_data in folders:
            folder = folder_data.strip().split(",")
            if (len(folder) > 1):
                folder_name = folder[0]
                folder_type = folder[1]
                if folder_type in ['folder', 'link']:
                    folders_list.append(folder_name)

        xml_output = add_CDATA_tags(folders_list)
        print(xml_output)
        exit(0)

    elif mode == 'upload':
        if upload_file(workspace,target_path,file_path) == False:
            exit(1)
        else:
            exit(0)

    elif mode == "download":
        if download_file(workspace,file_path,target_path) == False:
            exit(1)
        else:
            exit(0)

    elif mode == "createfolder":
        if not create_folder(target_path):
            exit(1)
        else:
            exit(0)

    else:
        print(f'Unsupported mode {mode}')
        exit(1)

