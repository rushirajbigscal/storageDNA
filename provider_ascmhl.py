import subprocess
import os
import shutil
from datetime import datetime
import xml.etree.ElementTree as ET
import argparse
from action_functions import *

def generate_mhl_file(source_folder):
    command = [
        'ascmhl','create',
        f"{source_folder}"
    ]
    result = subprocess.run(command)
    if result.returncode != 0:
        print(f"Error while generate mhl file: {result.stderr}")
        return False
    else:
        return True

def get_mhl_file_path(file_path):
    namespace = {'mhl': 'urn:ASC:MHL:DIRECTORY:v2.0'}
    tree = ET.parse(file_path)
    root = tree.getroot()
    mhl_path = root.find('.//mhl:path', namespace)
    if mhl_path.text:
        return f"{os.path.join("ascmhl",mhl_path.text)}"
    else:
        return False

def GetObjectDict(mhl_file_path,filter_type,filter_file,policy_file):
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
        is_dir = file_size == '0'

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
            include_file = file_in_policy(policy_dict, file_name, file_path, file_size, mtime_epoch_seconds)

        include_file = True
        file_object = {}
        if include_file == True:
            file_object["name"] = file_path
            file_object["size"] = file_size
            file_object["mode"] = "0"
            file_object["checksum"] = data["checksum"]
            file_object["type"] = "F_REG" if file_size != "0" else "F_DIR"
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


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-m', '--mode', required = True, help = 'upload,browse,download,list')
    parser.add_argument('-s','--source',help='source path')
    parser.add_argument('-t','--target',help='target_path')
    parser.add_argument('-ft', '--filtertype', required=False, choices=['none', 'include', 'exclude'], help='Filter type')
    parser.add_argument('-ff', '--filterfile', required=False, help='Extension file')
    parser.add_argument('-pf', '--policyfile', required=False, help='Policy file')

    args = parser.parse_args()
    mode = args.mode
    source_path = args.source
    target_path = args.target

    if mode == 'list':
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
        
        if os.path.isdir(source_path):
            mhl_path = os.path.join(source_path,"ascmhl")
            if os.path.exists(mhl_path):
                shutil.rmtree(mhl_path)
            if generate_mhl_file(source_path):
                file_path = "ascmhl/ascmhl_chain.xml"
                file_path = os.path.join(source_path,file_path)
            if os.path.exists(file_path):
                if get_mhl_file_path(file_path):
                    mhl_file_path = os.path.join(source_path,get_mhl_file_path(file_path))
                    objects_dict = GetObjectDict(mhl_file_path,filter_type,filter_file,policy_file)
                else:
                    print("Faild to genrate object dict.")
            else:
                print("Faild to create an mhl xml file.")
            if objects_dict and target_path:
                generate_xml_from_file_objects(objects_dict, target_path)
                print(f"Generated XML file: {target_path}")
            else:
                print("Failed to generate XML file.")
        else:
            exit(-1)
