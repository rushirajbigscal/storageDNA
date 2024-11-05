import requests
import argparse
from datetime import datetime
from action_functions import *
from urllib import request
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

VALID_MODES = ['upload', 'download', 'list']

END_STATES = {"5" : "PASSED", "4":"FAILED", "2": "KILLED"}

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

def get_files_data():
    params = {"caller" : cloud_config_info["Caller"]}

    response = requests.get(f"http://{cloud_config_info["hostname"]}:{cloud_config_info["port"]}/flashnet/api/v1/files/filter/", params=params).json()
    return response["Results"]

def archive_file_request(file_path):
    payload = {
        "Caller" : cloud_config_info["Caller"],
        "Priority" : 1,
        "Target" : cloud_config_info["group_name"],
        "VerifyFiles":"true",
        "DeleteFiles":"false",
        "Files" : [
            {
                "FullFileName" : file_path
            }
        ]
    }
    response = requests.post(f"http://{cloud_config_info["hostname"]}:{cloud_config_info["port"]}/flashnet/api/v1/assets/", json=payload).json()
    return response

def restore_file_request(guid,target_path):
    payload = {
        "Caller" : cloud_config_info["Caller"],
        "Priority" : 1,
        "Files" : [
            {
                "Guid" : guid, 
                "Path" : target_path
            }
        ]
    }
    response = requests.post(f"http://{cloud_config_info["hostname"]}:{cloud_config_info["port"]}/flashnet/api/v1/files/", json=payload).json()
    return response

def get_job_status(rid):
    # response = requests.get(f"http://{cloud_config_info["hostname"]}:{cloud_config_info["port"]}/flashnet/api/v1/jobs/{rid}").json()
    response = {"ExitState" : 5}
    return response["ExitState"]

def GetObjectDict(files_list : list):
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

    for data in files_list:
        mtime_struct = datetime.strptime(data['ArchiveDate'], "%Y-%m-%dT%H:%M:%S")
        atime_struct = datetime.strptime(data['LastRestoreDate'], "%Y-%m-%dT%H:%M:%S")
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
            file_object["name"] = data["FullFileName"]
            file_object["size"] = data["Size"]
            file_object["mode"] = "0"
            file_object["tmpid"] = data["Guid"]
            file_object["type"] = "F_REG"
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
    parser.add_argument('-m', '--mode', required = True, help = 'upload, download,list,create_folder')
    parser.add_argument('-s','--source',help='source file')
    parser.add_argument('-t','--target',help='target_path')
    parser.add_argument('-f','--foldername',help='folder_name_to_create')
    parser.add_argument('-tmp','--tmp_id',help='tmp_id')

    args = parser.parse_args()
    mode = args.mode
    file_path = args.source
    target_path = args.target
    folder_path = args.foldername
    guid = args.tmp_id

    cloud_config_info = {
        "hostname" : "192.168.1.172",
        "port" : "8000",
        "Caller" : "Caller_name",
        "group_name" : "group_name"
    }
    
    if mode == 'list':
        # files_list = get_files_data()
        files_list = [
                {
                "Guid":"06.jpg",
                "Size":73491,
                "UAN":"5678550E-F1B5-498E-B2AB-5218D47F3CFE",
                "Volume":"LT9987345",
                "Archive":5,
                "Group":"News2015",
                "IsDeleted":"false",
                "DeleteDate":"0001-01-01T00:00:00",
                "ArchiveDate":"2016-10-28T12:35:40",
                "LastRestoreDate":"2015-01-01T00:00:00",
                "RestoreCount":0,
                "Status":0,
                "FullFileName":"null",
                "MetaData":"null"
                },
                {
                "Guid":"08.jpg",
                "Size":8500,
                "UAN":"5678550E-F1B5-498E-B2AB-5218D47F3CFE",
                "Volume":"LT9987345",
                "Archive":5,
                "Group":"News2015",
                "IsDeleted":"false",
                "DeleteDate":"0001-01-01T00:00:00",
                "ArchiveDate":"2016-10-28T12:35:40",
                "LastRestoreDate":"2015-01-01T00:00:00",
                "RestoreCount":0,
                "Status":0,
                "FullFileName":"test\\rushiraj\\08.jpg",
                "MetaData":"null"
                },
            ]      
        objects_dict = GetObjectDict(files_list)
        if objects_dict and file_path:
            generate_xml_from_file_objects(objects_dict, file_path)
            print(f"Generated XML file: {file_path}")
        else:
            print("Failed to generate XML file.")
        #os.remove(directory)
        print("GOOD")

    elif mode == 'upload':
        # response = archive_file_request(file_path)
        response = {
            "Success":"true",
            "Message":"Successfully sent to archive as request id 308",
            "Errors":[],
            "Files":
            {
            "\\\\sgl43\\data\\01.jpg":"01.jpg",
            "\\\\sgl43\\data\\02.jpg":"02.jpg",
            "\\\\sgl43\\data\\03.jpg":"7133e5c7-8bfb-4156-8486-7c01f6577bd4"
            },
            "RID":308,
            "UAN":"44753D1D-6320-42A1-AAE1-9FC436A03E04"
            }

        state_id = ""
        rid = response["RID"]
        while state_id not in list(END_STATES.keys()):
            state_id += f"{get_job_status(rid)}"
            if state_id in list(END_STATES.keys()):
                print(END_STATES[state_id])
            else:
                print(state_id)

        print("File upload Successfully",file_path)

    elif mode == "download":
        # response = restore_file_request(guid,target_path)
        response = {
            "Success": "true",
            "Errors": [],
            "RID": 297,
            "RunState": 3,
            "ExitState": 0,
            "QueuedState": 1,
            "Action": 0,
            "Status": 5,
            "DisplayName": "Archive Files Test",
            "Priority": 55,
            "Server": "SGL43",
            "Group": "null",
            "Volume": "null",
            "Changer": "null",
            "CallingProduct": "FlashNet API Service",
            "CallingServer": "SGL43",
            "LFK": 0,
            "QK": 199,
            "Message": "Not Processed (5)"
            }
        
        state_id = ""
        rid = response["RID"]
        while state_id not in list(END_STATES.keys()):
            state_id += f"{get_job_status(rid)}"
            if state_id in list(END_STATES.keys()):
                print(END_STATES[state_id])
            else:
                print(state_id)
        print("File download Successfully",target_path)
        

