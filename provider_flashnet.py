import requests
import argparse
from datetime import datetime
from action_functions import *
from urllib import request
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

VALID_MODES = ['upload', 'download', 'list']

END_STATES = {"5" : "PASSED", "4":"FAILED", "2": "KILLED"}


def get_files_data():
    url = f"http://{params_map["hostname"]}:{params_map["port"]}/flashnet/api/v1/files/filter/"
    params = {"caller" : params_map["Caller"]}
    response = requests.get(url, params=params).json()
    return response["Results"]

def archive_file_request(file_path):
    url = f"http://{params_map["hostname"]}:{params_map["port"]}/flashnet/api/v1/assets/"
    payload = {
        "Caller" : params_map["Caller"],
        "Priority" : 1,
        "Target" : params_map["group_name"],
        "VerifyFiles":"true",
        "DeleteFiles":"false",
        "Files" : [
            {
                "FullFileName" : file_path
            }
        ]
    }
    response = requests.post(url, json=payload).json()
    return response

def restore_file_request(guid,target_path):
    url = f"http://{params_map["hostname"]}:{params_map["port"]}/flashnet/api/v1/files/"
    payload = {
        "Caller" : params_map["Caller"],
        "Priority" : 1,
        "Files" : [
            {
                "Guid" : guid, 
                "Path" : target_path
            }
        ]
    }
    response = requests.post(url, json=payload).json()
    return response

def get_job_status(rid):
    url = f"http://{params_map["hostname"]}:{params_map["port"]}/flashnet/api/v1/jobs/{rid}"
    # response = requests.get(url).json()
    response = {"ExitState" : 5}
    return response["ExitState"]

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

    for data in files_list:
        mtime_struct = datetime.strptime(data['ArchiveDate'], "%Y-%m-%dT%H:%M:%S")
        atime_struct = datetime.strptime(data['LastRestoreDate'], "%Y-%m-%dT%H:%M:%S")
        mtime_epoch_seconds = int(mtime_struct.timestamp())
        atime_epoch_seconds = int(atime_struct.timestamp())
        file_path = data["FullFileName"]
        file_name = get_filename(file_path)
        file_size = data["Size"]
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
            file_object["tmpid"] = data["Guid"]
            file_object["type"] = "F_REG" if file_type == "file" else "F_DIR"
            file_object["mtime"] = f'{mtime_epoch_seconds}'
            file_object["atime"] = f'{atime_epoch_seconds}'
            file_object["owner"] = "0"
            file_object["group"] = "0"
            file_object["index"] = params["indexid"]
            
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
    parser.add_argument('-c', '--config', required = True, help = 'Configuration name')
    parser.add_argument('-m', '--mode', required = True, help = 'upload, download,list,create_folder')
    parser.add_argument('-s','--source',help='source file')
    parser.add_argument('-t','--target',help='target_path')
    parser.add_argument('-f','--foldername',help='folder_name_to_create')
    parser.add_argument('-tmp','--tmp_id',help='tmp_id')
    parser.add_argument('-ft', '--filtertype', required=False, choices=['none', 'include', 'exclude'], help='Filter type')
    parser.add_argument('-ff', '--filterfile', required=False, help='Extension file')
    parser.add_argument('-pf', '--policyfile', required=False, help='Policy file')
    parser.add_argument('-in', '--indexid', required=False, help = 'REQUIRED if list')
    parser.add_argument('-jg', '--jobguid', required=False, help = 'REQUIRED if list')
    parser.add_argument('-ji', '--jobid', required=False, help = 'REQUIRED if bulk restore.')


    args = parser.parse_args()
    mode = args.mode
    file_path = args.source
    target_path = args.target
    folder_path = args.foldername
    guid = args.tmp_id

    logging_dict = loadLoggingDict(os.path.basename(__file__), args.jobguid)
    # config_map = loadConfigurationMap(args.config)
    config_map = {
        "hostname" : "192.168.1.172",
        "port" : "8000",
        "Caller" : "Caller_name",
        "group_name" : "group_name"
    }
    
    params_map = {}
    params_map["foldername"] = args.foldername
    params_map["source"] = args.source
    params_map["target"] = args.target
    params_map["filtertype"] = args.filtertype
    params_map["filterfile"] = args.filterfile
    params_map["policyfile"] = args.policyfile
    params_map["indexid" ] = args.indexid
    params_map["jobguid"] = args.jobguid
    params_map["jobid"] = args.jobid

    for key in config_map:
        if key in params_map:
            print(f'Skipping existing key {key}')
        else:
            params_map[key] = config_map[key]

    if mode == 'actions':
        print('upload,download,list')
        exit(0)

    if mode == 'list':
        # files_list = get_files_data(params_map)
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
        objects_dict = GetObjectDict(files_list,params_map)
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
        

