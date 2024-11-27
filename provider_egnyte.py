import requests
import time
import argparse
from datetime import datetime
from action_functions import *
from urllib import request
import urllib3
import hashlib


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def get_list_call(folder_path,recursive=None):
    if not folder_path:
        folder_path = ""
    url = f"{config_map['domain']}/pubapi/v1/fs/Shared/{folder_path}"
    headers = {
    'Authorization': f"Bearer {config_map['bearer_key']}"
    }
    params = {
        'list_content' : recursive
    }

    # response = requests.get(url,headers=headers,params=params)
    response = {
      "name": "MyFolder",
      "lastModified": 1554182069000,
      "uploaded": 1554178564015,
      "count": 0,
      "offset": 0,
      "path": "/Shared/MyDocuments/MyFolder",
      "folder_id": "d7d56ebc-ce31-4ba8-a6b3-292ffb43f215", 
      "parent_id": "b0sceebc-2edl-ab56-lapq-fb11def11123", 
      "total_count": 2,
      "is_folder": "true",
      "public_links": "files_folders",
      "allow_links": "true",
      "restrict_move_delete": "false",
      "folders": [
          {
              "name": "subfolder1",
              "lastModified": 1554185307000,
              "uploaded": 1554185307326,
              "path": "/Shared/MyDocuments/MyFolder/subfolder1",
              "folder_id": "fc8cf940-1097-491e-bb9d-b55b5797331c",
              "is_folder": "true",
              "parent_id": "1d12e243-0ddb-432f-820d-8b250c0c0fdc"
          },
          {
              "name": "subfolder2",
              "lastModified": 1554185307000,
              "uploaded": 1554185307326,
              "path": "/Shared/MyDocuments/MyFolder/subfolder1",
              "folder_id": "fc8cf940-1097-491e-bb9d-b55b5797331c",
              "is_folder": "true",
              "parent_id": "d7d56ebc-ce31-4ba8-a6b3-292ffb43f215"
          }
      ],
      "files": [
          {
              "checksum": "244b99790dcc91ebc5862eb547c8179515b2369bb6db5aaa1ddd46bf0035e7ba3849ba1494b294b20b7c2a055a52d3a65ccab8a090f06cf40106528f6e23a91e",
              "size" :238428,
              "path": "/Shared/MyDocuments/MyFolder/info.pdf",
              "name": "info.pdf",
              "locked": "false",
              "is_folder": "false",
              "entry_id": "b563a343-184b-4bce-8331-25d2dfb8125a",
              "group_id": "01dd4abd-983b-4104-bff6-e2ad44bff357",
              "parent_id": "d7d56ebc-ce31-4ba8-a6b3-292ffb43f215",
              "last_modified": "Tue, 02 Apr 2019 05:12:44 GMT",
              "uploaded_by": "jsmith",
              "uploaded": 1554182069464,
              "num_versions": 1
          },
          {
              "checksum": "7b275cfc0650d8cdfae4e11a4d149ce72fd65654e9aa56ec475eb51cbad898bbd0413edd7e49198563757b29aff21ade1e848b33cf2da8a47af2652d88ccbfbf",
              "size": 9322,
              "path": "/Shared/MyDocuments/MyFolder/document.gdoc",
              "name": "document.gdoc",
              "locked": "true",
              "is_folder": "false",
              "entry_id": "d32d4628-5346-44ce-8ae6-bce367e84e60",
              "group_id": "7de6cdd8-2038-4a09-8935-9d07cf62197f",
              "parent_id": "1d12e243-0ddb-432f-820d-8b250c0c0fdc",
              "last_modified": "Mon, 23 Nov 2020 11:07:11 GMT",
              "uploaded_by": "jsmith",
              "uploaded": 1606129632000,
              "num_versions": 3,
              "lock_info": {
                    "collaboration": "ewogICJhcHBJZCIgOiAiZ2RvY3MiLAogICJpbnRlZ3JhdGlvbklkIiA6ICJnZG9jRWRpdGRvYyIKfQ==",
                    "owner_id": 1,
                    "first_name": "John",
                    "last_name": "Smith"
              }
          }
      ]
  }
    return response

def download_file(file_id):
    url = f"{config_map['domain']}/pubapi/v1/fs-content/ids/file/{file_id}"
    headers = {
    'Authorization': f"Bearer {config_map['bearer_key']}"
    }

    response = requests.get(url,headers=headers)
    return True

def create_folder(folder_path):
    url = f"{config_map['domain']}/pubapi/v1/fs/{folder_path}"
    headers = {
    'Authorization': f"Bearer {config_map['bearer_key']}"
    }
    json = {
    "action": "add_folder"
    }

    response = requests.post(url,json=json,headers=headers)
    return True


def GetObjectDict(data,params):
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

    for file in data['files']:
        mtime_struct = datetime.strptime(file['last_modified'].split(".")[0], "%a, %d %b %Y %H:%M:%S GMT")
        mtime_epoch_seconds = int(mtime_struct.timestamp())
        atime_epoch_seconds = int((file["uploaded"]/1000))
        file_path = file["path"]
        file_name = get_filename(file_path)
        checksum = file["checksum"]
        file_size = file['size']
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
            file_object["tmpid"] = f"file|{file['entry_id']}"
            file_object["checksum"] = checksum
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
        

    for folder in data['folders']:
        mtime_epoch_seconds = int((folder["lastModified"]/1000))
        atime_epoch_seconds = int((folder["uploaded"]/1000))
        folder_path = folder["path"]
        checksum = "0"
        folder_size = "0"
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
            file_object["name"] = folder_path
            file_object["size"] = folder_size
            file_object["mode"] = "0"
            file_object["tmpid"] = f"folder|{folder['folder_id']}"
            file_object["checksum"] = checksum
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
    parser.add_argument('-tmp','--tmp_id',help='tmp_id')
    parser.add_argument('-in', '--indexid', required=False, help = 'REQUIRED if list')
    parser.add_argument('-jg', '--jobguid', required=False, help = 'REQUIRED if list')
    parser.add_argument('-ji', '--jobid', required=False, help = 'REQUIRED if bulk restore.')
    parser.add_argument('-p', "--projectname", required=False, help = 'Project name')
    

    args = parser.parse_args()
    mode = args.mode
    file_path = args.source
    folder_path = args.foldername
    tmp_id = args.tmp_id
    target_path = args.target

    config_map = loadConfigurationMap(args.config)
    logging_dict = loadLoggingDict(os.path.basename(__file__), args.jobguid)
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

    if mode == 'list':
        data = get_list_call(folder_path,recursive="true")
        objects_dict = GetObjectDict(data,params_map)
        if len(data) == 0:
            objects_dict = {}
            
        if objects_dict and target_path:
            generate_xml_from_file_objects(objects_dict, target_path)
            print(f"Generated XML file: {target_path}")
            exit(0)
        else:
            print("Failed to generate XML file.")
            exit(1)


    elif mode == 'upload':
        pass

    elif mode == 'browse':
        folders_data = get_list_call(folder_path,recursive="false")
        folders_list = []
        for folder in folders_data["folders"]:
            folders_list.append(folder["name"])
        xml_output = add_CDATA_tags(folders_list)
        print(xml_output)
        exit(0)

    elif mode == "download":
        type = tmp_id.split("|")[0]
        if type == "file":
            file_id = tmp_id.split("|")[-1]
        else:
            print("Add correct file id")
            exit(1)
        if file_id:
            if download_file(file_id):
                print("File download sucessfully")
                exit(0)
            else:
                print("Error While downloading File.")
                exit(1)
        else:
            print("File id not exist.")


    elif mode == "createfolder":
        if create_folder(folder_path):
            print("Folder created.")
            exit(0)
        else:
            print("Failed to create a folder")

    else:
        print(f'Unsupported mode {mode}')
        exit(1)