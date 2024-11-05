import requests
import argparse
from action_functions import *
import urllib3
from datetime import datetime


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

domain = "https://brandfolder.com"

'''
def get_list_brandfolder():
    headers = {
    'Accept': 'application/json',
    'Authorization': f'Bearer {config_map["Bearer_key"]}'
    }

    # response  = requests.get(f'{domain}/api/v4/brandfolders', headers=headers)
        if response.status_code != 200:
        print(f"Response error. Status - {response.status_code}, Error - {response.text}")
        return False
    response = response.json()
    response  = {
            "data": [
                {
                "id": "oqgiju-21olts-ce9egi",
                "type": "brandfolders",
                "attributes": {
                    "name": "Brandfolder",
                    "tagline": "You expected this - Brandfolder's Brandfolder!",
                    "privacy": "public",
                    "slug": "brandfolder"
                }
                },
                {
                "id": "oqgiju-ce9egi",
                "type": "brandfolders",
                "attributes": {
                    "name": "testfolder",
                    "tagline": "You expected this - Brandfolder's testfolder!",
                    "privacy": "public",
                    "slug": "brandfolder"
                }
                },
                {
                "id": "213465987",
                "type": "brandfolders",
                "attributes": {
                    "name": "test",
                    "tagline": "You expected this - Brandfolder's test!",
                    "privacy": "public",
                    "slug": "brandfolder"
                }
                }
            ]
            }
    data = response ["data"]
    return [x["id"] for x in data]
'''

def strdata_to_logging_file(str_data, filename):
    f = open(filename, "a")
    f.write(f'{str_data}\n')
    f.close()

def get_call_list_of_assets(collectionid):
    url = f'{domain}/api/v4/collections/{collectionid}/assets'
    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {config_map["Bearer_key"]}'
        }
    params = {
        "fields":"metadata",
        "include":"attachments",
        }
    response  = requests.get(url,headers=headers,params=params)
    if response.status_code != 200:
        if logging_dict["logging_level"] > 0:
            strdata_to_logging_file(url, logging_dict["logging_error_filename"])
        print(f"Response error. Status - {response.status_code}, Error - {response.text}")
        return False
    elif logging_dict["logging_level"] > 1:
        strdata_to_logging_file(url, logging_dict["logging_filename"])
    response = response.json()
    return response["data"] 

def get_call_list_of_files(collectionid):
    url = f'{domain}/api/v4/collections/{collectionid}/attachments'
    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {config_map["Bearer_key"]}'
        }
    params = {
        "fields":"metadata"
        }
    response  = requests.get(url,headers=headers,params=params)
    if response.status_code != 200:
        if logging_dict["logging_level"] > 0:
            strdata_to_logging_file(url, logging_dict["logging_error_filename"])
        print(f"Response error. Status - {response.status_code}, Error - {response.text}")
        return False
    elif logging_dict["logging_level"] > 1:
        strdata_to_logging_file(url, logging_dict["logging_filename"])
    response = response.json()
    return response["data"]

def get_list_of_collections():
    url = f'{domain}/api/v4/collections'
    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {config_map["Bearer_key"]}'
        }

    response  = requests.get(url,headers=headers)
    if response.status_code != 200:
        if logging_dict["logging_level"] > 0:
            strdata_to_logging_file(url, logging_dict["logging_error_filename"])
        print(f"Response error. Status - {response.status_code}, Error - {response.text}")
        return False
    elif logging_dict["logging_level"] > 1:
        strdata_to_logging_file(url, logging_dict["logging_filename"])
    response = response.json()
    return response["data"]

def create_collection(brandfolder_id,collection_name):
    url = f'{domain}/api/v4/brandfolders/{brandfolder_id}/collections'
    headers = {
    'Accept': 'application/json',
    'Authorization': f'Bearer {config_map["Bearer_key"]}'
    }

    data = {
        "data": {
            "attributes": {
                "name": collection_name
            }
        }
        }

    response = requests.post(url, json=data,headers=headers)
    return response["data"]["id"]


def get_upload_request(file_path):
    url = f'{domain}/api/v4/upload_requests'
    headers = {
    'Authorization': f'Bearer {config_map["Bearer_key"]}'
    }
    response = requests.get(url,headers=headers)
    if response.status_code != 200:
        if logging_dict["logging_level"] > 0:
            strdata_to_logging_file(url, logging_dict["logging_error_filename"])
        print(f"Response error. Status - {response.status_code}, Error - {response.text}")
        return False
    elif logging_dict["logging_level"] > 1:
        strdata_to_logging_file(url, logging_dict["logging_filename"])
    response = response.json()
    upload_url = response["upload_url"]
    object_url = response["object_url"]

    with open(file_path, 'rb') as f:
        x = requests.put(upload_url,data=f)
    if x.status_code !=200:
        if logging_dict["logging_level"] > 0:
            strdata_to_logging_file(url, logging_dict["logging_error_filename"])
        print(f"Response error. Status - {response.status_code}, Error - {response.text}")
        return False
    elif logging_dict["logging_level"] > 1:
        strdata_to_logging_file(url, logging_dict["logging_filename"])
    return object_url

def create_asset_call(collection_id,upload_url,file_path,section_key):
    url = f'{domain}/api/v4/collections/{collection_id}/assets'
    headers = {
    'Authorization': f'Bearer {config_map["Bearer_key"]}'
    }
    file_path = os.path.normpath(file_path)
    body = {
        "data": {
            "attributes": [
            {
                "attachments": [
                {
                    "url": upload_url,
                    "filename": file_path.split("\\")[-1]
                }
                ]
            }
            ]
        },
        "section_key": section_key
        }
    response = requests.post(url,headers=headers,json=body)
    if response.status_code != 200:
        if logging_dict["logging_level"] > 0:
            strdata_to_logging_file(url, logging_dict["logging_error_filename"])
        print(f"Response error. Status - {response.status_code}, Error - {response.text}")
        return False
    elif logging_dict["logging_level"] > 1:
        strdata_to_logging_file(url, logging_dict["logging_filename"])
    
    return response

def get_download_link(attachment_id):
    url = f'{domain}/api/v4/attachments/{attachment_id}'
    headers = {
    'Accept': 'application/json',
    'Authorization': f'Bearer {config_map["Bearer_key"]}'
    }
    response = requests.get(url,headers=headers)
    if response.status_code != 200:
        if logging_dict["logging_level"] > 0:
            strdata_to_logging_file(url, logging_dict["logging_error_filename"])
        print(f"Response error. Status - {response.status_code}, Error - {response.text}")
        return False
    elif logging_dict["logging_level"] > 1:
        strdata_to_logging_file(url, logging_dict["logging_filename"])
    response = response.json()
    return response["data"]["attributes"]["filename"],response["data"]["attributes"]["url"]

def get_attachment_metadata(attachment_id):
    url = f'{domain}/api/v4/attachments/{attachment_id}'
    headers = {
    'Accept': 'application/json',
    'Authorization': f'Bearer {config_map["Bearer_key"]}'
    }
    params = {
    "fields":"metadata"
            }
    response = requests.get(url,headers=headers,params=params)
    if response.status_code != 200:
        if logging_dict["logging_level"] > 0:
            strdata_to_logging_file(url, logging_dict["logging_error_filename"])
        print(f"Response error. Status - {response.status_code}, Error - {response.text}")
        return False
    elif logging_dict["logging_level"] > 1:
        strdata_to_logging_file(url, logging_dict["logging_filename"])
    response = response.json()
    return response["data"]["attributes"]["metadata"]


def get_list_of_sections(collection_id):
    url = f'{domain}/api/v4/collections/{collection_id}/sections'
    headers = {
    'Accept': 'application/json',
    'Authorization': f'Bearer {config_map["Bearer_key"]}'
    }
    response = requests.get(url,headers=headers)
    if response.status_code != 200:
        if logging_dict["logging_level"] > 0:
            strdata_to_logging_file(url, logging_dict["logging_error_filename"])
        print(f"Response error. Status - {response.status_code}, Error - {response.text}")
        return False
    elif logging_dict["logging_level"] > 1:
        strdata_to_logging_file(url, logging_dict["logging_filename"])
    response = response.json()
    return response["data"]

def process_dict_data(asset_data,file_data):
    dict_list = []
    processed_data_list = []

    for attchment_data in file_data:
        dict = {}
        dict["file_id"] = attchment_data["id"]
        dict["file_data"] = attchment_data["attributes"]
        dict_list.append(dict)

    for assets in asset_data:
        for dict in dict_list:
            for attachment in assets["relationships"]["attachments"]["data"]:
                if dict["file_id"] == attachment["id"]:
                    processed_data_dict = {}
                    processed_data_dict["file"] = dict
                    processed_data_dict["asset_id"] = assets["id"]
                    processed_data_dict["asset_data"] = assets["attributes"]
                    processed_data_list.append(processed_data_dict)

    return processed_data_list


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


    for file_data in files_list:
        for data in file_data:
            attachment_id = data["file"]["file_id"]
            attachment_metadata = data["file"]["file_data"]["metadata"]
            asset_metadata = data["asset_data"]["metadata"]
            mtime_struct = datetime.strptime(attachment_metadata["file_modify_date"].split("+")[0], "%Y:%m:%d %H:%M:%S") 
            atime_struct = datetime.strptime(attachment_metadata['file_access_date'].split("+")[0], "%Y:%m:%d %H:%M:%S")
            mtime_epoch_seconds = int(mtime_struct.timestamp())
            atime_epoch_seconds = int(atime_struct.timestamp())
            file_mode = symbolic_to_hex(attachment_metadata["file_permissions"])
            file_path = data["asset_data"]["name"] +"/"+ data["file"]["file_data"]["filename"]
            file_name = get_filename(file_path)
            file_size = data["file"]["file_data"]["size"]
            file_type = "file" if file_size != "0" else "dir"

            asset_metadata_file_name = f"C:/temp/asset_{data["asset_id"]}.html"
            attachment_metadata_file_name = f"C:/temp/file_{attachment_id}.html"
            asset_html = generate_html(asset_metadata,asset_metadata_file_name)
            file_html = generate_html(attachment_metadata,attachment_metadata_file_name)
           
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
                file_object["mode"] = file_mode
                file_object["url"] = f"{asset_html}|{file_html}"
                file_object["tmpid"] = f"{data["asset_id"]}|{attachment_id}"
                file_object["type"] = "F_REG" if file_type == "file" else "F_DIR"
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
    parser.add_argument('-c', '--config', required = True, help = 'Configuration name')
    parser.add_argument('-m', '--mode', required = True, help = 'upload, download, list,create_folder')
    parser.add_argument('-s','--source',help='source file')
    parser.add_argument('-t','--target',help='target_path')
    parser.add_argument('-f','--foldername',help='folder_name_to_create')
    # parser.add_argument('-bid','--brandfolder_id',help='brandfolder_id')
    parser.add_argument('-id','--collection_id',help='collection_id')
    parser.add_argument('-tmp','--tmp_id',help='tmp_id')
    parser.add_argument('-sid','--section_id',help='section_id')
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
    folder_name = args.foldername
    # brandfolder_id = args.brandfolder_id
    collectionid = args.collection_id
    tmp_id = args.tmp_id
    section_id = args.section_id


    # config_map = loadConfigurationMap(args.config)
    logging_dict = loadLoggingDict(os.path.basename(__file__), args.jobguid)
    config_map = {
                    "Bearer_key" : "eyJhbGciOiJIUzI1NiJ9.eyJvcmdhbml6YXRpb25fa2V5IjoicGcyeW13LTN1cnpway01d2dzczUiLCJpYXQiOjE3MjgwMjY0MzcsInVzZXJfa2V5IjoiOTMzdG5uY3I2Yng2NDZ4cGNjdDM3cHoiLCJzdXBlcnVzZXIiOmZhbHNlfQ.9PVGOlORzpoMofvkJA9Vffy027QgScNavVAFVvZGedE"
                }

    params_map = {}
    params_map["foldername"] = args.foldername
    params_map["source"] = args.source
    params_map["target"] = args.target
    params_map["collectionid"] = args.collection_id
    params_map["tmp_id"] = args.tmp_id
    params_map["section_id"] = args.section_id
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
        print('upload,browse,download,list')
        exit(0)
    
    if mode == 'list':
        if target_path is None:
             print('Target path (-t <targetpath> ) option are required for list')
             exit(1)
        files_list = []
        if collectionid:
            collection_id_list = [collectionid]
        else:
            collections_list = get_list_of_collections()
            collection_id_list = [collection_id["id"] for collection_id in collections_list]

        for collection_id in collection_id_list:
            asset_data = get_call_list_of_assets(collection_id)
            file_data = get_call_list_of_files(collection_id)
            files_list.append(process_dict_data(asset_data,file_data))
        objects_dict = GetObjectDict(files_list,params_map)
        if objects_dict and target_path:
            generate_xml_from_file_objects(objects_dict, target_path)
            exit(0)
            print(f"Generated XML file: {target_path}")
        else:
            print("Failed to generate XML file.")
            exit(1)

    elif mode == 'upload':
        if file_path is None or collectionid is None or section_id is None:
             print('Target path (-s <source> ) and Collection Id (-id <collection_id>) and Section Id (-sid <section_id>) options are required for list')
             exit(1)
        upload_url = get_upload_request(file_path)
        if upload_url:
            response = create_asset_call(collectionid,upload_url,file_path,section_id)
            if response.status_code != 200:
                print(f"Response error. Status - {response.status_code}, Error - {response.text}")
                exit(1)
            else:
                print(f"File Upload succesfull:{file_path}")
                exit(0)
        else:
            print("Faild to genrate Upload URL.")

    elif mode == 'browse':
        collection_name = []
        if collectionid:
            collections = get_list_of_sections(collectionid)
        else:
            collections = get_list_of_collections()
        for collection in collections:
            collection_name.append({"name" : collection["attributes"]["name"],
                                "id" : collection["id"]
                                })
        xml_output = add_CDATA_tags_with_id(collection_name)
        print(xml_output)
        exit(0)

    elif mode == "download":
        if target_path is None or tmp_id is None:
            print('Target path (-t <targetpath> ) and Tmp id (-tmp <tmp_id>) options are required for download')
            exit(1)
        download_path = os.path.normpath(target_path)
        tmp_id_list = tmp_id.split("|")
        attachment_id = tmp_id_list[1]
        file_name ,url = get_download_link(attachment_id)
        response = requests.get(url)
        if response.status_code == 200:
            os.makedirs(download_path, exist_ok=True)
            download_path = os.path.join(download_path, file_name)
            with open(download_path, 'wb') as file:
                file.write(response.content)
            print(f"File download at {download_path}")
        else:
            print(f"Error while downloding File:{file_name}")

    # elif mode == "createfolder":
    #     collection_id = create_collection(brandfolder_id,folder_name)
    #     print(collection_id)


    else:
        print(f'Unsupported mode {mode}')
        exit(1)


