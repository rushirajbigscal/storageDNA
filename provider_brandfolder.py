import requests
import argparse
from action_functions import *
import urllib3
from datetime import datetime


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

domain = "https://brandfolder.com"

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

'''
def get_list_brandfolder():
    headers = {
    'Accept': 'application/json',
    'Authorization': f'Bearer {cloud_config_info["Bearer_key"]}'
    }

    # response  = requests.get(f'{domain}/api/v4/brandfolders', headers=headers).json()
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

def get_call_list_of_assets(collectionid):
    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {cloud_config_info["Bearer_key"]}'
        }
    params = {
        "fields":"metadata",
        "include":"attachments",
        }
    response  = requests.get(f'{domain}/api/v4/collections/{collectionid}/assets',headers=headers,params=params).json()

    return response 


def get_list_of_collections():
    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {cloud_config_info["Bearer_key"]}'
        }

    response  = requests.get(f'{domain}/api/v4/collections',headers=headers).json()

    return response["data"]

def create_collection(brandfolder_id,collection_name):
    headers = {
    'Accept': 'application/json',
    'Authorization': f'Bearer {cloud_config_info["Bearer_key"]}'
    }

    data = {
        "data": {
            "attributes": {
                "name": collection_name
            }
        }
        }


    response = requests.post(f'{domain}/api/v4/brandfolders/{brandfolder_id}/collections', json=data,headers=headers)
    return response["data"]["id"]


def get_upload_request(file_path):
    headers = {
    'Accept': 'application/json',
    'Authorization': f'Bearer {cloud_config_info["Bearer_key"]}'
    }
    response = requests.get(f'{domain}/api/v4/upload_requests',headers=headers).json()
    upload_url = response["upload_url"]
    object_url = response["object_url"]

    with open(file_path, 'rb') as f:
        x = requests.put(upload_url,data=f)
        print(x.text)
    return object_url

def create_asset_call(collection_id,upload_url,file_path):
    headers = {
    'Authorization': f'Bearer {cloud_config_info["Bearer_key"]}'
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
        "section_key": "jknv4r2nk5jwwmqjp2vkcnz"
        }
    response = requests.post(f'{domain}/api/v4/collections/{collection_id}/assets',headers=headers,json=body).json()
    print(response)

def get_download_link(attachment_id):
    headers = {
    'Accept': 'application/json',
    'Authorization': f'Bearer {cloud_config_info["Bearer_key"]}'
    }
    response = requests.get(f'{domain}/api/v4/attachments/{attachment_id}',headers=headers).json()

    return response["data"]["attributes"]["filename"],response["data"]["attributes"]["url"]

def get_attachment_metadata(attachment_id):
    headers = {
    'Accept': 'application/json',
    'Authorization': f'Bearer {cloud_config_info["Bearer_key"]}'
    }
    params = {
    "fields":"metadata"
            }
    response = requests.get(f'{domain}/api/v4/attachments/{attachment_id}',headers=headers,params=params).json()

    return response["data"]["attributes"]["metadata"]


def get_list_of_sections(collection_id):
    headers = {
    'Accept': 'application/json',
    'Authorization': f'Bearer {cloud_config_info["Bearer_key"]}'
    }
    response = requests.get(f'{domain}/api/v4/collections/{collection_id}/sections',headers=headers).json()

    return response["data"]

def process_dict_data(data):
    dict_list = []
    processed_data_list = []

    for attchment_data in data["included"]:
        dict = {}
        dict["file_id"] = attchment_data["id"]
        dict["file_data"] = attchment_data["attributes"]
        dict_list.append(dict)

    for asset_data in data["data"]:
        for dict in dict_list:
            for attachment in asset_data["relationships"]["attachments"]["data"]:
                if dict["file_id"] == attachment["id"]:
                    processed_data_dict = {}
                    processed_data_dict["file"] = dict
                    processed_data_dict["asset_id"] = asset_data["id"]
                    processed_data_dict["asset_data"] = asset_data["attributes"]
                    processed_data_list.append(processed_data_dict)
        
    return processed_data_list


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
    for file_data in files_list:
        for data in file_data:
            attachment_id = data["file"]["file_id"]
            attachment_metadata = get_attachment_metadata(attachment_id)
            asset_metadata = data["asset_data"]["metadata"]
            mtime_struct = datetime.strptime(attachment_metadata["file_modify_date"].split("+")[0], "%Y:%m:%d %H:%M:%S") 
            atime_struct = datetime.strptime(attachment_metadata['file_access_date'].split("+")[0], "%Y:%m:%d %H:%M:%S")
            mtime_epoch_seconds = int(mtime_struct.timestamp())
            atime_epoch_seconds = int(atime_struct.timestamp())
            file_mode = symbolic_to_hex(attachment_metadata["file_permissions"])

            asset_metadata_file_name = f"C:\\temp\\asset_{data["asset_id"]}.html"
            file_metadata_file_name = f"C:\\temp\\file_{attachment_id}.html"
            asset_html = generate_html(asset_metadata,asset_metadata_file_name)
            file_html = generate_html(attachment_metadata,file_metadata_file_name)
           
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
                file_object["name"] = data["asset_data"]["name"] +"/"+ data["file"]["file_data"]["filename"]
                file_object["size"] = data["file"]["file_data"]["size"]
                file_object["mode"] = file_mode
                file_object["tmpid"] = f"{data["asset_id"]}|{attachment_id}"
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
    parser.add_argument('-m', '--mode', required = True, help = 'upload, download, list,create_folder')
    parser.add_argument('-s','--source',help='source file')
    parser.add_argument('-t','--target',help='target_path')
    parser.add_argument('-f','--foldername',help='folder_name_to_create')
    # parser.add_argument('-bid','--brandfolder_id',help='brandfolder_id')
    parser.add_argument('-id','--collection_id',help='collection_id')
    parser.add_argument('-tmp','--tmp_id',help='tmp_id')



    args = parser.parse_args()
    mode = args.mode
    file_path = args.source
    target_path = args.target
    folder_name = args.foldername
    # brandfolder_id = args.brandfolder_id
    collectionid = args.collection_id
    tmp_id = args.tmp_id

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

    cloud_config_info = {
                    "Bearer_key" : "eyJhbGciOiJIUzI1NiJ9.eyJvcmdhbml6YXRpb25fa2V5IjoicGcyeW13LTN1cnpway01d2dzczUiLCJpYXQiOjE3MjgwMjY0MzcsInVzZXJfa2V5IjoiOTMzdG5uY3I2Yng2NDZ4cGNjdDM3cHoiLCJzdXBlcnVzZXIiOmZhbHNlfQ.9PVGOlORzpoMofvkJA9Vffy027QgScNavVAFVvZGedE"
                }
    
    if mode == 'list':
        files_list = []
        if collectionid:
            collection_id_list = [collectionid]
        else:
            collections_list = get_list_of_collections()
            collection_id_list = [collection_id["id"] for collection_id in collections_list]

        for collection_id in collection_id_list:
            data = get_call_list_of_assets(collection_id)
            files_list.append(process_dict_data(data))
        objects_dict = GetObjectDict(files_list)
        if objects_dict and file_path:
            generate_xml_from_file_objects(objects_dict, file_path)
            print(f"Generated XML file: {file_path}")
        else:
            print("Failed to generate XML file.")
        #os.remove(directory)
        print ("GOOD")

    elif mode == 'upload':
        upload_url = get_upload_request(file_path)
        create_asset_call(collectionid,upload_url,file_path)
        print("File Upload succesfull:",file_path)

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
        # generate_xml(file_path,xml_output)
        # print(f"Generated XML file: {file_path}")

    elif mode == "download":
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

    # elif mode == "createfolder":
    #     collection_id = create_collection(brandfolder_id,folder_name)
    #     print(collection_id)


