import requests
import time
import argparse
from datetime import datetime
from action_functions import *
from urllib import request
import urllib3
import hashlib


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

VALID_MODES = ['upload', 'download', 'list']

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


file_ids = []
collection_ids = []
domain = 'https://app.iconik.io'

def get_call_of_collections():
    url = f"{domain}/API/assets/v1/collections/"
    headers = {"App-ID":cloud_config_info["App-ID"],
            "Auth-Token" : cloud_config_info["Auth-Token"]
            }
    resp_JSON = requests.get(url, headers=headers).json()
    for collection_id in resp_JSON['objects']:
        collection_ids.append({"collection_id" : collection_id['id'],"collectionname" : collection_id['title']}) 
    return resp_JSON['objects']

def get_call_of_collections_content(collection_id):
    url = f"{domain}/API/assets/v1/collections/{collection_id}/contents"
    headers = {"App-ID":cloud_config_info["App-ID"],
            "Auth-Token" : cloud_config_info["Auth-Token"]
            }
    resp_JSON = requests.get(url, headers=headers).json()
    return resp_JSON['objects']

def get_storage_id(storage_name,storage_method):
    headers = {"App-ID":cloud_config_info["App-ID"],
            "Auth-Token" : cloud_config_info["Auth-Token"]
            }
    response = requests.get(f'{domain}/API/files/v1/storages/', headers=headers).json()
    storage_id = []
    for storage in response["objects"]:
        if storage["name"] == storage_name and storage["method"] == storage_method:
            storage_id.append(storage["id"])
    return storage_id[0]

def create_asset_id(file_name,collection_id):
    payload = {"title": file_name,"type":"ASSET","collection_id":collection_id}
    payload_default = {"title": file_name,"type":"ASSET"}
    params  = {"apply_default_acls":"false","apply_collection_acls":"true"}
    params_default  = {"apply_default_acls":"true"}

    headers = {"App-ID":cloud_config_info["App-ID"],
               "Auth-Token" : cloud_config_info["Auth-Token"]
               }
    response = requests.post(
        f'{domain}/API/assets/v1/assets/',
        headers=headers,
        json=payload if collection_id else payload_default,
        params=params if collection_id else params_default
    ).json()
    return response['id'], response['created_by_user']


def create_collection(collection_name,collection_id = None):
    payload = {"title": collection_name,"parent_id":collection_id}
    payload_default = {"title": collection_name}

    params  = {"apply_default_acls":"true","restrict_collection_acls":"true"}

    headers = {"App-ID":cloud_config_info["App-ID"],
               "Auth-Token" : cloud_config_info["Auth-Token"]
               }
    response = requests.post(
        f'{domain}/API/assets/v1/collections/', 
        headers=headers, 
        json=payload if collection_id else payload_default,
        params=params if collection_id else None
    ).json()
    return response['id']


def get_filename_from_asset(asset_id):
    headers = {"App-ID":cloud_config_info["App-ID"],
            "Auth-Token" : cloud_config_info["Auth-Token"]
            }
    response = requests.get(f'{domain}/API/assets/v1/assets/{asset_id}/', headers=headers).json()
    return response["title"]

def add_asset_in_collection(asset_id,collection_id):
    payload = {"object_id":asset_id,"object_type":"assets"}
    params = {"index_immediately":"true"}
    headers = {"App-ID":cloud_config_info["App-ID"],
            "Auth-Token" : cloud_config_info["Auth-Token"]
            }
    requests.post(f'{domain}/API/assets/v1/collections/{collection_id}/contents', headers=headers, json=payload,params=params)
    
def create_format_id(asset_id, user_id):
    payload = {"user_id": user_id,"name": "ORIGINAL","metadata": [{"internet_media_type": "text/plain"}],"storage_methods": [cloud_config_info["method"]]}
    headers = {"App-ID":cloud_config_info["App-ID"],
               "Auth-Token" : cloud_config_info["Auth-Token"]
               }
    response = requests.post(f'{domain}/API/files/v1/assets/{asset_id}/formats/', headers=headers, json=payload).json()
    return response['id']

def create_fileset_id(asset_id, format_id, file_name, storage_id,upload_path):
    payload = {"format_id": format_id,"storage_id": storage_id,"base_dir": upload_path,"name": file_name,"component_ids": []}
    headers = {"App-ID":cloud_config_info["App-ID"],
               "Auth-Token" : cloud_config_info["Auth-Token"]
               }
    response = requests.post(f'{domain}/API/files/v1/assets/{asset_id}/file_sets/', headers=headers, json=payload).json()
    return response['id']


def get_upload_url(asset_id, file_name, file_size, fileset_id, storage_id, format_id,upload_path):
    file_info = {
        'original_name': file_name,
        'directory_path': upload_path,
        'size': file_size,
        'type': 'FILE',
        'storage_id': storage_id,
        'file_set_id': fileset_id,
        'format_id': format_id
    }
    headers = {"App-ID":cloud_config_info["App-ID"],
               "Auth-Token" : cloud_config_info["Auth-Token"]
               }
    response = requests.post(f'{domain}/API/files/v1/assets/{asset_id}/files/', headers=headers, json=file_info).json()
    return response['upload_url'], response['id']

def get_upload_url_s3(asset_id, file_name, file_size, fileset_id, storage_id, format_id,upload_path):
    file_info = {
        'original_name': file_name,
        'directory_path': upload_path,
        'size': file_size,
        'type': 'FILE',
        'storage_id': storage_id,
        'file_set_id': fileset_id,
        'format_id': format_id
    }
    headers = {"App-ID":cloud_config_info["App-ID"],
               "Auth-Token" : cloud_config_info["Auth-Token"]
               }
    response = requests.post(f'{domain}/API/files/v1/assets/{asset_id}/files/', headers=headers, json=file_info).json()
    return response['multipart_upload_url'], response['id']

def get_upload_url_b2(asset_id, file_name, file_size, fileset_id, storage_id, format_id,upload_path):
    file_info = {
        'original_name': file_name,
        'directory_path': upload_path,
        'size': file_size,
        'type': 'FILE',
        'storage_id': storage_id,
        'file_set_id': fileset_id,
        'format_id': format_id
    }
    headers = {"App-ID":cloud_config_info["App-ID"],
               "Auth-Token" : cloud_config_info["Auth-Token"]
               }
    response = requests.post(f'{domain}/API/files/v1/assets/{asset_id}/files/', headers=headers, json=file_info).json()
    return response['upload_url'], response['id'],response["upload_credentials"]["authorizationToken"],response["upload_filename"]


def get_upload_id_s3(upload_url):
    response = requests.post(upload_url)
    root = ET.fromstring(response.text)
    namespace = root.tag.split('}')[0] + '}'
    upload_id = root.find(f'{namespace}UploadId').text
    return upload_id

def get_part_url_s3(asset_id, file_id, upload_id):
    part_url = f'{domain}/API/files/v1/assets/{asset_id}/files/{file_id}/multipart_url/part/'
    params = {"parts_num": "1", "upload_id": upload_id, "per_page": "100", "page": "1"}
    headers = {"App-ID":cloud_config_info["App-ID"],
               "Auth-Token" : cloud_config_info["Auth-Token"]
               }
    response = requests.get(part_url, headers=headers, params=params)
    part_data = response.json()
    return part_data["objects"][0]["url"]

def upload_file_gcs(upload_url, file_path, file_size):
    google_headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-encoding': 'gzip, deflate, br',
        'accept-language': 'sv-SE,sv;q=0.9,en-US;q=0.8,en;q=0.7,nb;q=0.6,da;q=0.5',
        'content-length': '0',
        'dnt': '1',
        'origin': domain,
        'referer': f'{domain}/upload',
        'x-goog-resumable': 'start',
    }
    resp_JSON = requests.post(upload_url, headers=google_headers)
    upload_id = resp_JSON.headers['X-GUploader-Uploadid']

    google_headers = {
        'content-length': str(file_size),
        'x-goog-resumable': 'upload'
    }

    with open(file_path, 'rb') as f:
        full_upload_url = f"{upload_url}&upload_id={upload_id}"
        x = requests.put(full_upload_url, headers=google_headers, data=f)
        print(x.text)

def upload_file_s3(part_url, file_path,upload_id, asset_id, file_id):
    with open(file_path, 'rb') as file:
        response = requests.put(part_url, data=file)
        etag = response.headers['etag']
    complete_url_response = requests.get(
        f'{domain}/API/files/v1/assets/{asset_id}/files/{file_id}/multipart_url/',
        headers={"App-ID":cloud_config_info["App-ID"],
               "Auth-Token" : cloud_config_info["Auth-Token"]
               },
        params={"upload_id": upload_id, "type": "complete_url"}
    ).json()
    
    complete_url = complete_url_response['complete_url']
    xml_payload = '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
    <CompleteMultipartUpload>
    <Part>
        <ETag>{etag}</ETag>
        <PartNumber>1</PartNumber>
    </Part>
    </CompleteMultipartUpload>'''.format(etag=etag)
    headers = {'Content-Type': 'application/xml'}
    response = requests.post(complete_url, data=xml_payload, headers=headers)
    return response.status_code, response.text

def upload_file_b2(upload_url,authorizationToken,file_path,upload_filename,sha1_of_file):
    headers = {
               "Authorization" : authorizationToken,
               "X-Bz-File-Name" : upload_filename,
               "X-Bz-Content-Sha1" : sha1_of_file,
               "Content-Type" : "b2/x-auto"
               }
    
    with open(file_path, 'rb') as f:
        x = requests.post(upload_url,headers=headers,data=f)
        print(x.text)

def upload_file_azure(upload_url, file_path):
    headers = { "x-ms-blob-type" : "BlockBlob"}
    with open(file_path, 'rb') as file:
        x = requests.put(upload_url, data=file,headers=headers)
        print(x.text)

def file_status_update(asset_id, file_id):
    headers = {"App-ID":cloud_config_info["App-ID"],
            "Auth-Token" : cloud_config_info["Auth-Token"]
            }
    upload_file_status_close = requests.patch(f'{domain}/API/files/v1/assets/{asset_id}/files/{file_id}/', headers=headers, json={"status": "CLOSED", "progress_processed": 100})

def collection_fullpath(collection_id):
    headers = {"App-ID":cloud_config_info["App-ID"],
        "Auth-Token" : cloud_config_info["Auth-Token"]
        }
    params = {"get_upload_path": "true"}
    response = requests.get(f'{domain}/API/assets/v1/collections/{collection_id}/full/path', headers=headers,params=params).json()
    return '' if "errors" in response else response

def get_download_link_files(asset_id,file_id):
    headers = {"App-ID":cloud_config_info["App-ID"],
    "Auth-Token" : cloud_config_info["Auth-Token"]
    }
    response = requests.get(f'{domain}/API/files/v1/assets/{asset_id}/files/{file_id}/', headers=headers).json()

    return response["original_name"],response["url"]

def get_download_link_proxy(asset_id,proxies_id):
    headers = {"App-ID":cloud_config_info["App-ID"],
    "Auth-Token" : cloud_config_info["Auth-Token"]
    }
    response = requests.get(f'{domain}/API/files/v1/assets/{asset_id}/proxies/{proxies_id}/', headers=headers).json()

    return response["name"],response["url"]

def calculate_sha1(file_path):
    sha1 = hashlib.sha1()
    
    with open(file_path, 'rb') as file:
        while chunk := file.read(8192):
            sha1.update(chunk)
    
    return sha1.hexdigest()

def process_collection(collection : list):
    for x in collection:
        collection_id = x["id"]
        y = get_call_of_collections_content(collection_id)
        for obj in y:
            if 'files' in obj:
                file_entry = {
                    "asset_id": obj['id'],
                    "files": obj['files'],
                    "title": obj['title'],
                    "proxies" : obj['proxies'] if 'proxies' in obj else []
                }
                file_ids.append(file_entry)
            else:
                collection_ids.append({"collection_id" : obj['id'],"collectionname" : obj['title']})
                process_collection([obj])

    return file_ids,collection_ids


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
        for file in data['files']:
            mtime_struct = datetime.strptime(file['date_modified'].split(".")[0], "%Y-%m-%dT%H:%M:%S") 
            atime_struct = datetime.strptime(file['date_created'].split(".")[0], "%Y-%m-%dT%H:%M:%S")
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
                file_object["name"] = file['directory_path'] +"/"+ data['title'] + "/" + file['original_name'] 
                file_object["size"] = file['size']
                file_object["mode"] = "0"
                file_object["tmpid"] = f"{data["asset_id"]}|file|{file["id"]}"
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

        for proxies in data['proxies']:
            mtime_struct = datetime.strptime(proxies['date_modified'].split(".")[0], "%Y-%m-%dT%H:%M:%S") 
            atime_struct = datetime.strptime(proxies['date_created'].split(".")[0], "%Y-%m-%dT%H:%M:%S")
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
                file_object["name"] = file['directory_path'] +"/"+ data['title'] + "/" + proxies['name'] 
                file_object["size"] = "10"
                file_object["mode"] = "0"
                file_object["tmpid"] = f"{data["asset_id"]}|proxy|{proxies["id"]}"
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
    # parser.add_argument('-c', '--configname', required = True, help = 'Config name to work with in cloud_targets.conf')
    parser.add_argument('-s','--source',help='source file')
    parser.add_argument('-t','--target',help='target_path')
    parser.add_argument('-f','--foldername',help='folder_name_to_create')
    parser.add_argument('-id','--collection_id',help='collection_id')
    parser.add_argument('-tmp','--tmp_id',help='tmp_id')
    parser.add_argument('-ji', '--jobguid', help = 'Job GUID')
    parser.add_argument('-ft', '--filtertype', required=False, choices=['none', 'include', 'exclude'], help='Filter type')
    parser.add_argument('-ff', '--filterfile', required=False, help='Extension file')
    parser.add_argument('-pf', '--policyfile', required=False, help='Policy file')

    args = parser.parse_args()
    mode = args.mode
    file_path = args.source
    folder_path = args.foldername
    collectionid = args.collection_id
    tmp_id = args.tmp_id
    target_path = args.target

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
    "App-ID": "923d4e2c-54de-11ef-81e0-4e8dd0bedbee",
    "Auth-Token": "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpZCI6ImU5NDM0OWQ2LTU2MWItMTFlZi04ODQxLWFlODQ3Y2M3M2M1NyIsImV4cCI6MjAzODY0NjMzNH0._If9RA3zvBb0sQMziREjXtVkwWwKxTkowbL-q7QI0eU",
    "name": "SDNA_GCS",
    "method": "GCS"
}
    
    if mode == 'list':
        if collectionid:
            collection = [{'id':collectionid}]
        else:
            collection = get_call_of_collections()
        files_list,collections_list = process_collection(collection)
        objects_dict = GetObjectDict(files_list)
        if objects_dict and file_path:
            generate_xml_from_file_objects(objects_dict, file_path)
            print(f"Generated XML file: {file_path}")
        else:
            print("Failed to generate XML file.")
        #os.remove(directory)
        print ("GOOD")


    elif mode == 'upload':
        file_name = get_filename(file_path)
        file_size = os.path.getsize(file_path)
        storage_name = cloud_config_info["name"]
        storage_method = cloud_config_info["method"]
        # collection_id = "e429768c-59f4-11ef-a87c-eeb365145e25"
        collection_id = collectionid


        storage_id = get_storage_id(storage_name,storage_method)
        asset_id, user_id = create_asset_id(file_name,collection_id)
        add_asset_in_collection(asset_id,collection_id)
        upload_path = collection_fullpath(collection_id)
        format_id = create_format_id(asset_id, user_id)
        fileset_id = create_fileset_id(asset_id, format_id, file_name, storage_id,upload_path)

        if storage_method == "GCS":
            upload_url, file_id = get_upload_url(asset_id, file_name, file_size, fileset_id, storage_id, format_id,upload_path)
            upload_file_gcs(upload_url, file_path, file_size)
            file_status_update(asset_id, file_id)
        
        elif storage_method == "S3":
            upload_url, file_id = get_upload_url_s3(asset_id, file_name, file_size, fileset_id, storage_id, format_id,upload_path)
            upload_id = get_upload_id_s3(upload_url)
            part_url = get_part_url_s3(asset_id, file_id, upload_id)
            upload_file_s3(part_url, file_path,upload_id, asset_id, file_id)
            file_status_update(asset_id, file_id)

        elif storage_method == "B2":
            upload_url, file_id,authorizationToken,upload_filename = get_upload_url_b2(asset_id, file_name, file_size, fileset_id, storage_id, format_id,upload_path)
            sha1_of_file  = calculate_sha1(file_path)
            upload_file_b2(upload_url,authorizationToken,file_path,upload_filename,sha1_of_file)
            file_status_update(asset_id, file_id)

        elif storage_method == "AZURE":
            upload_url, file_id = get_upload_url(asset_id, file_name, file_size, fileset_id, storage_id, format_id,upload_path)
            upload_file_azure(upload_url, file_path)
            file_status_update(asset_id, file_id)
        

    elif mode == 'browse':
        folders = []
        # collection_id = "a70d2dca-59f4-11ef-b571-4a8ffd934f12"
        collection_id = collectionid
        if collection_id:
            collections = get_call_of_collections_content(collection_id)
        else:
            collections = get_call_of_collections() 
        for x in collections:
            if 'files' not in x:
                folders.append({"name" : x["title"],
                                "id" : x["id"]
                                })
                
        xml_output = add_CDATA_tags_with_id(folders)
        print(xml_output)
        # generate_xml(file_path,xml_output)
        # print(f"Generated XML file: {file_path}")

    elif mode == "download":
        download_path = os.path.normpath(target_path)
        tmp_id_list = tmp_id.split("|")
        asset_id = tmp_id_list[0]
        file_id = tmp_id_list[-1]
        file_type = tmp_id_list[1]
        if file_type =="file":
            file_name ,url = get_download_link_files(asset_id,file_id)
        elif file_type =="proxy":
            file_name ,url = get_download_link_proxy(asset_id,file_id)

        response = requests.get(url)
        if response.status_code == 200:
            os.makedirs(download_path, exist_ok=True)
            download_path = os.path.join(download_path, file_name)
            with open(download_path, 'wb') as file:
                file.write(response.content)
            print(f"File download at {download_path}")

    elif mode == "createfolder":
        # folder_path = "rushiraj/tej/123/456"
        # collection_id = ["3ded220c-596b-11ef-a848-52f392b714aa"]
        collection_id = []
        collection_id.append(collectionid)
        collection_path = os.path.normpath(folder_path).split("\\")
        for collection_path_parts in collection_path:
            id = create_collection(collection_path_parts,collection_id[-1])
            collection_id.append(id)
        print(collection_id[-1])


