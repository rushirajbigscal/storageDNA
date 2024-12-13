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

file_ids = []
collection_ids = []
domain = 'https://app.iconik.io'

def strdata_to_logging_file(str_data, filename):
    f = open(filename, "a")
    f.write(f'{str_data}\n')
    f.close()

def get_call_of_collections():
    url = f"{domain}/API/assets/v1/collections/"
    headers = {"app-id":params_map["app-id"],
            "auth-token" : params_map["auth-token"]
            }
    response = requests.get(f"{domain}/API/assets/v1/collections/", headers=headers)
    if response.status_code != 200:
        if logging_dict["logging_level"] > 0:
            strdata_to_logging_file(url, logging_dict["logging_error_filename"])       
        print(f"Response error. Status - {response.status_code}, Error - {response.text}")
        return False
    elif logging_dict["logging_level"] > 1:
        strdata_to_logging_file(url, logging_dict["logging_filename"])
    response = response.json()
    for collection_id in response['objects']:
        collection_ids.append({"collection_id" : collection_id['id'],"collectionname" : collection_id['title']}) 
    return response['objects']

def get_call_of_collections_content(collection_id):
    url = f"{domain}/API/assets/v1/collections/{collection_id}/contents"
    headers = {"app-id":params_map["app-id"],
            "auth-token" : params_map["auth-token"]
            }
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        if logging_dict["logging_level"] > 0:
            strdata_to_logging_file(url, logging_dict["logging_error_filename"])
        print(f"Response error. Status - {response.status_code}, Error - {response.text}")
        return False
    elif logging_dict["logging_level"] > 1:
        strdata_to_logging_file(url, logging_dict["logging_filename"])
    response = response.json()
    return response['objects']

def get_storage_id(storage_name,storage_method):
    url = f'{domain}/API/files/v1/storages/'
    headers = {"app-id":params_map["app-id"],
            "auth-token" : params_map["auth-token"]
            }
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        if logging_dict["logging_level"] > 0:
            strdata_to_logging_file(url, logging_dict["logging_error_filename"])
        print(f"Response error. Status - {response.status_code}, Error - {response.text}")
        return False
    elif logging_dict["logging_level"] > 1:
        strdata_to_logging_file(url, logging_dict["logging_filename"])
    response = response.json()
    storage_id = []
    for storage in response["objects"]:
        if storage["name"] == storage_name and storage["method"] == storage_method:
            storage_id.append(storage["id"])
    return storage_id[0]

def create_asset_id(file_name,collection_id):
    url = f'{domain}/API/assets/v1/assets/'
    payload = {"title": file_name,"type":"ASSET","collection_id":collection_id}
    payload_default = {"title": file_name,"type":"ASSET"}
    params  = {"apply_default_acls":"false","apply_collection_acls":"true"}
    params_default  = {"apply_default_acls":"true"}

    headers = {"app-id":params_map["app-id"],
               "auth-token" : params_map["auth-token"]
               }
    response = requests.post(
        url,
        headers=headers,
        json=payload if collection_id else payload_default,
        params=params if collection_id else params_default
    )
    if response.status_code != 201:
        if logging_dict["logging_level"] > 0:
            strdata_to_logging_file(url, logging_dict["logging_error_filename"])
        print(f"Response error. Status - {response.status_code}, Error - {response.text}")
        return False
    elif logging_dict["logging_level"] > 1:
        strdata_to_logging_file(url, logging_dict["logging_filename"])
    response = response.json()
    return response['id'], response['created_by_user']


def create_collection(collection_name,collection_id):
    url = f'{domain}/API/assets/v1/collections/'
    payload = {"title": collection_name,"parent_id":collection_id}
    payload_default = {"title": collection_name}

    params  = {"apply_default_acls":"true","restrict_collection_acls":"true"}

    headers = {"app-id":params_map["app-id"],
               "auth-token" : params_map["auth-token"]
               }
    response = requests.post(
        url, 
        headers=headers, 
        json=payload if collection_id else payload_default,
        params=params if collection_id else None
    )
    if response.status_code != 201:
        if logging_dict["logging_level"] > 0:
            strdata_to_logging_file(url, logging_dict["logging_error_filename"])
        print(f"Response error. Status - {response.status_code}, Error - {response.text}")
        return False
    elif logging_dict["logging_level"] > 1:
        strdata_to_logging_file(url, logging_dict["logging_filename"])
    response = response.json()
    return response['id']


def get_filename_from_asset(asset_id):
    url = f'{domain}/API/assets/v1/assets/{asset_id}/'
    headers = {"app-id":params_map["app-id"],
            "auth-token" : params_map["auth-token"]
            }
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        if logging_dict["logging_level"] > 0:
            strdata_to_logging_file(url, logging_dict["logging_error_filename"])
        print(f"Response error. Status - {response.status_code}, Error - {response.text}")
        return False
    elif logging_dict["logging_level"] > 1:
        strdata_to_logging_file(url, logging_dict["logging_filename"])
    response = response.json()
    return response["title"]

def add_asset_in_collection(asset_id,collection_id):
    url = f'{domain}/API/assets/v1/collections/{collection_id}/contents'
    payload = {"object_id":asset_id,"object_type":"assets"}
    params = {"index_immediately":"true"}
    headers = {"app-id":params_map["app-id"],
            "auth-token" : params_map["auth-token"]
            }
    response = requests.post(url, headers=headers, json=payload,params=params)
    if response.status_code != 201:
        if logging_dict["logging_level"] > 0:
            strdata_to_logging_file(url, logging_dict["logging_error_filename"])
        print(f"Response error. Status - {response.status_code}, Error - {response.text}")
        return False
    elif logging_dict["logging_level"] > 1:
        strdata_to_logging_file(url, logging_dict["logging_filename"])
    return True
    
def create_format_id(asset_id, user_id):
    url = f'{domain}/API/files/v1/assets/{asset_id}/formats/'
    payload = {"user_id": user_id,"name": "ORIGINAL","metadata": [{"internet_media_type": "text/plain"}],"storage_methods": [params_map["method"]]}
    headers = {"app-id":params_map["app-id"],
               "auth-token" : params_map["auth-token"]
               }
    response = requests.post(url, headers=headers, json=payload)
    if response.status_code != 201:
        if logging_dict["logging_level"] > 0:
            strdata_to_logging_file(url, logging_dict["logging_error_filename"])
        print(f"Response error. Status - {response.status_code}, Error - {response.text}")
        return False
    elif logging_dict["logging_level"] > 1:
        strdata_to_logging_file(url, logging_dict["logging_filename"])
    response = response.json()
    return response['id']

def create_fileset_id(asset_id, format_id, file_name, storage_id,upload_path):
    url = f'{domain}/API/files/v1/assets/{asset_id}/file_sets/'
    payload = {"format_id": format_id,"storage_id": storage_id,"base_dir": upload_path,"name": file_name,"component_ids": []}
    headers = {"app-id":params_map["app-id"],
               "auth-token" : params_map["auth-token"]
               }
    response = requests.post(url, headers=headers, json=payload)
    if response.status_code != 201:
        if logging_dict["logging_level"] > 0:
            strdata_to_logging_file(url, logging_dict["logging_error_filename"])
        print(f"Response error. Status - {response.status_code}, Error - {response.text}")
        return False
    elif logging_dict["logging_level"] > 1:
        strdata_to_logging_file(url, logging_dict["logging_filename"])
    response = response.json()
    return response['id']


def get_upload_url(asset_id, file_name, file_size, fileset_id, storage_id, format_id,upload_path):
    url = f'{domain}/API/files/v1/assets/{asset_id}/files/'
    file_info = {
        'original_name': file_name,
        'directory_path': upload_path,
        'size': file_size,
        'type': 'FILE',
        'storage_id': storage_id,
        'file_set_id': fileset_id,
        'format_id': format_id
    }
    headers = {"app-id":params_map["app-id"],
               "auth-token" : params_map["auth-token"]
               }
    response = requests.post(url, headers=headers, json=file_info)
    if response.status_code != 201:
        if logging_dict["logging_level"] > 0:
            strdata_to_logging_file(url, logging_dict["logging_error_filename"])
        print(f"Response error. Status - {response.status_code}, Error - {response.text}")
        return False
    elif logging_dict["logging_level"] > 1:
        strdata_to_logging_file(url, logging_dict["logging_filename"])
    response = response.json()
    return response['upload_url'], response['id']

def get_upload_url_s3(asset_id, file_name, file_size, fileset_id, storage_id, format_id,upload_path):
    url = f'{domain}/API/files/v1/assets/{asset_id}/files/'
    file_info = {
        'original_name': file_name,
        'directory_path': upload_path,
        'size': file_size,
        'type': 'FILE',
        'storage_id': storage_id,
        'file_set_id': fileset_id,
        'format_id': format_id
    }
    headers = {"app-id":params_map["app-id"],
               "auth-token" : params_map["auth-token"]
               }
    response = requests.post(url, headers=headers, json=file_info)
    if response.status_code != 201:
        if logging_dict["logging_level"] > 0:
            strdata_to_logging_file(url, logging_dict["logging_error_filename"])
        print(f"Response error. Status - {response.status_code}, Error - {response.text}")
        return False
    elif logging_dict["logging_level"] > 1:
        strdata_to_logging_file(url, logging_dict["logging_filename"])
    response = response.json()
    return response['multipart_upload_url'], response['id']

def get_upload_url_b2(asset_id, file_name, file_size, fileset_id, storage_id, format_id,upload_path):
    url = f'{domain}/API/files/v1/assets/{asset_id}/files/'
    file_info = {
        'original_name': file_name,
        'directory_path': upload_path,
        'size': file_size,
        'type': 'FILE',
        'storage_id': storage_id,
        'file_set_id': fileset_id,
        'format_id': format_id
    }
    headers = {"app-id":params_map["app-id"],
               "auth-token" : params_map["auth-token"]
               }
    response = requests.post(url, headers=headers, json=file_info)
    if response.status_code != 201:
        if logging_dict["logging_level"] > 0:
            strdata_to_logging_file(url, logging_dict["logging_error_filename"])
        print(f"Response error. Status - {response.status_code}, Error - {response.text}")
        return False
    elif logging_dict["logging_level"] > 1:
        strdata_to_logging_file(url, logging_dict["logging_filename"])
    response = response.json()
    return response['upload_url'], response['id'],response["upload_credentials"]["authorizationToken"],response["upload_filename"]


def get_upload_id_s3(upload_url):
    response = requests.post(upload_url)
    if response.status_code != 200:
        if logging_dict["logging_level"] > 0:
            strdata_to_logging_file(upload_url, logging_dict["logging_error_filename"])
        print(f"Response error. Status - {response.status_code}, Error - {response.text}")
        return False
    elif logging_dict["logging_level"] > 1:
        strdata_to_logging_file(upload_url, logging_dict["logging_filename"])
    root = ET.fromstring(response.text)
    namespace = root.tag.split('}')[0] + '}'
    upload_id = root.find(f'{namespace}UploadId').text
    return upload_id

def get_part_url_s3(asset_id, file_id, upload_id):
    part_url = f'{domain}/API/files/v1/assets/{asset_id}/files/{file_id}/multipart_url/part/'
    params = {"parts_num": "1", "upload_id": upload_id, "per_page": "100", "page": "1"}
    headers = {"app-id":params_map["app-id"],
               "auth-token" : params_map["auth-token"]
               }
    response = requests.get(part_url, headers=headers, params=params)
    if response.status_code != 200:
        if logging_dict["logging_level"] > 0:
            strdata_to_logging_file(part_url, logging_dict["logging_error_filename"])
        print(f"Response error. Status - {response.status_code}, Error - {response.text}")
        return False
    elif logging_dict["logging_level"] > 1:
        strdata_to_logging_file(url, logging_dict["logging_filename"])
    response = response.json()
    return response["objects"][0]["url"]

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
    if resp_JSON.status_code != 201:
        if logging_dict["logging_level"] > 0:
            strdata_to_logging_file(upload_url, logging_dict["logging_error_filename"])
        print(f"Response error. Status - {upload_url.status_code}, Error - {upload_url.text}")
        return False
    elif logging_dict["logging_level"] > 1:
        strdata_to_logging_file(upload_url, logging_dict["logging_filename"])
    upload_id = resp_JSON.headers['X-GUploader-Uploadid']

    google_headers = {
        'content-length': str(file_size),
        'x-goog-resumable': 'upload'
    }

    with open(file_path, 'rb') as f:
        full_upload_url = f"{upload_url}&upload_id={upload_id}"
        x = requests.put(full_upload_url, headers=google_headers, data=f)
        if x.status_code != 200:
            if logging_dict["logging_level"] > 0:
                strdata_to_logging_file(full_upload_url, logging_dict["logging_error_filename"])
            print(f"Response error. Status - {x.status_code}, Error - {x.text}")
            return False
        elif logging_dict["logging_level"] > 1:
            strdata_to_logging_file(full_upload_url, logging_dict["logging_filename"])
        return x

def upload_file_s3(part_url, file_path,upload_id, asset_id, file_id):
    with open(file_path, 'rb') as file:
        response = requests.put(part_url, data=file)
        if response.status_code != 200:
            if logging_dict["logging_level"] > 0:
                strdata_to_logging_file(part_url, logging_dict["logging_error_filename"])
            print(f"Response error. Status - {response.status_code}, Error - {response.text}")
            return False
        elif logging_dict["logging_level"] > 1:
            strdata_to_logging_file(part_url, logging_dict["logging_filename"])
        etag = response.headers['etag']
        multipart_url = f'{domain}/API/files/v1/assets/{asset_id}/files/{file_id}/multipart_url/'
    complete_url_response = requests.get(
        multipart_url,
        headers={"app-id":params_map["app-id"],
               "auth-token" : params_map["auth-token"]
               },
        params={"upload_id": upload_id, "type": "complete_url"}
    )
    if complete_url_response.status_code != 200:
        if logging_dict["logging_level"] > 0:
            strdata_to_logging_file(multipart_url, logging_dict["logging_error_filename"])
        print(f"Response error. Status - {response.status_code}, Error - {response.text}")
        return False
    elif logging_dict["logging_level"] > 1:
            strdata_to_logging_file(multipart_url, logging_dict["logging_filename"])
    complete_url_response =complete_url_response.json()
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
    if response.status_code != 200:
        if logging_dict["logging_level"] > 0:
            strdata_to_logging_file(complete_url, logging_dict["logging_error_filename"])
        print(f"Response error. Status - {response.status_code}, Error - {response.text}")
        return False
    elif logging_dict["logging_level"] > 1:
            strdata_to_logging_file(complete_url, logging_dict["logging_filename"])
    return response

def upload_file_b2(upload_url,authorizationToken,file_path,upload_filename,sha1_of_file):
    headers = {
               "Authorization" : authorizationToken,
               "X-Bz-File-Name" : upload_filename,
               "X-Bz-Content-Sha1" : sha1_of_file,
               "Content-Type" : "b2/x-auto"
               }
    
    with open(file_path, 'rb') as f:
        x = requests.post(upload_url,headers=headers,data=f)
        if x.status_code != 200:
            if logging_dict["logging_level"] > 0:
                strdata_to_logging_file(part_url, logging_dict["logging_error_filename"])
            print(f"Response error. Status - {x.status_code}, Error - {x.text}")
            return False
        elif logging_dict["logging_level"] > 1:
            strdata_to_logging_file(url, logging_dict["logging_filename"])
        return x

def upload_file_azure(upload_url, file_path):
    headers = { "x-ms-blob-type" : "BlockBlob"}
    with open(file_path, 'rb') as file:
        x = requests.put(upload_url, data=file,headers=headers)
        if x.status_code != 200:
            if logging_dict["logging_level"] > 0:
                strdata_to_logging_file(part_url, logging_dict["logging_error_filename"])
            print(f"Response error. Status - {x.status_code}, Error - {x.text}")
            return False
        elif logging_dict["logging_level"] > 1:
            strdata_to_logging_file(url, logging_dict["logging_filename"])
        return x

def file_status_update(asset_id, file_id):
    url = f'{domain}/API/files/v1/assets/{asset_id}/files/{file_id}/'
    headers = {"app-id":params_map["app-id"],
            "auth-token" : params_map["auth-token"]
            }
    upload_file_status_close = requests.patch(url, headers=headers, json={"status": "CLOSED", "progress_processed": 100})
    if upload_file_status_close.status_code != 200:
        if logging_dict["logging_level"] > 0:
            strdata_to_logging_file(url, logging_dict["logging_error_filename"])
        print(f"Response error. Status - {response.status_code}, Error - {response.text}")
        return False
    elif logging_dict["logging_level"] > 1:
        strdata_to_logging_file(url, logging_dict["logging_filename"])
    return upload_file_status_close

def collection_fullpath(collection_id):
    url = f'{domain}/API/assets/v1/collections/{collection_id}/full/path'
    headers = {"app-id":params_map["app-id"],
        "auth-token" : params_map["auth-token"]
        }
    params = {"get_upload_path": "true"}
    response = requests.get(url, headers=headers,params=params)
    if response.status_code != 200:
        if logging_dict["logging_level"] > 0:
            strdata_to_logging_file(url, logging_dict["logging_error_filename"])
        print(f"Response error. Status - {response.status_code}, Error - {response.text}")
        return False
    elif logging_dict["logging_level"] > 1:
        strdata_to_logging_file(url, logging_dict["logging_filename"])
    response = response.json()
    return '' if "errors" in response else response

def get_download_link_files(asset_id,file_id):
    url = f'{domain}/API/files/v1/assets/{asset_id}/files/{file_id}/'
    headers = {"app-id":params_map["app-id"],
    "auth-token" : params_map["auth-token"]
    }
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        if logging_dict["logging_level"] > 0:
            strdata_to_logging_file(url, logging_dict["logging_error_filename"])
        print(f"Response error. Status - {response.status_code}, Error - {response.text}")
        return False
    elif logging_dict["logging_level"] > 1:
        strdata_to_logging_file(url, logging_dict["logging_filename"])
    response = response.json()
    return response["original_name"],response["url"]

def get_download_link_proxy(asset_id,proxies_id):
    url = f'{domain}/API/files/v1/assets/{asset_id}/proxies/{proxies_id}/'
    headers = {"app-id":params_map["app-id"],
    "auth-token" : params_map["auth-token"]
    }
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        if logging_dict["logging_level"] > 0:
            strdata_to_logging_file(url, logging_dict["logging_error_filename"])
        print(f"Response error. Status - {response.status_code}, Error - {response.text}")
        return False
    elif logging_dict["logging_level"] > 1:
        strdata_to_logging_file(url, logging_dict["logging_filename"])
    response = response.json()
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
        for file in data['files']:
            mtime_struct = datetime.strptime(file['date_modified'].split(".")[0], "%Y-%m-%dT%H:%M:%S") 
            atime_struct = datetime.strptime(file['date_created'].split(".")[0], "%Y-%m-%dT%H:%M:%S")
            mtime_epoch_seconds = int(mtime_struct.timestamp())
            atime_epoch_seconds = int(atime_struct.timestamp())
            file_path = file['directory_path'] +"/"+ data['title'] + "/" + file['original_name'] 
            file_name = get_filename(file_path)
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
                local_asset_id = data["asset_id"]
                local_file_id = file["id"]
                file_object["name"] = file_path
                file_object["size"] = file_size
                file_object["mode"] = "0"
                file_object["tmpid"] = f"{local_asset_id}|file|{local_file_id}"
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

        for proxies in data['proxies']:
            mtime_struct = datetime.strptime(proxies['date_modified'].split(".")[0], "%Y-%m-%dT%H:%M:%S") 
            atime_struct = datetime.strptime(proxies['date_created'].split(".")[0], "%Y-%m-%dT%H:%M:%S")
            mtime_epoch_seconds = int(mtime_struct.timestamp())
            atime_epoch_seconds = int(atime_struct.timestamp())
            file_path = file['directory_path'] +"/"+ data['title'] + "/" + proxies['name'] 
            file_name = get_filename(file_path)
            file_size = "10"
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
                local_asset_id = data["asset_id"]
                local_proxy_id = proxies["id"]
                file_object["name"] = file_path
                file_object["size"] = file_size
                file_object["mode"] = "0"
                file_object["tmpid"] = f"{local_asset_id}|proxy|{local_proxy_id}"
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
    parser.add_argument('-id','--collection_id',help='collection_id')
    parser.add_argument('-tmp','--tmp_id',help='tmp_id')
    parser.add_argument('-in', '--indexid', required=False, help = 'REQUIRED if list')
    parser.add_argument('-jg', '--jobguid', required=False, help = 'REQUIRED if list')
    parser.add_argument('-ji', '--jobid', required=False, help = 'REQUIRED if bulk restore.')
    parser.add_argument('-p', "--projectname", required=False, help = 'Project name')

    args = parser.parse_args()
    mode = args.mode
    file_path = args.source
    folder_path = args.foldername
    collectionid = args.collection_id
    tmp_id = args.tmp_id
    target_path = args.target

    # if collectionid is None and mode == 'list':
    #     if file_path is None or len(file_path) == 0:
    #         print("Error collection id or path required.")
    #         sys.exit(1)
    #     path_parts = file_path.split("/")
    #     path_parts = list(filter(None, path_parts))
    #     print(path_parts)
    #     last_path_part = path_parts[-1]
    #     collectionid = last_path_part.split(":")[0]

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

    if mode == 'list':
        if target_path is None:
            print('Target path (-t <targetpath> ) -in <index> options are required for list')
            exit(1)
        if collectionid:
            collection = [{'id':collectionid}]
        else:
            collection = get_call_of_collections()
        files_list,collections_list = process_collection(collection)
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


    elif mode == 'upload':
        if collectionid is None or file_path is None:
            print(f'Collection id (-id <collection_id> ) and file path (-s <source> ) options is required for upload')
            exit(1)
        file_name = get_filename(file_path)
        file_size = os.path.getsize(file_path)
        storage_name = params_map["name"]
        storage_method = params_map["method"]

        if storage_method and storage_name:
            collection_id = collectionid
            storage_id = get_storage_id(storage_name,storage_method)
            asset_id, user_id = create_asset_id(file_name,collection_id)
            add_asset_in_collection(asset_id,collection_id)
            upload_path = collection_fullpath(collection_id)
            format_id = create_format_id(asset_id, user_id)
            fileset_id = create_fileset_id(asset_id, format_id, file_name, storage_id,upload_path)
        else:
            print("Storage Name and Method is requird.")
            exit(1)

        if storage_method == "GCS":
            upload_url, file_id = get_upload_url(asset_id, file_name, file_size, fileset_id, storage_id, format_id,upload_path)
            upload_code = upload_file_gcs(upload_url, file_path, file_size)
            if upload_code.status_code == 200:
                response = file_status_update(asset_id, file_id)
                if response.status_code != 200:
                    print(f"Response error. Status - {response.status_code}, Error - {response.text}")
                    exit(1)
                print("File Upload Succesfully")
                exit(0)
            else:
                print(f"Response error. Status - {upload_code.status_code}, Error - {upload_code.text}")
                exit(1)
        
        elif storage_method == "S3":
            upload_url, file_id = get_upload_url_s3(asset_id, file_name, file_size, fileset_id, storage_id, format_id,upload_path)
            upload_id = get_upload_id_s3(upload_url)
            part_url = get_part_url_s3(asset_id, file_id, upload_id)
            upload_code = upload_file_s3(part_url, file_path,upload_id, asset_id, file_id)
            if upload_code.status_code == 200:
                response = file_status_update(asset_id, file_id)
                if response.status_code != 200:
                    print(f"Response error. Status - {response.status_code}, Error - {response.text}")
                    exit(1)
                print("File Upload Succesfully")
                exit(0)
            else:
                print(f"Response error. Status - {upload_code.status_code}, Error - {upload_code.text}")
                exit(1)

        elif storage_method == "B2":
            upload_url, file_id,authorizationToken,upload_filename = get_upload_url_b2(asset_id, file_name, file_size, fileset_id, storage_id, format_id,upload_path)
            sha1_of_file  = calculate_sha1(file_path)
            upload_code = upload_file_b2(upload_url,authorizationToken,file_path,upload_filename,sha1_of_file)
            if upload_code.status_code == 200:
                response = file_status_update(asset_id, file_id)
                if response.status_code != 200:
                    print(f"Response error. Status - {response.status_code}, Error - {response.text}")
                    exit(1)
                print("File Upload Succesfully")
                exit(0)
            else:
                print(f"Response error. Status - {upload_code.status_code}, Error - {upload_code.text}")
                exit(1)

        elif storage_method == "AZURE":
            upload_url, file_id = get_upload_url(asset_id, file_name, file_size, fileset_id, storage_id, format_id,upload_path)
            upload_code = upload_file_azure(upload_url, file_path)
            if upload_code.status_code == 200:
                response = file_status_update(asset_id, file_id)
                if response.status_code != 200:
                    print(f"Response error. Status - {response.status_code}, Error - {response.text}")
                    exit(1)
                print("File Upload Succesfully")
                exit(0)
            else:
                print(f"Response error. Status - {upload_code.status_code}, Error - {upload_code.text}")
                exit(1)
        else:
            exit(1)
        

    elif mode == 'buckets':
        buckets = []
        collections = get_call_of_collections()
        for x in collections:
            if 'files' not in x:
                buckets.append(f"{x['id']}:{x['title']}")
        print(','.join(buckets))
        exit(0)

    elif mode == 'bucketsfolders':
        folders = []
        collections = get_call_of_collections() 
        for x in collections:
            if 'files' not in x:
                folders.append({"name" : x["title"],
                                "id" : x["id"]
                                })
                
        xml_output = add_CDATA_tags_with_id(folders)
        print(xml_output)
        exit(0)

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
                folder_name = f'{x["id"]}:{x["title"]}'
                folders.append({"name" : folder_name,
                                "id" : x["id"]
                                })
                
        xml_output = add_CDATA_tags_with_id(folders)
        print(xml_output)
        exit(0)


    elif mode == "download":
        if target_path is None or tmp_id is None:
            print('Target path (-t <targetpath> ) and Tmp id (-tmp <tmp_id>) options are required for download')
            exit(1)
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
            exit(0)
        else:
            exit(1)

    elif mode == "createfolder":
        # folder_path = "rushiraj/tej/123/456"
        # collection_id = ["3ded220c-596b-11ef-a848-52f392b714aa"]
        if folder_path is None:
            print(f'Folder path (-f <folderpath> ) option is required for upload')
            exit(1)
        collection_id = []
        collection_id.append(collectionid)
        collection_path = os.path.normpath(folder_path).split("\\")
        for collection_path_parts in collection_path:
            id = create_collection(collection_path_parts,collection_id[-1])
            collection_id.append(id)
        print(collection_id[-1])

    else:
        print(f'Unsupported mode {mode}')
        exit(1)