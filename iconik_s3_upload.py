import requests
import json
import os
import xml.etree.ElementTree as ET

cloud_config_info = {
        "App-ID": "923d4e2c-54de-11ef-81e0-4e8dd0bedbee",
        "Auth-Token": "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpZCI6ImU5NDM0OWQ2LTU2MWItMTFlZi04ODQxLWFlODQ3Y2M3M2M1NyIsImV4cCI6MjAzODY0NjMzNH0._If9RA3zvBb0sQMziREjXtVkwWwKxTkowbL-q7QI0eU",
        "name": "SDNA_S3",
        "method": "S3"
 
    }
domain = 'https://app.iconik.io'



def create_storage_id(storage_name,storage_method):

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
    params  = {"apply_default_acls":"false","apply_collection_acls":"true"}
    headers = {"App-ID":cloud_config_info["App-ID"],
               "Auth-Token" : cloud_config_info["Auth-Token"]
               }
    response = requests.post(f'{domain}/API/assets/v1/assets/', headers=headers, json=payload,params=params).json()
    return response['id'], response['created_by_user']

def add_asset_in_collection(asset_id,collection_id):
    payload = {"object_id":asset_id,"object_type":"assets"}
    params = {"index_immediately":"true"}
    headers = {"App-ID":cloud_config_info["App-ID"],
            "Auth-Token" : cloud_config_info["Auth-Token"]
            }
    requests.post(f'{domain}/API/assets/v1/collections/{collection_id}/contents', headers=headers, json=payload,params=params)
    response = requests.get(f'{domain}/API/assets/v1/collections/{collection_id}/full/path', headers=headers)
    return response.text[1:-2]
    
def create_format_id(asset_id, user_id):
    payload = {"user_id": user_id,"name": "ORIGINAL","metadata": [{"internet_media_type": "text/plain"}],"storage_methods": [cloud_config_info["method"]]}
    headers = {"App-ID":cloud_config_info["App-ID"],
               "Auth-Token" : cloud_config_info["Auth-Token"]
               }
    response = requests.post(f'{domain}/API/files/v1/assets/{asset_id}/formats/', headers=headers, json=payload).json()
    return response['id']

def create_fileset_id(asset_id, format_id, file_name, storage_id,upload_path):
    payload = {"format_id": format_id,"storage_id": storage_id,"base_dir": "","name": file_name,"component_ids": []}
    headers = {"App-ID":cloud_config_info["App-ID"],
               "Auth-Token" : cloud_config_info["Auth-Token"]
               }
    response = requests.post(f'{domain}/API/files/v1/assets/{asset_id}/file_sets/', headers=headers, json=payload).json()
    return response['id']


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


def get_upload_id_s3(upload_url):
    response = requests.post(upload_url)
    root = ET.fromstring(response.text)
    namespace = root.tag.split('}')[0] + '}'
    upload_id = root.find(f'{namespace}UploadId').text
    return upload_id


def get_part_url(asset_id, file_id, upload_id):
    part_url = f'{domain}/API/files/v1/assets/{asset_id}/files/{file_id}/multipart_url/part/'
    params = {"parts_num": "1", "upload_id": upload_id, "per_page": "100", "page": "1"}
    headers = {"App-ID":cloud_config_info["App-ID"],
               "Auth-Token" : cloud_config_info["Auth-Token"]
               }
    response = requests.get(part_url, headers=headers, params=params)
    part_data = response.json()
    return part_data["objects"][0]["url"]


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


def file_status_update(asset_id, file_id):
    headers = {"App-ID":cloud_config_info["App-ID"],
               "Auth-Token" : cloud_config_info["Auth-Token"]
               }
    requests.patch(f'{domain}/API/files/v1/assets/{asset_id}/files/{file_id}/', headers=headers, json={"status": "CLOSED", "progress_processed": 100})


def main():
    file_name = 'TEJ-files.xml'
    file_path = 'D:\\storageDNA\\TEJ-files.xml'
    file_size = os.path.getsize(file_path)
    storage_name = cloud_config_info["name"]
    storage_method = cloud_config_info["method"]
    collection_id = "e429768c-59f4-11ef-a87c-eeb365145e25"
    storage_id = create_storage_id(storage_name,storage_method)
    asset_id, user_id = create_asset_id(file_name,collection_id)
    upload_path = add_asset_in_collection(asset_id,collection_id)
    format_id = create_format_id(asset_id, user_id)
    fileset_id = create_fileset_id(asset_id, format_id, file_name, storage_id,upload_path)
    upload_url, file_id = get_upload_url(asset_id, file_name, file_size, fileset_id, storage_id, format_id,upload_path)
    upload_id = get_upload_id(upload_url)
    part_url = get_part_url(asset_id, file_id, upload_id)
    etag = upload_file(part_url, file_path)
    complete_upload(etag, upload_id, asset_id, file_id)
    file_status_update(asset_id, file_id)


if __name__ == "__main__":
    main()
