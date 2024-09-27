import requests
import argparse
from json import dumps, loads
import sys
import logging
import xml.etree.ElementTree as ET

'''
To get this we got to login to ICONIK, click Admin, then on the left toolbar click the Settings Bubble, 
and then click the Application Tokens button.
'''
DEFAULT_HEADER = None

def load_configurations(file_name):
    json_str = ''
    with open(file_name) as f:
        for line in f:
            json_str += line
    
    config_info = loads(json_str)
    return config_info
    
'''
This method is meant to extract a filename from a full path that is given as a parameter
Note: Works with windows paths as well.
'''
def extract_file_name(file_path):
    if '/' in file_path: 
        return file_path[ len(file_path) - file_path[::-1].index('/') : ]
    elif '\\' in file_path:
        return file_path[ len(file_path) - file_path[::-1].index('\\') : ]
    else:
        return file_path

'''
This is the intial call that we make in order to create amn asset object that we can then work with
'''
def post_asset(file_name):
    url = 'https://app.iconik.io/API/assets/v1/assets/'
    data = {
        "analyze_status": "N/A",
        "archive_status": "NOT_ARCHIVED",
        "external_id": "string",
        "is_online": False,
        "status": "ACTIVE",
        "title": file_name,
        "type": "ASSET"
    }

    response = requests.post(url, json = data, headers = DEFAULT_HEADER).json()

    logging.debug(dumps(response, indent = 4))

    return response

def get_group_and_user_id():
    url = 'https://app.iconik.io/API/users/v1/users/'
    
    try:
        response = requests.get(url, headers = DEFAULT_HEADER).json()
        logging.info(f'\n{dumps(response, indent = 4)}\n')
        for obj in response['objects']:
            if obj['is_admin']:
                return (obj['id'], obj['groups'][0])

    except ValueError:
        print("Failed to get the group and user id")
        exit(1)

    except KeyError:
        print("The response did not yield an object attribute")
        exit(2)

    except IndexError:
        print("Ran into an index error")
        exit(3)


'''
This changes the access control list to give the user_id and the group_id access to the asset
'''
def put_asset(asset_id, user_ids, group_ids):
    print("********this is not working******************")
    url = 'https://app.iconik.io/API/acls/v1/acl/assets/'
    
    data = {
        'group_ids' : group_ids,
        'object_keys' : [
            asset_id
        ],
            'permissions' : [
            'read',
            'write'
        ],
        'user_ids' : user_ids
    }

    try:
        requests.put(url, json = data, headers = DEFAULT_HEADER)
    
    except ValueError:
        print("Failed to get the gorup and user id")
        exit(1)

    except KeyError:
        print("The responce did not yield an object attribute")
        exit(2)
        
    except IndexError:
        print("Ran into an index error")
        exit(3)

'''
Post the proxy to ICONIK, creates a spot for it
'''
def post_proxy(asset_id, file_name):
    url = f'https://app.iconik.io/API/files/v1/assets/{asset_id}/proxies/'

    data = {
        "asset_id": asset_id,
        "bit_rate": 3000000,
        "codec": "H.264, AAC",
        "filename": file_name,
        "format": "MPEG-4",
        "frame_rate": "23.98",
        "is_drop_frame": True,
        "name": file_name,
        "resolution": {
            "height": 360,
            "width": 640
        },
        "start_time_code": None,
        "status": "AWAITED",
        "storage_id": "bf7f13c8-cead-11e9-87fb-0a580a3c0eb1"
    }

    logging.debug(f'This is my data: {dumps(data, indent = 4)}')

    try:
        response = requests.post(url, headers = DEFAULT_HEADER, json = data).json()
        return (response['upload_url'], response['id'])
    
    except ValueError:
        print("Failed to get the gorup and user id")
        exit(1)

    except KeyError:
        print("The responce did not yield an object attribute")
        exit(2)
        
    except IndexError:
        print("Ran into an index error")
        exit(3)
'''
Uploads the file into ICONIK via the Google uploading API
'''
def upload_proxy_file(file_name, upload_link):
    print(f'This is the upload link: {upload_link}')
    google_header = {
        'x-goog-resumable' : 'start',
        'Content-Length' : '0'
    }

    response_headers = requests.post(upload_link, headers = google_header).headers
    new_upload_url = response_headers['Location']

    print(f'This is the new upload link {new_upload_url}')

    google_header.__delitem__('Content-Length')
    new_response = requests.put(new_upload_url, headers = google_header, files = {'upload_file' : open(file_name, 'rb').read()})

    print(f'This is the response of the proxy file upload {new_response}')

'''
After uploading the file a few calls need to be made in order to close of the asset
'''
def perform_post_upload_patches(asset_id, proxy_id):
    proxy_patch_url = f'https://app.iconik.io/API/files/v1/assets/{asset_id}/proxies/{proxy_id}/'
    asset_patch_url = f'https://app.iconik.io/API/assets/v1/assets/{asset_id}/'

    proxy_patch_body = {
        'status' : 'CLOSED'
    }

    asset_patch_body = {
        'type' : 'ASSET'
    }

    print(requests.patch(proxy_patch_url, data = dumps(proxy_patch_body), headers = DEFAULT_HEADER))
    print(requests.patch(asset_patch_url, data = dumps(asset_patch_body), headers = DEFAULT_HEADER))

def extract_missing_fields(fields, mapped_values):
    lookup = set(mapped_values)
    current_fields = {field['label'] for field in fields}

    missing_fields = lookup - current_fields

    logging.debug(f'These are the lookup values : {lookup}, current_fields : {current_fields}, and the missing_fields : {missing_fields}')

    return missing_fields

def add_fields_to_view(fields_to_add, view_name, view_id):
    fields_to_add_set = set(fields_to_add)

    get_fields_url = f'https://app.iconik.io/API/metadata/v1/fields/'
    fields = requests.get(get_fields_url, headers = DEFAULT_HEADER).json()

    logging.debug(f'This is the fields : {dumps(fields, indent = 4)}')

    view_adds = {x['label'] for x in fields['objects'] if x['label'] in fields_to_add_set}

    field_adds = fields_to_add_set - view_adds
    logging.debug(f'This is the field_adds: {field_adds}')

    for f in field_adds:
        field_bod = {
            "field_type" : "string",
            "label": f,
            "name" : f,
            "options": [
                {
                    "label": f,
                    "value": "string"
                }
            ]
        }
        logging.info(f'This is the field_bod : {dumps(field_bod, indent = 4)}')
        requests.post(get_fields_url, data = dumps(field_bod), headers = DEFAULT_HEADER)
        view_adds.add(f)
    
    view_add_list = [{'name' : v} for v in view_adds]
    logging.debug(f'This is the view_add_list : {view_add_list}')

    put_view_url = f'https://app.iconik.io/API/metadata/v1/views/{view_id}/'
    put_bod = {
        'name' : view_name,
        'view_fields' : view_add_list
    }

    requests.put(put_view_url, data = dumps(put_bod), headers = DEFAULT_HEADER)

def perform_preparatory_metadata_steps(mapping, view_id):
    get_fields_for_view_url = f'https://app.iconik.io/API/metadata/v1/views/{view_id}/'
    view_info = requests.get(get_fields_for_view_url, headers = DEFAULT_HEADER).json()

    logging.debug(f'This is the view information {dumps(view_info, indent = 4)}')
    fields = view_info['view_fields']

    missing_fields = extract_missing_fields(fields , mapping.values())
    logging.info(f'Here are the missing fields: {missing_fields}')

    add_fields_to_view(missing_fields, view_info['name'], view_id)
    
    return view_info['name']

def construct_metadata_body_from_file(file_name, mappings):
    name_dict = mappings

    return_body = {'metadata_values' : {} }

    tree = ET.parse(file_name)
    root = tree.getroot()

    for child in root:
        
        #Meant for the file xml file. 
        if child.tag != 'actions' and child.tag in name_dict.keys():
            return_body['metadata_values'][name_dict[child.tag]] = {
                'field_values' : [
                    {
                        'value' : child.text
                    }
                ]
            }
        
        #Case to handle a clip xml file 
        elif child.tag == 'meta-data':
            for data in child.iter():
                if 'name' in data.attrib and data.attrib['name'] in name_dict.keys():
                    return_body['metadata_values'][name_dict[data.attrib['name']]] = {
                        'field_values' : [
                            {
                                'value' : data.text
                            }
                        ]
                    }
    
    logging.info(dumps(return_body, indent = 4))
    return dumps(return_body)

'''
Adds a metadata value to the asset
'''
def perform_metadata_addition(asset_id, view_id, data):
    
    put_url = f'https://app.iconik.io/API/metadata/v1/assets/{asset_id}/assets/{asset_id}/views/{view_id}/'

    logging.debug(f'This is the metadata addition url {put_url}')

    put_body = data

    print(requests.put(put_url, data = put_body, headers = DEFAULT_HEADER).json())

def move_asset_to_collection(asset_id, collection_id):
    post_url = f'https://app.iconik.io/API/assets/v1/collections/{collection_id}/contents'
    
    logging.debug(f'This is the post url for moving the asset into a collection: {post_url}')
    
    post_body = {
        "collection_id" : collection_id,
        "object_id" : asset_id,
        "object_type" : "assets"
    }
    print(requests.post(post_url, data = dumps(post_body), headers = DEFAULT_HEADER).json())

if __name__ == '__main__':
    logging.basicConfig(level = logging.DEBUG)
    parser = argparse.ArgumentParser()

    parser.add_argument('-p', '--proxy', help = 'path to the file that you want to upload')
    parser.add_argument('-f', '--file', required = ('-m' not in sys.argv), help = 'path to the file to add')
    parser.add_argument('-m', '--metadata', required = ('-f' not in sys.argv), help = 'path to the metadata to add')
    args = parser.parse_args()

    logging.debug(f'Here are the args that we have : {args}')

    logging.info('Loading up config...')
    
    config = load_configurations('./iconik.json')

    logging.info(dumps(config, indent = 4))

    DEFAULT_HEADER = {
        'Auth-Token' : config['AUTH_TOKEN'],
        'App-ID' : config['APP_ID']
    }

    view_name = perform_preparatory_metadata_steps(config['CLIP_MAPPINGS'][0]['MAPPINGS'], config['CLIP_MAPPINGS'][0]['VIEW_ID'])

    file_name = ''

    if args.file != None:
        file_name = extract_file_name(args.file)
    elif args.metadata != None:
        file_name = extract_file_name(args.metadata)
    
    logging.debug(file_name)

    logging.info('Running...')
    
    logging.info('Posting the asset')
    post_response = post_asset(file_name)
    asset_id = post_response['id']
    logging.info(f'This is the id of the asset {asset_id}')
    
    user_ids, group_ids = config['USER_IDS'], config['GROUP_IDS']

    put_asset(asset_id, user_ids, group_ids)

    if args.proxy != None:
        upload_url, proxy_id = post_proxy(asset_id, extract_file_name(args.proxy))
        upload_proxy_file(args.proxy, upload_url)
        perform_post_upload_patches(asset_id, proxy_id)

    if args.file != None:
        perform_metadata_addition(asset_id, config['FILE_MAPPING']['VIEW_ID'], construct_metadata_body_from_file(args.file, config['FILE_MAPPING']['MAPPINGS']))
    else:
        for mapping in config['CLIP_MAPPINGS']:
            perform_metadata_addition(asset_id, mapping['VIEW_ID'], construct_metadata_body_from_file(args.metadata, mapping['MAPPINGS']))


    collection_id = config['COLLECTION_ID']
    logging.info('About to move this asset into the collection specified')
    move_asset_to_collection(asset_id, collection_id)

    print(f'These are all of my ids. user_id : {user_ids}, asset_id : {asset_id}, group_id : {group_ids}, collecion_id : {collection_id}')
    print('Donezo!')