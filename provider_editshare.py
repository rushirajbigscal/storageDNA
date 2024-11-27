import subprocess
import os
import shutil
from datetime import datetime
import xml.etree.ElementTree as ET
import argparse
from action_functions import *
import requests
from urllib import request
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def get_search_call(folder_name,recursive=None):
    url = f"https://{params_map["hostname"]}:{params_map["port"]}/api/v2/search"
    params = {
        "recursive" : recursive,
        "dir" : folder_name,
        "clips" : "true",
        "images" : "false",
        "markers" : "false",
        "subclips" : "false",
        "sequences" : "false"
    }
    # response = requests.get(url,params=params)
    # if response.status_code != 200:
    #     print(f"Response error. Status - {response.status_code}, Error - {response.text}")
    #     return False
    response = [
        {
            "clip_id": 12
        },
        {
            "clip_id": 20
        }
    ]
    return response

def get_clip_call(clip_id):
    url = f"https://{params_map["hostname"]}:{params_map["port"]}/api/v2/metadata/clips/{clip_id}"
    params = {
        "proxy_details" : "true"
    }
    # response = requests.get(url,params=params)
    # if response.status_code != 200:
    #     print(f"Response error. Status - {response.status_code}, Error - {response.text}")
    #     return False
    response = {
    "asset": {
        "asset_id": 144,
        "asset_type": 3,
        "asset_type_id": 913801,
        "asset_type_text": "clip",
        "asset_version": -1,
        "comment": "these are my comments",
        "last_modified": "2021-03-29T11:52:46Z",
        "revision": 3,
        "thumbnail": "8eb671d1-f44c-4e2b-846b-0f181f9c79d9.png",
        "uuid": "20403d64-6a0d-4f26-954d-d31bd419237d",
        "custom": {
            "field_2": "New value for custom meta data field with 'db_key' field_2"
        },
        "customtypes": {
            "field_2": "QString"
        },
        "vendor_data": [
            {
                "vendor": "avid",
                "key": "town",
                "value": "Basingstoke"
            },
            {
                "vendor": "adobe",
                "key": "uuid",
                "value": "20fb09fb-9cb5-4b10-ab69-4232c6899899"
            }
        ]
    },
    "audio": [
        {
            "bit_depth": 16,
            "channel": 0,
            "clip_id": 913801,
            "compression": "pcm24le",
            "essence_id": 2529453,
            "file_id": 1492747,
            "sample_rate": 48000,
            "track": 1
        }
    ],
    "capture": {
        "capture_id": 87,
        "chunk_group_id": 10,
        "chunk_group_index": 0,
        "chunk_group_uuid": "21aeafc1-dca3-40a0-b7b4-ea62bf600756",
        "project": "MyProject",
        "recording_id": "13837ac0-b035-4eea-8752-baa980cee318",
        "source": "SDI1",
        "tape": "Tape 001"
    },
    "clip_id": 913801,
    "play_aspect_ratio": "16:9",
    "display_audio": "A1-A2, 24 bit, WAV, 48.000 kHz",
    "display_filesize": "133.44 MB",
    "display_filetype": "mxfop1a",
    "display_frame_rate": "25 fps",
    "display_name": "c2021-03-29-12-49-57-01",
    "display_standard": "1080i25",
    "display_video_codec": "XDCAM-EX 35Mb 1080i",
    "display_video_size": "1920 x 1080",
    "has_audio": "true",
    "has_data": "false",
    "has_index_file": "false",
    "has_video": "true",
    "has_transcript": "true",
    "is_recording": "false",
    "metadata": {
        "comment": "these are my comments",
        "project": "My Project",
        "scene": "Scene1",
        "take": "Take1",
        "tape": "Tape 001",
        "captured": "2020-08-06T14:13:07Z",
        "clip_name": "this is my clip",
        "clip_name_with_extension": "this is my clip.mxf",
        "modified": "2015-11-11T02:29:00Z",
        "timecode_end": "10:00:29:24:25/1",
        "timecode_in": "10:00:04:10:25/1",
        "timecode_out": "10:00:15:05:25/1",
        "timecode_start": "10:00:00:00:25/1"
    },
    "mob_ids": {
        "master": "52947134-0cbc-54ba-0052-9471340cbc00",
        "video_tracks": [
            "52947134-0cbc-54ba-0052-9471340cbc01"
        ],
        "audio_tracks": [
            "52947134-0cbc-54ba-0052-9471340cbc01",
            "52947134-0cbc-54ba-0052-9471340cbc01"
        ]
    },
    "proxy_filename": "b093e5e8-dd42-4ddb-97b4-ac4b4c3cb3de.txt",
    "proxy_has_index_file": "false",
    "proxy_id": 429220,
    "proxy_path": "proxy/proxy-de/b093e5e8-dd42-4ddb-97b4-ac4b4c3cb3de.txt",
    "status_flags": [
        "online",
        "virtual_masterclip"
    ],
    "status_text": "Online, Virtual Source clip",
    "video": [
        {
            "aspect_ratio": "16:9",
            "channel": -1,
            "clip_id": 913801,
            "data_rate": 35000,
            "dominance": "top field first",
            "encoding_id": 408,
            "essence_id": 869223,
            "file": {
                "display_filesize": "133.44 MB",
                "display_filetype": "mov",
                "display_name": "c2021-03-29-12-49-57-01",
                "file": {
                    "created": "2023-11-11T02:29:00Z",
                    "filesize": 916846592,
                    "hash": "2:03d96435c9a6dfa760d3faa741d78a8a",
                    "modified": "2023-11-11T02:29:00Z",
                    "type": "mov",
                    "file_id": 1492747,
                    "is_placeholder": "false",
                    "mxf_descriptor_data": {
                        "aspectRatio": {
                            "denominator": 9,
                            "numerator": 16
                        },
                        "blackRefLevel": 16,
                        "colorRange": 225,
                        "componentWidth": 8,
                        "dataOffset": 393241,
                        "didImageSize": 77438976,
                        "didResolutionID": 1253,
                        "displayHeight": 1080,
                        "displayWidth": 1920,
                        "displayXOffset": 0,
                        "displayYOffset": 0,
                        "editRate": 29.970029830932617,
                        "frameIndexOffset": 77463726,
                        "frameLayout": 0,
                        "frameSampleSize": 188416,
                        "horizontalSubsampling": 2,
                        "length": 411,
                        "offsetToRLEFrameIndexes": -1,
                        "sampledHeight": 1080,
                        "sampledWidth": 1920,
                        "sampledXOffset": 0,
                        "sampledYOffset": 0,
                        "storedHeight": 1080,
                        "storedWidth": 1920,
                        "verticalSubsampling": 8,
                        "videoLineMap": [
                            42,
                            0
                        ],
                        "whiteRefLevel": 235
                    },
                    "mob_ids": {
                        "file_mob_id": "060a2b340101010501010f1013-000000-220ac4ac95538005-f0a700155d3f-43e8",
                        "master_mob_id": "060a2b340101010501010f1013-000000-220ac49c95538005-8ece00155d3f-43e8",
                        "source_mob_id": "060a2b340101010501010d1213-a1a5d0-a0f88e0353950580-3f2800155d3f-43e8"
                    }
                },
                "locations": [
                    {
                        "archive": "false",
                        "file_id": 1492747,
                        "media_space_location_id": 4162,
                        "media_space_name": "quicktime ingest",
                        "media_space_uuid": "beebf627-6a21-4d0c-b7a9-073ce4fa2540",
                        "username": "Gandalf",
                        "userpath": "subfolder/promo.mp4"
                    }
                ],
                "status_flags": [
                    "online"
                ],
                "status_text": "Online"
            },
            "file_id": 1492747,
            "fourcc": "xdvc",
            "frame_rate": "25/1",
            "gop_length": 0,
            "height": 1080,
            "is_image_sequence": "false",
            "timecode_duration": "00:00:30:00:25/1",
            "timecode_end": "10:00:29:24:25/1",
            "timecode_start": "10:00:00:00:25/1",
            "track": 1,
            "video_codec": "XDCAM-EX 35Mb 1080i",
            "width": 1920
        },
        {
            "aspect_ratio": "16:9",
            "channel": -1,
            "clip_id": 913801,
            "data_rate": 35000,
            "dominance": "top field first",
            "encoding_id": 408,
            "essence_id": 869223,
            "file": {
                "display_filesize": "133.44 MB",
                "display_filetype": "mov",
                "display_name": "c2021-03-29-12-49-57-01",
                "file": {
                    "created": "2023-11-11T02:29:00Z",
                    "filesize": 916846592,
                    "hash": "2:03d96435c9a6dfa760d3faa741d78a8a",
                    "modified": "2023-11-11T02:29:00Z",
                    "type": "mov",
                    "file_id": 1492747,
                    "is_placeholder": "false",
                    "mxf_descriptor_data": {
                        "aspectRatio": {
                            "denominator": 9,
                            "numerator": 16
                        },
                        "blackRefLevel": 16,
                        "colorRange": 225,
                        "componentWidth": 8,
                        "dataOffset": 393241,
                        "didImageSize": 77438976,
                        "didResolutionID": 1253,
                        "displayHeight": 1080,
                        "displayWidth": 1920,
                        "displayXOffset": 0,
                        "displayYOffset": 0,
                        "editRate": 29.970029830932617,
                        "frameIndexOffset": 77463726,
                        "frameLayout": 0,
                        "frameSampleSize": 188416,
                        "horizontalSubsampling": 2,
                        "length": 411,
                        "offsetToRLEFrameIndexes": -1,
                        "sampledHeight": 1080,
                        "sampledWidth": 1920,
                        "sampledXOffset": 0,
                        "sampledYOffset": 0,
                        "storedHeight": 1080,
                        "storedWidth": 1920,
                        "verticalSubsampling": 8,
                        "videoLineMap": [
                            42,
                            0
                        ],
                        "whiteRefLevel": 235
                    },
                    "mob_ids": {
                        "file_mob_id": "060a2b340101010501010f1013-000000-220ac4ac95538005-f0a700155d3f-43e8",
                        "master_mob_id": "060a2b340101010501010f1013-000000-220ac49c95538005-8ece00155d3f-43e8",
                        "source_mob_id": "060a2b340101010501010d1213-a1a5d0-a0f88e0353950580-3f2800155d3f-43e8"
                    }
                },
                "locations": [
                    {
                        "archive": "false",
                        "file_id": 1492747,
                        "media_space_location_id": 4162,
                        "media_space_name": "quicktime ingest",
                        "media_space_uuid": "beebf627-6a21-4d0c-b7a9-073ce4fa2540",
                        "username": "Gandalf",
                        "userpath": "subfolder/promo.mp4"
                    }
                ],
                "status_flags": [
                    "online"
                ],
                "status_text": "Online"
            },
            "file_id": 1492747,
            "fourcc": "xdvc",
            "frame_rate": "25/1",
            "gop_length": 0,
            "height": 1080,
            "is_image_sequence": "false",
            "timecode_duration": "00:00:30:00:25/1",
            "timecode_end": "10:00:29:24:25/1",
            "timecode_start": "10:00:00:00:25/1",
            "track": 1,
            "video_codec": "XDCAM-EX 35Mb 1080i",
            "width": 1920
        }
    ],
    "virtual_child_clip_ids": [
        913804
    ]
}
    return response

def get_download_id(file_id):
    url = f"https://{params_map["hostname"]}:{params_map["port"]}/api/v2/transfer/download"
    json_body = {
    "file_id": file_id,
    "offset": 0
    }

    # response = requests.post(url,json=json_body)
    # if response.status_code != 201:
    #     print(f"Response error. Status - {response.status_code}, Error - {response.text}")
    #     return False
    response = [
        {
            "expires": "2021-08-18T15:21:57Z",
            "file_id": "null",
            "file_path": "folder/Kittens.mov",
            "file_size_bytes": 8589934592,
            "mediaspace": "Test",
            "name": "Kittens",
            "offset": 0,
            "transfer": "c838cd8f-93bf-49d4-ab4c-5ff745b01468"
        }
    ]

    return response[0]["transfer"]

def get_upload_id(file_path,mediaspace,user):
    url = f"https://{params_map["hostname"]}:{params_map["port"]}/api/v2/transfer/upload"
    json_body = {
    "create_proxy": "true",
    "fail_if_exists": "false",
    "file_path":file_path ,
    "mediaspace":mediaspace ,
    "user": user
}

    # response = requests.post(url,json=json_body)
    # if response.status_code != 201:
    #     print(f"Response error. Status - {response.status_code}, Error - {response.text}")
    #     return False
    response = [
  {
    "create_proxy": "true",
    "expires": "2021-08-18T15:21:57Z",
    "fail_if_exists": "false",
    "file_path": "folder/Rabbits.mov",
    "file_size_bytes": 6442450944,
    "group": "a81b10ae-404b-4113-a981-6b00fd6974c7",
    "mediaspace": "Test",
    "resource_type": "asset",
    "scan": "true",
    "transfer": "3cc92a07-e052-448e-b660-7352c8f21edf",
    "user": "bbunny"
  }
]

    return response[0]["transfer"]


def upload_file(upload_id):
    url = f"https://{params_map["hostname"]}:{params_map["port"]}/api/v2/transfer/upload/{upload_id}"
    # response = requests.put(url)
    # if response.status_code != 200:
    #     print(f"Response error. Status - {response.status_code}, Error - {response.text}")
    #     return False
    return True



def download_file(download_id):
    url = f"https://{params_map["hostname"]}:{params_map["port"]}/api/v2/transfer/download/{download_id}"
    # response = requests.get(url)
    # if response.status_code != 200:
    #     print(f"Response error. Status - {response.status_code}, Error - {response.text}")
        # return False
    return True

def process_clip_data(clip_data):
    list = []
    for clip in clip_data["video"]:
        data1 = {}
        data1["filename"] = clip["file"]["locations"][0]["userpath"]
        data1["filesize"] = clip["file"]["file"]["filesize"]
        data1["mtime"] = clip["file"]["file"]["created"]
        data1["atime"] = clip["file"]["file"]["modified"]
        data1["file_id"] = clip["file"]["file"]["file_id"]
        data1["hash"] = clip["file"]["file"]["hash"]
        list.append(data1)

    data = {}
    data["clip_id"] = clip_data["clip_id"]
    data["metadata"] = clip_data["metadata"]
    data["proxy_path"] = clip_data["proxy_path"]
    data["proxy_id"] = clip_data["proxy_id"]
    data["videos"] = list

    return data


def GetObjectDict(data_list,params):
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

    for data in data_list:
        clip_id = data["clip_id"]
        metadata = data["metadata"]

        for video in data["videos"]:
            file_path = video["filename"]
            file_name = get_filename(file_path)
            file_size = video["filesize"]
            file_type = "file" if file_size != "0" else "dir"
            mtime_struct = datetime.strptime(video['mtime'], "%Y-%m-%dT%H:%M:%SZ")
            atime_struct = datetime.strptime(video['atime'], "%Y-%m-%dT%H:%M:%SZ")
            mtime_epoch_seconds = int(mtime_struct.timestamp())
            atime_epoch_seconds = int(atime_struct.timestamp())
            clip_metadata_file_name = f"C:/temp/clip_{clip_id}.html"
            clip_html = generate_html(metadata,clip_metadata_file_name)

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
                file_object["url"] = f"{clip_html}"
                file_object["tmpid"] = f"file|{video["file_id"]}"
                file_object["checksum"] = video["hash"]
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

        proxy_path = data["proxy_path"]
        proxy_file_name = get_filename(proxy_path)
        proxy_id = data["proxy_id"]
        mtime_epoch_seconds = "0"
        proxy_size = "10"
        proxy_file_type = "file" if proxy_size != "0" else "dir"

        if proxy_file_type.lower() != 'file'  or filter_type == 'none':
            include_file = True
        elif len(extensions) == 0:
            continue
        else:
            file_in_list = isFilenameInFilterList(proxy_file_name, extensions)
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
                include_file = file_in_policy(policy_dict, proxy_file_name, proxy_path, proxy_size, mtime_epoch_seconds)

        file_object = {}
        if include_file == True:
            file_object["name"] = proxy_path
            file_object["size"] = proxy_size
            file_object["mode"] = "0"
            file_object["tmpid"] = f"proxy|{proxy_id}"
            file_object["type"] = "F_REG" if proxy_file_type == "file" else "F_DIR"
            file_object["mtime"] = f'{mtime_epoch_seconds}'
            file_object["atime"] = f'{mtime_epoch_seconds}'
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


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', required = True, help = 'Configuration name')
    parser.add_argument('-m', '--mode', required = True, help = 'upload,download,list,actions')
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
    target_path = args.target
    folder_name = args.foldername
    tmp_id = args.tmp_id

    config_map = loadConfigurationMap(args.config)
    logging_dict = loadLoggingDict(os.path.basename(__file__), args.jobguid)
    filter_file_dict = loadFilterPolicyFiles (args.jobguid)

    params_map = {}
    params_map["foldername"] = args.foldername
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
        if target_path is None or args.indexid is None:
            print('Target path (-t <targetpath>) and -in <index>  options are required for list')
            exit(1)
        clip_id_list = get_search_call(folder_name,recursive="true")
        clip_ids = [clip["clip_id"] for clip in clip_id_list]
        clips_data = []
        for clip_id in clip_ids:
            if get_clip_call(clip_id):
                clips_data.append(process_clip_data(get_clip_call(clip_id)))
            else:
                print("Failed to get an clip data.")
                
        objects_dict = GetObjectDict(clips_data,params_map)
        if len(clips_data) == 0:
            objects_dict = {}
            
        if objects_dict and target_path:
            generate_xml_from_file_objects(objects_dict, target_path)
            print(f"Generated XML file: {target_path}")
            exit(0)
        else:
            print("Failed to generate XML file.")
            exit(1)

    elif mode == 'upload':
        upload_id = get_upload_id(file_path,mediaspace="test",user="test")
        if upload_file(upload_id):
            print(f"File Uploaded sucessfully. {file_path}")
            exit(0)
        else:
            print("Failed to upload file.")
            exit(1)

        
    elif mode == 'browse':
        pass

    elif mode == "download":
        file_type = tmp_id.split("|")[0]
        if file_type == "file":
            file_id = tmp_id.split("|")[-1]
        else:
            print("Add correct file id")
            exit(1)
        download_id = get_download_id(file_id)
        if download_file(download_id):
            print(f"File Downloaded: {target_path}")
            exit(0)
        else:
            print("Faild to download file.")
            exit(1)
        
    else:
        print(f'Unsupported mode {mode}')
        exit(1)