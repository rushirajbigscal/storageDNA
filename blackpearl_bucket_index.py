import argparse
import logging
import os
import time
import requests
import json
import subprocess
import base64
import urllib
import urllib.parse
import shutil 
import urllib3
import datetime
import plistlib
import sys
import threading
import concurrent.futures
import xml.etree.ElementTree as ET
from time import sleep
from itertools import repeat
from configparser import ConfigParser
from urllib.request import urlopen, Request
from ds3 import ds3, ds3Helpers

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

MAX_THREADS=10

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


def get_bucket_objects(ds3Client, bucket : str, maxKeys : int, marker : str, full_object_list : list):
    #print(f'In get_bucket_objects... HEY')
    #print(f'MARKER = {marker}')
    bucket_contents = ds3Client.get_bucket(ds3.GetBucketRequest(bucket, max_keys=maxKeys, marker=marker))
    bucket_contents_result = bucket_contents.result
    if bucket_contents_result["IsTruncated"] == 'true':
        #print(f'Is TRUNCATED TRUE')
        marker = bucket_contents_result["NextMarker"]
        return get_bucket_objects(ds3Client, bucket, maxKeys, marker, full_object_list + bucket_contents_result["ContentsList"])
    else:
        return full_object_list + bucket_contents_result["ContentsList"]


def get_file_information(file_item : dict):
    #print(f"File Item = {file_item}") 
    file_path = file_item['Key']
    file_size = file_item['Size']
    file_mod_time = file_item['LastModified']
    file_mod_time = file_mod_time.replace("T"," ")
    if "." in file_mod_time:
        split_string = file_mod_time.split(".")
        file_mod_time = split_string[0]
    file_mod_time = file_mod_time.replace("Z","")
    pattern = '%Y-%m-%d %H:%M:%S'
    file_mod_time_epoch = int(time.mktime(time.strptime(file_mod_time, pattern)))
    #print (f"FILE PATH = {file_path}, MOD TIME = {file_mod_time_epoch}, SIZE = {file_size}")
    return (f"{file_path}##SDNA##{file_size}##SDNA##{file_mod_time_epoch}##SDNA##{file_path}##SDNA##File")


if __name__ == '__main__':
    #start = datetime.datetime.now()
    #print(f"Start time = {start}")

    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--configname', required = True, help = 'Blackpearl config name to work with in cloud_targets.conf')
    parser.add_argument('-b', '--bucket', required = True, help = 'Blackpearl bucket to scan.')
    parser.add_argument('-i', '--sourceindex', required = True, help = 'This is the source index of the selected source folder.')
    parser.add_argument('-t', '--targetscanfile', required = True, help = 'This is the final scan file with file/folder listing.')
    parser.add_argument('-n', '--archivename', required = True, help = 'This is the name of the project.')
    parser.add_argument('-s', '--sourcepath', required = True, help = 'This is the source path.')
    parser.add_argument('-l', '--label', required = False, help = 'This is the label used by the project.')
    
    args = parser.parse_args()

    bucket = args.bucket
    scan_target_file = args.targetscanfile
    source_index = args.sourceindex
    config_name = args.configname
    archive_name = args.archivename
    if args.label is not None:
        label = args.label
    else:
        label = ""
    source_path = args.sourcepath

    strip_path = False
    keep_top = False

    if source_path.endswith("/./"):
        strip_path = True
        keep_top = False
        source_path = source_path.replace("/./","")
    elif "/./" in source_path:
        source_path = source_path.replace("/./", "/")
        strip_path = True
        keep_top = True
        
    if source_path.endswith("/"):
         source_path = source_path[:-1]

    cloudTargetPath = ''
    catalogPath = ''
    tapeProxyPath = ''

    if is_linux == 1:
        config_parser = ConfigParser()
        config_parser.read(DNA_CLIENT_SERVICES)
        if config_parser.has_section('General') and config_parser.has_option('General','cloudconfigfolder'):
            section_info = config_parser['General']
            cloudTargetPath = section_info['cloudconfigfolder'] + "/cloud_targets.conf"
        if config_parser.has_section('General') and config_parser.has_option('General','tapeproxypath'):
            section_info = config_parser['General']
            tapeProxyPath = section_info['tapeproxypath']
    else:
        with open(DNA_CLIENT_SERVICES, 'rb') as fp:
            my_plist = plistlib.load(fp)
            cloudTargetPath = my_plist["CloudConfigFolder"] + "/cloud_targets.conf"
            tapeProxyPath = my_plist["TapeProxyPath"]

    #print(f'TAPE PROXY path = {tapeProxyPath}, CLOUD TARGET path = {cloudTargetPath}')

    if len(label) == 0:
        baseCatalogPath = section_info['tapeproxypath'] + "/" + archive_name + "/1"
    else:
        baseCatalogPath = section_info['tapeproxypath'] + "/" + archive_name + "/1/" + label
    
    if not strip_path and not keep_top:
        catalogPath = baseCatalogPath + source_path + "/"
    elif strip_path and not keep_top:
        catalogPath = baseCatalogPath + "/"
    elif strip_path and keep_top:
        catalogPath = baseCatalogPath + "/" + source_path.split("/")[-1] + "/"

    #print (f"CATALOG path = {catalogPath}")

    if not os.path.exists(cloudTargetPath):
        err= "Unable to find cloud target file: " + cloudTargetPath
        sys.exit(err)

    config_parser = ConfigParser()
    config_parser.read(cloudTargetPath)
    if not config_name in config_parser.sections():
        err = 'Unable to find cloud configuration: ' + config_name
        sys.exit(err)
        
    cloud_config_info = config_parser[config_name]
    access_key = cloud_config_info["access_key"]
    secret_key = cloud_config_info["secret_key"]
    end_point = cloud_config_info["endpoint"]
    if access_key is None or access_key == "":
        err = 'Blackpearl access key not found.'
        sys.exit(err)
    if secret_key is None or secret_key == "":
        err = 'Blackpearl secret key not found.'
        sys.exit(err)
    if end_point is None or end_point == "":
        err = 'Blackpearl endpoint not found.'
        sys.exit(err)

    #print(f'SERVER INFO = access_key = {access_key}, secret_key = {secret_key}, end_point = {end_point}')

    ds3_client = ds3.Client(f"{end_point}", ds3.Credentials(f"{access_key}", f"{secret_key}"))

    full_list = []
    max_keys = 500000
    marker = ""
    bucket_contents_files_info_list = get_bucket_objects(ds3_client, bucket, max_keys, marker, full_list)
    
    #print (bucket_contents_files_info_list)
    #print (f"LEN  = {len(bucket_contents_files_info_list)}")
    
    final_file_list = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [executor.submit(get_file_information, file_item) for file_item in bucket_contents_files_info_list]
        final_file_list = [future.result() for future in concurrent.futures.as_completed(futures)]
    
    #print(f'FINAL FILE LIST = {final_file_list}')
    #print (f'FINAL LIST COUNT = {len(final_file_list)}')

    selected_file_count = 0
    if len(final_file_list) == 0:
        final_scan_xml = '<files scanned="0" selected="0" size="0" bad_dir_count="0" delete_count="0">\n</files>'
    else:
        total_file_count = 0
        total_size = 0
        file_xml_list = []
        for file_info in final_file_list:
            #print (f'\nFILE INFO STR = {file_info}')
            file_split_string = file_info.split("##SDNA##")
            
            file_name = file_split_string[0]
            file_name = file_name.replace("//","")
            if file_name.endswith("/"):
                #print (f"File name ends with /. Invalid. Skip.")
                continue
            
            file_time = file_split_string[2]
            if "." in file_time:
                split_string = file_time.split(".")
                file_time = split_string[0]
            ##Make sure file_time i.e. mod time (epoch) is only 10 characters long
            if len(file_time) > 10:
                #print (f'MOD FILE TIME len > 10  = {len(file_time)}. This is an issue.')
                file_time = file_time[0:10]
                #print (f'NEW MOD FILE TIME = {file_time}')

            file_size = int(file_split_string[1])
            total_size = total_size + file_size

            file_full_path = file_split_string[3]

            item_type = file_split_string[4]

            if catalogPath.endswith("/"):
                catalog_file_path = catalogPath + file_name
            else:
                catalog_file_path = catalogPath + "/" + file_name

            if item_type == "File":
                total_file_count += 1
           
            #print (f'File - ({file_name}) time = {int(file_time)}') 
            ##Here we're replacing the folder paths with spaces before and after with $_ and _$ respectively to prevent catalog not found error.
            if not os.path.exists(catalog_file_path):
                #print ("CATALOG FILE NOT FOUND for = "+catalog_file_path)
                catalog_file_parts_list = catalog_file_path.split("/")
                for i, part in enumerate(catalog_file_parts_list):
                    #print (f"OLD PART = {part}...")
                    if part != "":
                        if part.endswith(" ") and part.startswith(" "):
                            #print(f"1 HERE with part = {part}")
                            part = f"$_{part}_$"
                        elif part.endswith(" "):
                            #print(f"2 HERE with part = {part}")
                            part = f"{part}_$"
                        elif part.startswith(" "):
                            #print(f"3 HERE with part = {part}")
                            part = f"$_{part}"
                        elif ":" in part:
                            #print(f"4 HERE with part = {part}")
                            part = part.replace(":","$;$")
                    #print (f"NEW PART = {part}...")
                    catalog_file_parts_list[i] = part
                catalog_file_path = ("/").join(catalog_file_parts_list)

            #print ("NEW CATALOG FILE built = "+catalog_file_path)   
            if os.path.exists(catalog_file_path):
                #print (f'M-time catalog file - ({file_name}) = {int(os.path.getmtime(catalog_file_path))}.')
                if int(os.path.getmtime(catalog_file_path)) == int(file_time):
                    #print (f'SKIPPING FILE - {file_name}\n')
                    continue

            file_name = file_name.replace("//","")
            file_name = file_name.replace("&","&amp;")
            file_name = file_name.replace("\"","&quot;")

            file_full_path = file_full_path.replace("//","")
            file_full_path = file_full_path.replace("&","&amp;")
            file_full_path = file_full_path.replace("\"","&quot;")

            if item_type == "Folder":
                file_xml_list.append(f'\t<file name="{file_name}" size="0" mode="0x493" type="F_DIR" mtime="{file_time}" atime="{file_time}" owner="0" group="0" index="{source_index}" tmpid="{file_full_path}"/>')
            else:
                selected_file_count += 1
                file_xml_list.append(f'\t<file name="{file_name}" size="{file_size}" mode="0x493" type="F_REG" mtime="{file_time}" atime="{file_time}" owner="0" group="0" index="{source_index}" tmpid="{file_full_path}"/>')

        total_file_stats = f'<files scanned="{total_file_count}" selected="{selected_file_count}" size="{total_size}" bad_dir_count="0" delete_count="0">' 
        file_xml_string = ("\n").join(file_xml_list)
        final_scan_xml = f'{total_file_stats}\n{file_xml_string}\n</files>'
    
        #print(f"TOTAL COUNT = {total_file_count}, SIZE = {total_size}")
     
    #Writing data to xml scan file
    with open(scan_target_file, "w+") as file1:  
        file1.write(final_scan_xml) 
       
    #print(f'{final_scan_xml}')
    
    #end = datetime.datetime.now()
    #print("Execution: {} ".format(end-start))
    exit(0)
    
    