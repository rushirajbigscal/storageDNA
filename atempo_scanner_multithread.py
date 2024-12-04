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


def get_list_folder_xml(url : str, archive : str, path : str):
    archive = urllib.parse.quote(archive)
    path = urllib.parse.quote(path)
    src_path = f'{archive}@{path}'
    #print(f'GET LS SRC PATH = {src_path}')
    url += f"?src={src_path}"
    #print(f'GET LS FINAL URL = {url}')
    httprequest = Request(url)
    with urlopen(httprequest) as response:
        #print(response.status)
        #print(response.read().decode())
        return response.read().decode()
   

def get_archived_file_details_xml(url : str, archive : str, path : str):
    archive = urllib.parse.quote(archive)
    path = urllib.parse.quote(path)
    src_path = f'{archive}@{path}'
    #print(f'GET FILESTAT SRC PATH = {src_path}')
    url += f"?src={src_path}"
    #print(f'GET FILESTAT FINAL URL = {url}')
    httprequest = Request(url)
    with urlopen(httprequest) as response:
        #print(response.status)
        #print(response.read().decode())
        return response.read().decode()
    

def search_for_files(atempo_url : str, atempo_archivename : str, folder_path : str, files_list : str):
    #print(f'FOLDER PATH = {folder_path}')
    url = f'{atempo_url}/ls'
    #print(f"LS URL = {url}")
    folder_xml_resp = get_list_folder_xml(url, atempo_archivename, folder_path)
    if folder_xml_resp is None or folder_xml_resp == "":
        err = 'No xml response obtained for folder listing.'
        sys.exit(err)
    #print(f'FOLDER RESP XML = {folder_xml_resp}')
    root_elem = ET.fromstring(folder_xml_resp)
    #print(f'FOLDER LISTING ROOT ELEM = {root_elem.tag}')
    #print("Object XML tag found.")
    return_code = root_elem.find('ReturnCode').attrib['ADARetCode']
    #print(f'RETURN CODE = {return_code}')
    if return_code != "1":
        err = 'Issue with GET request on folders.'
        sys.exit(err)
    for obj in root_elem.findall('object'):
        attrib_dict = obj.attrib
        obj_type = attrib_dict['type']
        obj_name = attrib_dict['name']
        #print(f"OBJ TYPE = {obj_type}, NAME = {obj_name}")
        if obj_type == 'folder' or obj_type == 'directory':
            #print("Folder/Dir found.")
            new_folder_path = f'{folder_path}/{obj_name}'
            new_folder_path = new_folder_path.replace("//","/")
            search_for_files(atempo_url, atempo_archivename, new_folder_path, files_list)
        else:
            file_path = f'{folder_path}/{obj_name}'
            file_path = file_path.replace("//","/")
            files_list.append(file_path)
    return files_list


def get_file_information(atempo_url : str, atempo_archivename : str, orig_folder_path : str, file_path : str):
    #print("File found.") 
    file_url = f'{atempo_url}/getFileStatus'
    #print(f"\nFILE URL = {file_url}")
    #print(f'FILE PATH = {file_path}')
    file_attrib_dict = {}
    found_result = False
    count=0
    while not found_result:
        count = count + 1
        if count > 10:
            #print (f"Exiting after 10 tries. Failed to find file details for - {file_path}")
            break
        file_xml_resp = get_archived_file_details_xml(file_url, atempo_archivename, file_path)
        #print(f'FILE RESP XML = {file_xml_resp}')
        file_root_elem = ET.fromstring(file_xml_resp)
        #print(f'FILE DETAILS ROOT ELEM = {file_root_elem.tag}')
        if file_root_elem.find('instance') == None or len(file_root_elem.findall('instance')) == 0:
            #print ("ISSUE FINDING FILE DETAILS. RETRYING...")
            sleep(0.5)
            continue
        else:
            found_result = True
            file_attrib_dict = file_root_elem.find('instance').attrib
    #print(f'FILE ATTRIB DICT = {file_attrib_dict}')
    file_mod_time_epoch = "NOT_FOUND"
    file_size = "NOT_FOUND"
    if len(file_attrib_dict) > 0:
        if 'last_update' in file_attrib_dict:
            file_mod_time = file_attrib_dict['last_update']
            if len(file_mod_time.strip()) != 0:
                pattern = '%Y/%m/%d-%H:%M:%S'
                #print (f"FILE MOD TIME FROM ATEMPO BEFORE EPOCH CONVERSION = {file_mod_time}")
                file_mod_time_epoch = int(time.mktime(time.strptime(file_mod_time, pattern)))

        if file_mod_time_epoch == "NOT_FOUND" and 'last_access' in file_attrib_dict:
            file_mod_time = file_attrib_dict['last_access']
            if len(file_mod_time.strip()) != 0:
                pattern = '%Y/%m/%d-%H:%M:%S'
                #print (f"FILE ACCESS TIME FROM ATEMPO BEFORE EPOCH CONVERSION = {file_mod_time}")
                file_mod_time_epoch = int(time.mktime(time.strptime(file_mod_time, pattern)))
    
        if 'file_size' in file_attrib_dict:
            file_size = file_attrib_dict['file_size']

    relative_file_path = file_path.replace(orig_folder_path, "/")
    relative_file_path = relative_file_path.replace("//", "/")
    #print (f"FILE PATH = {relative_file_path}, MOD TIME = {file_mod_time_epoch}, SIZE = {file_size}")
    return (f"{relative_file_path}##SDNA##{file_size}##SDNA##{file_mod_time_epoch}##SDNA##{file_path}##SDNA##File")


if __name__ == '__main__':
    #start = datetime.datetime.now()
    #print(f"Start time = {start}")

    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--configname', required = True, help = 'FileCatalyst config name to work with in cloud_targets.conf')
    parser.add_argument('-p', '--folderpath', required = True, help = 'Folder path to scan.')
    parser.add_argument('-i', '--sourceindex', required = True, help = 'This is the source index of the selected source folder.')
    parser.add_argument('-t', '--targetscanfile', required = True, help = 'This is the final scan file with file/folder listing.')
    parser.add_argument('-n', '--archivename', required = True, help = 'This is the name of the project.')
    parser.add_argument('-s', '--sourcepath', required = True, help = 'This is the source path.')
    parser.add_argument('-l', '--label', required = False, help = 'This is the label used by the project.')
    
    args = parser.parse_args()

    folder_path = args.folderpath
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
    atempo_url = cloud_config_info["URL"]
    atempo_archive_name = cloud_config_info["archive_name"]
    if atempo_url is None or atempo_url == "":
        err = 'Atempo URL not found.'
        sys.exit(err)
    if atempo_archive_name is None or atempo_archive_name == "":
        err = 'Atempo archive name not found.'
        sys.exit(err)

    #print(f'SERVER INFO = IP = {server_ip}, PORT = {server_port}, USER = {user_name}, PASS = {pass_word}')
    
    #atempo_url = "http://ada-db.msvpost.local/meta/6187C61F31030C4A54B6DF83610F5124/760765700d7e1e7d0902/ADA/WS"
    #atempo_archive_name = "SEQUENCE_POST"
    #folder_path = "/SEQ_007/SPP_G-RAID/MSV_LTO007/JUNE_2011_BACKUPS/EDIT_LOCAL_BACKUPS080411/EDIT1"
    
    fileList = []
    file_list = search_for_files(atempo_url, atempo_archive_name, folder_path, fileList)
    
    '''
    #Multi threading using concurrent futures
    final_file_list = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        file_deets_list = list(executor.map(get_file_information, repeat(atempo_url), repeat(atempo_archive_name), repeat(folder_path), file_list))

        for file_deets in file_deets_list:
            #print(f'FILE DEETS STR = {file_deets}')
            final_file_list.append(file_deets)

    #print(f'FINAL FILE LIST = {final_file_list}')
    #print (f'FINAL LIST COUNT = {len(final_file_list)}')
    '''

    final_file_list = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [executor.submit(get_file_information, atempo_url, atempo_archive_name, folder_path, file_item) for file_item in file_list]
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
            
            file_time = file_split_string[2]
            #print (f'FILE TIME = {file_time}')
            if file_time == "NOT_FOUND":
                #print (f'SKIPPING FILE - {file_name} as file_time = NOT_FOUND.')
                continue
            else:
                if "." in file_time:
                    split_string = file_time.split(".")
                    file_time = split_string[0]
                ##Make sure file_time i.e. mod time (epoch) is only 10 characters long
                if len(file_time) > 10:
                    #print (f'MOD FILE TIME len > 10  = {len(file_time)}. This is an issue.')
                    file_time = file_time[0:10]
                    #print (f'NEW MOD FILE TIME = {file_time}')

            file_size = 0
            if file_split_string[1] == "NOT_FOUND":
                #print (f'SKIPPING FILE - {file_name} as file_size = NOT_FOUND.')
                continue
            else:
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
                    #print (f"NEW PART = {part}...")
                    catalog_file_parts_list[i] = part
                catalog_file_path = ("/").join(catalog_file_parts_list)

            #print ("NEW CATALOG FILE built = "+catalog_file_path)   
            #print (f'M-time catalog file - ({file_name}) = {int(os.path.getmtime(catalog_file_path))}.')
            if os.path.exists(catalog_file_path):
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
