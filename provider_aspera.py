import subprocess
import csv
import os
from datetime import datetime
import argparse
from action_functions import *

def browse_folder(aspera_workspace,file_path):
    command = [
        'ascli', 'aoc', 'files',
        f'--workspace={aspera_workspace}',
        'browse', f'{file_path}',
        '--format=csv'
    ]
    # result = subprocess.run(command, capture_output=True)
    result = """Test,folder,6558951,,,edit
DRUM_SAMPLES,link,,,2024-09-19T13:31:25Z,edit"""
    csv_output = result.strip().split('\n')
    return csv_output


def scan_files(aspera_workspace,file_path):
    command = [
        'ascli', 'aoc', 'files',
        f'--workspace={aspera_workspace}',
        'find', f'{file_path}',
        '@ruby:->(f){f["type"].eql?("file")}',
        '--fields=size,path,modified_time,type',
        '--format=csv'
    ]
    # result = subprocess.run(command, capture_output=True)
    result = """544743466,/DRUM_SAMPLES/Squid_Game_Season_1_Episode_1_81262746_00010421_00015108_ko_8.mov,,file
287737214,/DRUM_SAMPLES/The_Witcher_Season_1_Four_Marks_80244464_00145801_00150902_en_6.mov,2024-09-19T13:31:47Z,file
6148,/DRUM_SAMPLES/.DS_Store,2024-08-23T12:22:48Z,file
45918,/DRUM_SAMPLES/CPCT_Kit_1_Hi_Hat_01.wav,2024-08-23T12:22:48Z,file
52092,/DRUM_SAMPLES/CPCT_Kit_1_Kick_01.wav,2024-08-23T12:22:48Z,file
153612,/DRUM_SAMPLES/CPCT_Kit_1_Shaker_02.wav,2024-08-23T12:22:48Z,file
81120,/DRUM_SAMPLES/CPCT_Kit_1_Snare_01.wav,2024-08-23T12:22:48Z,file
445,/Test/sdna-wildfly.properties,2024-09-04T18:27:10Z,file
6557002,/Test/sdna.log,2024-10-09T06:52:10Z,file
1504,/Test/FolderA/STAR Filename.xml,2024-10-09T06:56:16Z,file"""

    csv_output = result.strip().split('\n')
    return csv_output

def upload_file(aspera_workspace,target_path,file_path):
    mkdir_command = [
        'ascli', 'aoc', 'files',
        f'--workspace={aspera_workspace}',
        'mkdir', target_path
    ]
    upload_command = [
        'ascli', 'aoc', 'files',
        f'--workspace={aspera_workspace}',
        'upload',
        f'--to-folder={target_path}',
        file_path
    ]
    subprocess.run(mkdir_command)
    subprocess.run(upload_command)
    print(f"File '{file_path}' uploaded to '{target_path}' successfully.")


def download_file(aspera_workspace, file_path, target_path):
    command = [
        'ascli', 'aoc', 'files',
        f'--workspace={aspera_workspace}',
        'download',
        f'--to-folder={target_path}/',
        file_path
    ]
    subprocess.run(command)
    print(f"File '{file_path}' downloaded to '{target_path}' successfully.")
    
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
        file_list = file_data.strip().split(",")
        file_type = file_list.pop()
        modified_time = file_list.pop()
        if modified_time:
            mtime_struct = datetime.strptime(modified_time.split(".")[0], "%Y-%m-%dT%H:%M:%SZ")
        else:
            mtime_struct = datetime.now()
        mtime_epoch_seconds = int(mtime_struct.timestamp())
        file_name = file_list.pop()
        file_size = file_list.pop()
           
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
            file_object["name"] = file_name
            file_object["size"] = file_size
            file_object["mode"] = "0"
            file_object["type"] = "F_REG" if file_type == "file" else "F_DIR"
            file_object["mtime"] = f'{mtime_epoch_seconds}'
            file_object["atime"] = f'{mtime_epoch_seconds}'
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


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-w','--aspera-workspace', required=True, help='workspace name')
    parser.add_argument('-m', '--mode', required = True, help = 'upload,browse,download,list')
    parser.add_argument('-s','--source',help='source file')
    parser.add_argument('-t','--target',help='target_path')
    parser.add_argument('-f','--foldername',help='folder_name_to_create')
    parser.add_argument('-x','--xmlfilepath',help='xmlfilename')


    args = parser.parse_args()
    aspera_workspace = args.aspera_workspace
    mode = args.mode
    file_path = args.source
    target_path = args.target
    folder_name = args.foldername
    xmlfilepath = args.xmlfilepath


    if mode == 'list':
        file_path = file_path if file_path else "/"
        files_list = scan_files(aspera_workspace,file_path)
        objects_dict = GetObjectDict(files_list)
        if objects_dict and file_path:
            generate_xml_from_file_objects(objects_dict, xmlfilepath)
            print(f"Generated XML file: {xmlfilepath}")
        else:
            print("Failed to generate XML file.")

    elif mode == 'upload':
        upload_file(aspera_workspace,target_path,file_path)

    elif mode == 'browse':
        folders_list = []
        file_path = file_path if file_path else "/"
        folders = browse_folder(aspera_workspace,file_path)
        for folder_data in folders:
            folder = folder_data.strip().split(",")
            folder_name = folder[0]
            folder_type = folder[1]
            if folder_type in ['folder', 'link']:
                folders_list.append(folder_name)

        xml_output = add_CDATA_tags(folders_list)
        print(xml_output)
        # generate_xml(file_path,xml_output)
        # print(f"Generated XML file: {file_path}")

    elif mode == "download":
        download_file(aspera_workspace,file_path,target_path)