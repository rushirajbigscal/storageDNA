import argparse
import os
import urllib3
import datetime
from action_functions import *
from ds3 import ds3, ds3Helpers

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def get_bucket_objects(ds3Client, bucket : str, maxKeys : int, marker : str, full_object_list : list):
    bucket_contents = ds3Client.get_bucket(ds3.GetBucketRequest(bucket, max_keys=maxKeys, marker=marker))
    bucket_contents_result = bucket_contents.result
    if bucket_contents_result["IsTruncated"] == 'true':
        marker = bucket_contents_result["NextMarker"]
        return get_bucket_objects(ds3Client, bucket, maxKeys, marker, full_object_list + bucket_contents_result["ContentsList"])
    else:
        return full_object_list + bucket_contents_result["ContentsList"]
    
    
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


    for file_item in files_list:
        mtime_struct = datetime.strptime(file_item['LastModified'].split(".")[0], "%Y-%m-%dT%H:%M:%SZ") 
        mtime_epoch_seconds = int(mtime_struct.timestamp())
        file_path = file_item['Key']
        file_name = get_filename(file_path)
        file_size = file_item['Size']
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
            file_object["name"] = file_path
            file_object["size"] = file_size
            file_object["mode"] = "0"
            file_object["type"] = "F_REG" if file_type == "file" else "F_DIR"
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

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', required = True, help = 'Configuration name')
    parser.add_argument('-m', '--mode', required = True, help = 'upload,browse,download,list,actions')
    parser.add_argument('-b', '--bucket', required = True, help = 'Blackpearl bucket to scan.')
    parser.add_argument('-t','--target',help='target_path')
    parser.add_argument('-in', '--indexid', required=False, help = 'REQUIRED if list')
    parser.add_argument('-jg', '--jobguid', required=False, help = 'REQUIRED if list')
    parser.add_argument('-ji', '--jobid', required=False, help = 'REQUIRED if bulk restore.')
    parser.add_argument('-p', "--projectname", required=False, help = 'Project name')
    
    args = parser.parse_args()

    bucket = args.bucket
    mode = args.mode
    target_path = args.target
    
    logging_dict = loadLoggingDict(os.path.basename(__file__), args.jobguid)
    config_map = loadConfigurationMap(args.config)
    filter_file_dict = loadFilterPolicyFiles (args.jobguid)

    params_map = {}
    params_map["bucket"] = args.bucket
    params_map["target"] = args.target
    params_map["indexid" ] = args.indexid
    params_map["jobguid"] = args.jobguid
    params_map["jobid"] = args.jobid
    params_map["project_name"] = args.projectname
    

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
            print('Target path (-t <targetpath> ) -in <index>  options are required for list')
            exit(1)
        access_key = params_map["access_key"]
        secret_key = params_map["secret_key"]
        end_point = params_map["endpoint"]
    
        ds3_client = ds3.Client(f"{end_point}", ds3.Credentials(f"{access_key}", f"{secret_key}"))
        
        full_list = []
        max_keys = 500000
        marker = ""
        bucket_contents_files_info_list = get_bucket_objects(ds3_client, bucket, max_keys, marker, full_list)
        objects_dict = GetObjectDict(bucket_contents_files_info_list,params_map)
        if len(bucket_contents_files_info_list) == 0:
            objects_dict = {}
        if target_path:
            generate_xml_from_file_objects(objects_dict, target_path)
            print(f"Generated XML file: {target_path}")
            exit(0)
        else:
            print("Failed to generate XML file.")
            exit(1)
    
    