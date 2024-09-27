import argparse
import os
import sys
import subprocess

ACTION_LIST = "list"
ACTION_UPLOAD = "upload"
ACTION_DOWNLOAD = "download"
ACTION_DELETE = "delete"
ACTION_BROWSE = "browse"
ACTION_CREATEFOLDER = "createfolder"
ACTION_BULKRESTORE = "bulkrestore"
ACTION_PROGRESS = "progress"

linux_dir = "/opt/sdna/bin"
is_linux = 0
if os.path.isdir(linux_dir):
    is_linux = 1
DNA_CLIENT_SERVICES = ''
if is_linux == 1:    
    DNA_CLIENT_SERVICES = '/etc/StorageDNA/DNAClientServices.conf'
    SERVERS_CONF_FILE = "/etc/StorageDNA/Servers.conf"
    PROVIDER_SCRIPT_PATH = "/opt/sdna/bin"
else:
    DNA_CLIENT_SERVICES = '/Library/Preferences/com.storagedna.DNAClientServices.plist'
    SERVERS_CONF_FILE = "/Library/Preferences/com.storagedna.Servers.plist"
    PROVIDER_SCRIPT_PATH = "/"


def process_list_request(provider_script, config_name, index, job_guid, action, filter_type, filter_file, policy_file, target_file):
    if filter_type.lower() == 'none':
        list_proc = subprocess.run(['python3', provider_script, "-m", action, "-c", config_name, "-in" , index, "-ft", filter_type, "-pf", policy_file , "-ji", job_guid, "-t", target_file])
    else:
        list_proc = subprocess.run(['python3', provider_script, "-m", action, "-c", config_name, "-in" , index, "-ft", filter_type, "-ff", filter_file, "-pf", policy_file , "-ji", job_guid, "-t", target_file])

    if list_proc.returncode != 0:
        print(f'Unable to get list of files from provider - {list_proc.returncode} {list_proc.stderr}')
        sys.exit(4)


def process_upload_request(provider_script, config_name, action, source_file, target_file):
    copy_proc = subprocess.run(['python3', provider_script, "-c", config_name, "-m", action, "-s",  source_file, "-t",  target_file], capture_output=True, text=True)
    if copy_proc.returncode != 0:
        print('Unable to (upload) copy file {copy_proc.stderr}')
        sys.exit(4)


def process_download_request(provider_script, config_name, action, source_file, target_file):
    copy_proc = subprocess.run(['python3', provider_script, "-c", config_name, "-m",  action, "-s", source_file, "-t",  target_file], capture_output=True, text=True)
    if copy_proc.returncode != 0:
        print('Unable to (download) copy file {copy_proc.stderr}')
        sys.exit(4)


def processs_bulk_restore_request(provider_script, config_name, action, restore_ticket_path, job_guid, progress_file):
    bulk_restore_proc = subprocess.run(['python3', provider_script, "-m", action, "-c", config_name, "--restoreticketpath", restore_ticket_path, "-ji" , job_guid, "--progressfile", progress_file])
    if bulk_restore_proc.returncode != 0:
        print(f'Unable to perform bulk restore - {bulk_restore_proc.returncode} {bulk_restore_proc.stderr}')
        sys.exit(4)


def process_browse_request(provider_script, config_name, action, folder_name):
    browse_proc = subprocess.run(['python3', provider_script, "-m", action, "-c", config_name, "-s",  folder_name], capture_output=True, text=True)
    if browse_proc.returncode != 0:
        print('Unable to browse folder {browse_proc.stderr}')
        sys.exit(4)
    print(browse_proc.stdout)

'''
def process_delete_request(provider_script, action, target_file):
    
    #Call the provider script e.g. if provider is rsync, the script will be provider_rsync.py

    del_proc = subprocess.run(['python3', provider_script, "-a", action, "-t",  target_file], capture_output=True, text=True)
    if del_proc.returncode != 0:
        print('Unable to delete target file {target_file}  {del_proc.stderr}')
        sys.exit(4)
    print(del_proc.stdout)

def process_create_folder_request(provider_script, action, folder_name):
    
    #Call the provider script e.g. if provider is rsync, the script will be provider_rsync.py

    create_proc = subprocess.run(['python3', provider_script, "-a", action, "-f",  folder_name], capture_output=True, text=True)
    if create_proc.returncode != 0:
        print('Unable to copy file {create_proc.stderr}')
        sys.exit(4)
    print(create_proc.stdout)
    
#def start_server(provider_script,provider):
 #   start_server = subprocess.run(['python3', provider_script,"--provider", provider])

def process_progress_request(provider_script,action,progress_id,progress_file,project_id,run_id,provider):
    process_progress_request = subprocess.run(['python3', provider_script,"--provider", provider,"--action",action,"--progress-id", progress_id,"--progress-file",progress_file , "--project-id" , project_id , "--run-id",run_id])
    if process_progress_request.returncode != 0:
        print('Unable to copy file {process_progress_request.stderr}')
        sys.exit(4)
'''


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--provider', required=True, help='Provider needed for the call')
    parser.add_argument('--configname', required=False, help='Object config name')
    parser.add_argument("--index", required=False, help="Index value (e.g. listing for archiware)")
    parser.add_argument('--jobguid', required=False, help='Job GUID')
    parser.add_argument('--sessionrunid', required=False, help='Session/run guid')
    parser.add_argument('--jobid', required=False, help='Job Id')
    parser.add_argument('--action', required=False, help='Action to perform',
                        choices=['list', 'upload', 'download', 'delete', 'browse', 'createfolder','bulkrestore','progress'])
    parser.add_argument('-s', '--source', required=False, help='Source file for upload, download, list commands.')
    parser.add_argument('-t', '--target', required=False, help='Target file for upload, download and list commands.')
    parser.add_argument('-f', '--foldername', required=False, help='Folder name used for browse and create folder.')
    parser.add_argument('--filtertype', required=False, choices=['none', 'include', 'exclude'], help='Filter type')
    parser.add_argument('--filterfile', required=False, help='Extension file')
    parser.add_argument('--policyfile', required=False, help='Policy file')
    parser.add_argument('--progressid', required=False, help='progress-id')
    parser.add_argument('--progressfile', required=False, help='progress-file')
    parser.add_argument('--restoreticketpath', required=False, help='XML restore ticket file')
    
    args = parser.parse_args()

    provider_script_name = f'provider_{args.provider}.py'
    if not os.path.isfile(provider_script_name):
        print(f'Invalid provider given. No script exists for {provider_script_name}')
        sys.exit(3)
    
    if args.action == ACTION_LIST:
        filter_type = 'none'
        if args.filtertype is None or args.filtertype == "":
            filter_type = 'none'
        if not filter_type.lower() == 'none' and args.filterfile is None:
            print("Action list must have --filter-file parameter.")
            sys.exit(1)

        policy_filename = ""
        if not args.policyfile is None:
            policy_filename = args.policyfile
            if not os.path.isfile(policy_filename):
                print(f'Policy file {policy_filename} cannot be found.')
                sys.exit(5)
                
        process_list_request(provider_script_name, args.configname, args.index, args.jobguid , args.action, filter_type, args.filterfile, policy_filename, args.target)

    elif args.action == ACTION_UPLOAD:
        process_upload_request(provider_script_name, args.configname, args.action, args.source, args.target)

    elif args.action == ACTION_DOWNLOAD:
        process_download_request(provider_script_name, args.configname, args.action, args.source, args.target)

    elif args.action == ACTION_BULKRESTORE:
        processs_bulk_restore_request(provider_script_name, args.configname, args.action, args.restoreticketpath, args.jobguid, args.progressfile)
        
    elif args.action == ACTION_BROWSE:
        if args.foldername is None:
            print("Action browse must have --foldername parameter")
            sys.exit(1)
        process_browse_request(provider_script_name, args.configname, args.action, args.foldername)
    '''
    elif args.action == ACTION_DELETE:
        if args.targetfile is None:
            print("Action delete must have --targetfile parameter")
            sys.exit(1)
        process_delete_request(provider_script_path, args.action, args.targetfile)
    
    elif args.action == ACTION_CREATEFOLDER:
        if args.foldername is None:
            print("Action createfolder must have --foldername parameter")
            sys.exit(1)
        process_create_folder_request(provider_script_path, args.action, args.foldername)

    
    elif args.action == ACTION_PROGRESS:
        if args.progress_id and args.progress_file is None:
            print("Action progress must have --progress-id and --progress-file parameter.")
            sys.exit(1)
        if args.project_id and args.run_id is None:
            print("Action progress must have --project-id and --run-id parameter.")
            sys.exit(1)
        process_progress_request(provider_script_path,args.action,args.progress_id,args.progress_file,args.project_id,args.run_id,args.provider)
    '''
    sys.exit(0)