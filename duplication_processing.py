import requests

import argparse
import platform
import sys
from configparser import ConfigParser
import plistlib
import os
import time
import uuid
import json
import pathlib
from strsimpy.jaro_winkler import JaroWinkler
import dateutil.parser as dateparser
import datetime 
requests.packages.urllib3.disable_warnings()

DEBUG_PRINT = False
DEBUG_TO_FILE = False

def debug_print(str):
    if DEBUG_PRINT == False:
        return
    if DEBUG_TO_FILE == False:
        print(str)
    else:
        debug_file = open("/tmp/duplication.out", "a")
        debug_file.write(f'{datetime.datetime.now()} {str}\n')
        debug_file.close()

def get_api_url(hostname, port, no_ssl):

    if (no_ssl):
        protocol="http"
    else:
        protocol="https"
    
    if len(port) > 0:
        return f'{protocol}://{hostname}:{port}'
    else:
        return f'{protocol}://{hostname}'

scan_info = {}

def send_progress(totals, request_id):

    duration = (int(time.time()) - int(totals["duration"])) * 1000
    run_guid = totals["run_id"]
    job_id = totals["job_id"]
    progress_path = totals["progress_path"]

    num_files_scanned = totals["totalFiles"]
    num_bytes_scanned = totals["totalSize"]
    num_files_processed = totals["processedFiles"]
    num_bytes_processed = totals["processedBytes"]

    debug_print(f"Progres:: {num_files_processed} {num_files_scanned}")
    debug_print(f"Progres:: {num_bytes_processed} {num_bytes_scanned}")

    avg_bandwidth=232
    file = open(progress_path, "w")

    xml_str = f"""<update-job-progress duration="{duration}'" avg_bandwidth="{avg_bandwidth}">
      <progress jobid="{job_id}" cur_bandwidth="0" stguid="{run_guid}" requestid="{request_id}">
        <scanning>false</scanning>
        <scanned>{num_files_scanned}</scanned>
        <run-status>ANALYZING DUPLICATES</run-status>
        <quick-index>true</quick-index>
        <is_hyper>false</is_hyper>
        <session>
            <selected-files>{num_files_scanned}</selected-files>
            <selected-bytes>{num_bytes_scanned}</selected-bytes>
            <deleted-files>0</deleted-files>
            <processed-files>{num_files_processed}</processed-files>
            <processed-bytes>{num_bytes_processed}</processed-bytes>
        </session>
      </progress>
    <transfers>
    </transfers>
    <transferred/>
    <deleted/>
</update-job-progress>"""

    file.write(xml_str)
    file.close()
     
def get_duplication_settings():

    settings = {}

    is_linux=0
    if platform.system() == "Linux":
        DNA_CLIENT_SERVICES = '/etc/StorageDNA/DNAClientServices.conf'
        is_linux=1
    elif platform.system() == "Darwin":
        DNA_CLIENT_SERVICES = '/Library/Preferences/com.storagedna.DNAClientServices.plist'

    nodeAPIKey = ""
    duplicationSymLinkFolder = ""

    if is_linux == 1:
        config_parser = ConfigParser()
        config_parser.read(DNA_CLIENT_SERVICES)
        if config_parser.has_section('General') and config_parser.has_option('General','DuplicateSymLinkFolder'):
            section_info = config_parser['General']
            duplicationSymLinkFolder = section_info['DuplicateSymLinkFolder']
        if config_parser.has_section('General') and config_parser.has_option('General','NodeAPIKey'):
            section_info = config_parser['General']
            nodeAPIKey = section_info['NodeAPIKey']
    else:
        with open(DNA_CLIENT_SERVICES, 'rb') as fp:
            my_plist = plistlib.load(fp)
            duplicationSymLinkFolder = section_info['DuplicateSymLinkFolder']
            nodeAPIKey= section_info['NodeAPIKey']

    settings["DuplicateSymLinkFolder"] = duplicationSymLinkFolder
    settings["APIKey"] = nodeAPIKey

    #debug_print(settings)
    return settings
 

def conv_Bytes_to_GB(input_bytes):
        gigabyte = 1.0/1073741824
        convert_gb = gigabyte * input_bytes
        return float("{:.2f}".format(convert_gb))

def insert_duplicate_summary(api_url, repo_guid, run_guid, job_guid, run_date, totals, duplicate_type, fuzzy_value):

    summary_array = []

    summary_data = {}

    summary_data["projectName"] = repo_guid
    summary_data["runId"] = run_guid
    summary_data["jobId"] = job_guid
    json_data = json.dumps(dateparser.parse(run_date), default=serialize_datetime)
    summary_data["runDate"] = json_data.strip('\"')
    summary_data["totalFiles"] = totals["totalFiles"]
    summary_data["totalSizeInBytes"] = str(totals["totalSize"])
    summary_data["duplicateFiles"] = totals["duplicateFiles"]
    summary_data["duplicateSizeInBytes"] = str(totals["duplicateBytes"])
    summary_data["duplicationType"] = duplicate_type
    summary_data["fuzzyValue"] = fuzzy_value

    summary_array.append(summary_data)

    try:

        URL = api_url + "/duplicateAnalysis/summary"
        HEADERS = { 'version' : '2', 'X-Server-Select' : 'api', 'apikey': token }

        DATA = summary_array
        debug_print(URL)
        debug_print(HEADERS)
        debug_print(DATA)

        output = {}
        r = requests.post(url= URL, json=DATA, headers=HEADERS)
        debug_print(r)
        if r.status_code != 200:
             output["success"] = False
             output["data"] = r.text
             print(r.text)
        else:
            output["success"] = True
            output["data"]  =  r.json()

    except Exception as err:
       print('Error!')
       print(err)
       output["data"] = err

def serialize_datetime(obj): 
    if isinstance(obj, datetime.datetime): 
        return f'{obj.isoformat()}.000Z'

    raise TypeError("Type not serializable") 

def open_csv_file(given_filename):
    file_given = open(given_filename,"w")
    file_given.write("projectName.string(),runId.string(),jobGuid.string(),runDate.date(2006-01-02 15:04:05),totalFiles.auto(),totalSize.string(),checkSum.string() ,sizeInGB.string(),instances.auto(),fullSourcePath.string(),symLinkFullPath.string()"+ "\n")
    return file_given

def close_csv_file(file_given):
    file_given.close()

def append_to_csv_file(file_given, data_array, totals):
    for row in data_array:
        total_size = conv_Bytes_to_GB(totals["totalSize"])
        set_size = conv_Bytes_to_GB(totals["setSize"])
        file_given.write(f'{row["projectName"]},{row["runId"]},{row["jobGuid"]},"{row["runDate"]},{totals["totalFiles"]},{total_size},{row["checkSum"]},{set_size},{totals["setFiles"]},{row["fullSourcePath"]}\n')

MAX_UPLOAD_COUNT = 10000

def insert_current_set(api_url, the_set, set_hash, run_guid, job_guid, run_date, totals, file_given, token):
 
    output = {}    
    data_array = []
    num_files = len(the_set)
    num_bytes = 0
    file_counter = 0

    for item in the_set:
        new_item = {}
        new_item["projectName"] = item["projectName"]
        new_item["runId"] = run_guid
        new_item["jobGuid"] = job_guid
        json_data = json.dumps(dateparser.parse(run_date), default=serialize_datetime)
        new_item["runDate"] = json_data.strip('\"')
        new_item["checkSum"] = set_hash
        new_item["fileName"] = os.path.basename(item["fullPath"])
        new_item["fullSourcePath"] = os.path.dirname(item["fullPath"])
        new_item["totalFiles"] = totals["totalFiles"]
        new_item["totalSize"] = str(totals["setSize"])
        new_item["sizeInGB"] = str(conv_Bytes_to_GB(totals["setSize"]))
        new_item["fileSizeBytes"] = str(item["size"])
        new_item["instances"] = totals["setFiles"]
        new_item["modTime"] = item["modTime"]

        if "clip-name" in item:
            new_item["clipName"] = item["clip-name"]
        data_array.append(new_item)

        if file_counter >= MAX_UPLOAD_COUNT:
            result = call_insert_duplication_api(api_url, token, data_array, totals, file_given)
            if result["success"] != True:
                 debug_print("Insert duplication failed")
            data_array = []
            file_counter = 0
        else:
            file_counter = file_counter +1

    if file_counter > 0:
        result = call_insert_duplication_api(api_url, token, data_array, totals, file_given)
        if result["success"] != True:
            debug_print("Insert duplication failed")


def call_insert_duplication_api(api_url, token, data_array, totals, file_given):

    try:

        output = {}
        URL = api_url + '/duplicateAnalysis'
        HEADERS = { 'version' : '2', 'X-Server-Select' : 'api', 'apikey': token }
        DATA = data_array

        debug_print(URL)
        debug_print(str(len(data_array)))
        debug_print(HEADERS)
       # debug_print_body(DATA)

        r = requests.post(url= URL, json=DATA, headers=HEADERS)
        debug_print(r)
        if r.status_code != 200:
             output["success"] = False
             output["data"] = r.text
             print(r.text)
        else:
            output["success"] = True
            output["data"]  =  r.json()

        append_to_csv_file(file_given, data_array, totals)

    except Exception as err:
      print('Error!')
      print(err)
      output["data"] = err

    return output;


def generate_current_hash(current_file, duplication_type):
    if duplication_type == 'checksum':
        return current_file['checksum']
    else:
        return str(uuid.uuid4())

def get_catalog_file_key(current_file, duplication_type):

    if duplication_type == 'checksum':
        return current_file['checksum']

    elif duplication_type == 'fuzzy-file-name':
        return current_file['fileName']

    elif duplication_type == 'fuzzy-clip-name':
        return current_file["clip-name"];

    return ""

ROWS_PER_QUERY = 10000

def perform_duplicate_analysis(api_url, paths, token, base_url, args):

    fuzzy_value = args.fuzzy
    duplication_type = args.duplicate_type
    repo_guid = args.repoguid
    run_guid = args.runguid
    run_date = args.rundate
    job_guid = args.jobguid
    source_repo_guid = args.sourcerepoguid
    duration = time.time()
    jarowinkler = JaroWinkler()
  
    threshold = float(0.7) + (float(fuzzy_value) * 0.03)
    
    file_to_match = ""
    file_to_match_hash = ""

    current_record_index = 0
    total_records = -1
    total_size = -1
    total_files = 0
    current_set_size = 0
    current_set = []
    sorted_list = []
    duplicate_files = 0
    duplicate_bytes = 0
    totals = {}

    file_given = open_csv_file(args.csvfilepath)

    totals["duration"] = int(time.time())
    totals["run_id"] = args.runguid
    totals["job_id"] = args.jobid
    totals["progress_path"] = args.progresspath
    totals["processedBytes"] = 0
    totals["processedFiles"] = 0

    result = query_catalog_totals(base_url, source_repo_guid, token)
    if result["success"] == False:
        print("Unable to obtain total count: ")
        print(result)
        sys.exit(2)

    totals_dict = result["data"]
    totals["totalFiles"] = int(totals_dict["totalCount"])
    totals["totalSize"] = 0

    debug_print(len(sorted_list))
    done = False
    while not done:

         if len(sorted_list) == 0:

             # Get more records here...
 
             if current_record_index == total_records:
                 break

             result = query_catalog_entries(base_url, source_repo_guid, token, paths, current_record_index, ROWS_PER_QUERY, duplication_type)
             if result["success"] == False:
                 print(result["data"])
                 sys.exit(1)

             result_data = result["data"]
             sorted_list = result_data["catalogs"]

              # If there are no more records, then break
             if len(sorted_list) == 0:
                 break

         current_record_index = current_record_index+1
         catalog_file = sorted_list.pop(0)
         debug_print(catalog_file)
         if catalog_file["isDir"]:
              continue

         if catalog_file["size"] is None:
             catalog_file_size = 0
         else:
             catalog_file_size = int(catalog_file["size"])
              
         totals["processedFiles"] = totals["processedFiles"] + 1
         totals["processedBytes"] = totals["processedBytes"] + catalog_file_size
         totals["totalSize"] = totals["processedBytes"]

         if (current_record_index % 2500) == 0:
             send_progress(totals, current_record_index)

         if len(current_set) == 0 and len(file_to_match) == 0:
             current_set.append(catalog_file)
             current_set_size += catalog_file_size
             file_to_match = catalog_file
             file_to_match_hash = get_catalog_file_key(catalog_file, duplication_type)
             current_set_hash = generate_current_hash(catalog_file, duplication_type)

             debug_print("setting intial hash" + file_to_match_hash)
         else:
             if duplication_type == "checksum" or fuzzy_value == 10:
                 match_found = file_to_match_hash == get_catalog_file_key(catalog_file, duplication_type)
             else:
                 match_value = jarowinkler.similarity(file_to_match_hash, get_catalog_file_key(catalog_file, duplication_type))
                 match_found = (match_value >= threshold)
                 debug_print(f'match value: { match_value} {threshold} {file_to_match_hash} {get_catalog_file_key(catalog_file, duplication_type)} {match_found}')
             
             if match_found == False:
                  debug_print("match not found")
                  if len(current_set) > 1:
                       totals["setFiles"] = len(current_set)
                       totals["setSize"] = current_set_size
                       duplicate_files +=  totals["setFiles"]
                       duplicate_bytes += totals["setSize"]
                       insert_current_set(api_url, current_set, current_set_hash, run_guid, job_guid, run_date,  totals, file_given, token)
                       send_progress(totals, current_record_index)
                  current_set = []
                  current_set.append(catalog_file)
                  current_set_size = 0
                  file_to_match = catalog_file
                  file_to_match_hash = get_catalog_file_key(catalog_file, duplication_type)
                  debug_print("setting hash" + file_to_match_hash)
                  current_set_size += catalog_file_size
                  current_set_hash = generate_current_hash(catalog_file, duplication_type)
             else:
                 debug_print(f'match found: {get_catalog_file_key(catalog_file, duplication_type)}')
                 file_to_match = catalog_file
                 file_to_match_hash = get_catalog_file_key(catalog_file, duplication_type)
                 debug_print("setting hash" + file_to_match_hash)
                 current_set.append(catalog_file)
                 current_set_size += catalog_file_size

    if len(current_set) > 1:
         totals["setFiles"] = len(current_set)
         totals["setSize"] = current_set_size
         duplicate_files +=  totals["setFiles"]
         duplicate_bytes += totals["setSize"]
         send_progress(totals, current_record_index)
         insert_current_set(api_url, current_set, current_set_hash, run_guid, job_guid, run_date, totals, file_given, token)

    totals["duplicateFiles"] = duplicate_files
    totals["duplicateBytes"] = duplicate_bytes

    insert_duplicate_summary(api_url, repo_guid, run_guid, job_guid, run_date, totals, duplication_type, fuzzy_value)
    close_csv_file(file_given)
    print(f'{totals["totalFiles"]}:{totals["totalSize"]}:{totals["processedFiles"]}:{totals["processedBytes"]}')
  
def get_sort_field_by_type(dup_type):

     if dup_type == "checksum":
         return "checksum"
     elif dup_type == "fuzzy-file-name":
        return "fileName"
     elif dup_type == "fuzzy-clip-name":
        return "clip-name"
     else:
        debug_print("Illegal sort field provided: " + dup_type)
        exit(10)


def query_catalog_totals(api_url, repoguid, token):

    debug_print(str(f'query_catalog_totals: repoguid: {repoguid}, '))
       
    output = {}

    try:
        URL = api_url + f'/catalogs/count/{repoguid}'
        HEADERS = { 'version' : '2', 'X-Server-Select' : 'api', 'apikey': token }

        debug_print(URL)
        debug_print(HEADERS)	
        r = requests.get(url= URL,  verify=False, headers=HEADERS)
        debug_print(r)
        if r.status_code != 200: 
            output["success"] = False
            output["data"] = r.text
        else:
            output["success"] = True
            output["data"]  =  r.json()

    except Exception as err:
       debug_print("Exception")
       debug_print(err)
       output["data"] = err
       output["success"] = False

    return output


def query_catalog_entries(api_url, repoguid, token,  paths, current_index, rows_per, duplication_type):

    sort_field =  get_sort_field_by_type(duplication_type)
    debug_print(str(f'query start: {current_index} , rows per: {rows_per}, sort: {sort_field}, repoguid: {repoguid}, '))
       
    output = {}

    try:
        URL = api_url + '/catalogs/list'
        HEADERS = { 'version' : '2', 'X-Server-Select' : 'api', 'apikey': token }
        DATA = { "repoGuid" : repoguid , "sortField" : sort_field, "sortOrder" : 1 }
        DATA["browsePaths"] = paths
        DATA["startsFrom"] = current_index
        DATA["rowsPerPage"] = rows_per

        r = requests.post(url= URL, data=DATA, verify=False, headers=HEADERS)
        debug_print(r)
        if r.status_code != 200:
             output["success"] = False
             output["data"] = r.text
        else:
            output["success"] = True
            output["data"]  =  r.json()

    except Exception as err:
       debug_print("Exception")
       debug_print(err)
       output["data"] = err
       output["success"] = False

    return output

    
if __name__ == '__main__':

    start = time.time()
    parser = argparse.ArgumentParser()
    parser.add_argument('-r', '--repoguid', required = True, help = 'Action is either delete or update')
    parser.add_argument('--hostname', required = True, help = 'Action is either delete or update')
    parser.add_argument('--port', required = False, help = 'Action is either delete or update')
    parser.add_argument('--no-ssl', dest='nossl', action='store_true')
    parser.set_defaults(nossl=False)
    parser.add_argument('-f', '--fuzzy', required = True, help = 'Value to 1 to 10')
    parser.add_argument('-d', '--duplicate_type', required = True, help = 'Values are checksum, file-name, clip-name')
    parser.add_argument('-c', '--catalog_paths', required = False)
    parser.add_argument('-i', '--runguid', required = True)
    parser.add_argument('-j', '--jobguid', required = True)
    parser.add_argument('-s', '--sourcerepoguid', required = True)
    parser.add_argument('--rundate', required = True)
    parser.add_argument('--progresspath', required = True)
    parser.add_argument('--csvfilepath', required = True)
    parser.add_argument('--jobid', required = True)
    parser.add_argument('--duration', required = True)
    parser.add_argument('--symlinkpath' ,required=False)
    parser.add_argument('--debug', required=False)
  
    start = time.time()
 
    args = parser.parse_args()

    if not args.debug is None:
        DEBUG_PRINT = True

    if args.catalog_paths is not None:
        catalog_paths = args.catalog_paths
    else:
        catalog_paths= "/"

    port=""
    if args.port is not None:
        port = args.port

    settings = get_duplication_settings()
    if len(settings["APIKey"]) == 0:
        print("No API token found.")
        exit(2)

    token = settings["APIKey"]
    base_url = get_api_url(args.hostname, port,  args.nossl)
    perform_duplicate_analysis(base_url, catalog_paths, token, base_url, args)

    exit(0)

