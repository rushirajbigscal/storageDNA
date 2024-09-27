import csv
import os
import shutil
import argparse
import json
import fnmatch
import time
import dateutil.parser as dateparser
from datetime import datetime

def open_csv_file(given_filename):
    file_given = open(given_filename,"w")
    file_given.write("projectName.string(),runId.string(),jobGuid.string(),runDate.date(2006-01-02 15:04:05),fileName.string(),fileSize.string()"+ "\n")
    return file_given

def close_csv_file(file_given):
    file_given.close()

def append_to_csv_file(file_given, data):
    file_given.write(data + '\n')


def send_progress(totals, request_id):

    duration = (int(time.time()) - int(totals["duration"])) * 1000
    run_guid = totals["run_id"]
    job_id = totals["job_id"]
    progress_path = totals["progress_path"]

    num_files_scanned = totals["totalFiles"]
    num_bytes_scanned = totals["totalSize"]
    num_files_processed = totals["processedFiles"]
    num_bytes_processed = totals["processedBytes"]

    # print(f"Progress:: {num_files_processed} {num_files_scanned}")
    # print(f"Progress:: {num_bytes_processed} {num_bytes_scanned}")

    avg_bandwidth=232
    file = open(progress_path, "w")

    xml_str = f"""<update-job-progress duration="{duration}" avg_bandwidth="{avg_bandwidth}">
      <progress jobid="{job_id}" cur_bandwidth="0" stguid="{run_guid}" requestid="{request_id}">
        <scanning>false</scanning>
        <scanned>{num_files_scanned}</scanned>
        <run-status>ANALYZING DELETE AND MOVE FILES</run-status>
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

def serialize_datetime(obj): 
    if isinstance(obj, datetime): 
        return f'{obj.isoformat()}.000Z'

def process_files(args):
    run_date = args.rundate
    json_data = json.dumps(dateparser.parse(run_date), default=serialize_datetime)
    runDate = json_data.strip('\"')
    with open(args.optionsfilepath, 'r') as f:
        data = json.load(f)

    action = data["action"]
    destination_folder = data["destination_folder"]
    dry_run = data["dry_run"]
    filename_filter = data["filename_filter"]
    filepath_filter = data["filepath_filter"]
    filesize_filter = data["filesize_filter"]
    filesize = data["filesize"]

    totals = {}
    totals["duration"] = int(time.time())
    totals["run_id"] = args.runguid
    totals["job_id"] = args.jobid
    totals["progress_path"] = args.progresspath
    totals["processedBytes"] = 0
    totals["processedFiles"] = 0
    file_given = open_csv_file(args.csvfilepath)

    try:
        # Read the list of files from the CSV file
        with open(args.csv_file, newline='') as file:
            reader = csv.DictReader(file)
            file_list = list(reader)

        filtered_file_list = []


        for file_info in file_list:
            file_size_bytes = int(file_info['File Size Bytes'])
            if filename_filter and not fnmatch.fnmatch(file_info['File Name'], filename_filter):
                continue
            if filepath_filter and not fnmatch.fnmatch(file_info['Full Source Path'], filepath_filter):
                continue

            if filesize_filter:
                if filesize_filter == 'greater' and file_size_bytes <= filesize:
                    continue
                elif filesize_filter == 'less' and file_size_bytes >= filesize:
                    continue
                elif filesize_filter == 'equal' and file_size_bytes != filesize:
                    continue
            filtered_file_list.append(file_info)

        totals["totalFiles"] = len(filtered_file_list)
        totals["totalSize"] = 0
        current_record_index = 0

        for file_info in filtered_file_list:
            file_path = os.path.join(file_info['Full Source Path'], file_info['File Name'])
            if file_path.startswith('/'):
                file_path = file_path[1:]
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            if not os.path.exists(file_path):
                error_msg = f"File not found: {file_path}"
                print(error_msg)

            if action == 'delete':
                if dry_run:
                    print(f"Dry Run this file will be deleted: {file_path}")
                    data = f'{file_info["Project Name"]},{totals["run_id"]},{totals["job_id"]},{runDate},{file_info['File Name']},{file_info['File Size Bytes']}'
                    append_to_csv_file(file_given,data)
                else:
                    try:
                        os.remove(file_path)
                        data = f'{file_info["Project Name"]},{totals["run_id"]},{totals["job_id"]},{runDate},{file_info['File Name']},{file_info['File Size Bytes']}'
                        append_to_csv_file(file_given,data)
                        print(f"This File Deleted: {file_path} at time {timestamp}")
                    except Exception as e:
                        error_msg = f"Error deleting {file_path}: {e}"
                        print(error_msg)

            elif action == 'move':
                if destination_folder:
                    if dry_run:
                        print(f"Dry Run this file will be moved from: {file_path} to {destination_folder}")
                        data = f'{file_info["Project Name"]},{totals["run_id"]},{totals["job_id"]},{runDate},{file_info['File Name']},{file_info['File Size Bytes']}'
                        append_to_csv_file(file_given,data)
                    else:
                        try:
                            os.makedirs(destination_folder, exist_ok=True)
                            shutil.move(file_path, destination_folder)
                            data = f'{file_info["Project Name"]},{totals["run_id"]},{totals["job_id"]},{runDate},{file_info['File Name']},{file_info['File Size Bytes']}'
                            append_to_csv_file(file_given,data)
                            print(f"File Moved: {file_path} to {destination_folder} at time {timestamp}")
                        except Exception as e:
                            error_msg = f"Error moving {file_path} to {destination_folder}: {e}"
                            print(error_msg)
                else:
                    print("Destination folder path is required for move file.")

            current_record_index = current_record_index+1
            totals["processedFiles"] = totals["processedFiles"] + 1
            totals["processedBytes"] = totals["processedBytes"] + file_size_bytes
            totals["totalSize"] = totals["processedBytes"]

            if (current_record_index % 2500) == 0:
                send_progress(totals, current_record_index)
                
        send_progress(totals, current_record_index)
        close_csv_file(file_given)
        print(f'{totals["totalFiles"]}:{totals["totalSize"]}:{totals["processedFiles"]}:{totals["processedBytes"]}')
    
    except Exception as e:
        error_msg = f"An unexpected error occurred: {e}"
        print(error_msg)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process files based on a CSV input.')
    parser.add_argument('--csv_file', required= True, help='CSV file path')
    parser.add_argument('-o','--optionsfilepath', required = True)
    parser.add_argument('-r', '--repoguid', required = True, help = 'Action is either delete or update')
    parser.add_argument('-i', '--runguid', required = True)
    parser.add_argument('-j', '--jobguid', required = True)
    parser.add_argument('--rundate', required = True)
    parser.add_argument('--progresspath', required = True)
    parser.add_argument('--csvfilepath', required = True)
    parser.add_argument('--jobid', required = True)
    parser.add_argument('--duration', required = True)

    args = parser.parse_args()
    process_files(args)
    exit(0)

