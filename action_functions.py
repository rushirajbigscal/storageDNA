import os
import json
import time
import xml.etree.ElementTree as ET
import glob

def generate_html(metadata, file_name):
    if isinstance(metadata, str):
        metadata = json.loads(metadata)

    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <style>
        pre {{
            white-space: pre-wrap;
        }}
    </style>
</head>
<body>
    <pre>
{json.dumps(metadata, indent=4)}
    </pre>
</body>
</html>"""

    with open(file_name, "w") as file:
        file.write(html_content)
    
    return file_name



def add_CDATA_tags(folders : list):
    xml_output = '<FOLDERLIST>\n'
    for folder in folders:
        xml_output += f'<FOLDER><![CDATA[{folder}]]></FOLDER>\n'
    xml_output += '</FOLDERLIST>'
    return xml_output


def add_CDATA_tags_with_id(folders : list):
    xml_output = '<FOLDERLIST>\n'
    for folder in folders:
        xml_output += f'<FOLDER ID="{folder["id"]}",ACCESS="1",TYPE="NORMAL"><![CDATA[{folder["name"]}]]></FOLDER>\n'
    xml_output += '</FOLDERLIST>'
    return xml_output


def get_filename(filePath : str):
    filePath = os.path.normpath(filePath)
    return filePath.split("\\")[-1]


def isMatch(s, p):
    m, n = len(s), len(p)

    # Create a 2D DP table to store matching information
    dp = [[False] * (n + 1) for _ in range(m + 1)]

    # Empty pattern matches empty string
    dp[0][0] = True

    # Fill the first row of the DP table (when s is empty)
    for j in range(1, n + 1):
        if p[j - 1] == '*':
            dp[0][j] = dp[0][j - 1]

    # Fill the DP table using bottom-up dynamic programming
    for i in range(1, m + 1):
        for j in range(1, n + 1):
            if p[j - 1] == '*':
                dp[i][j] = dp[i - 1][j] or dp[i][j - 1]
            elif p[j - 1] == '?' or p[j - 1] == s[i - 1]:
                dp[i][j] = dp[i - 1][j - 1]

    return dp[m][n]


def isFilenameInFilterList(filename, filter_list):
    for filter in filter_list:
        if isMatch(filename, filter):
            return True
    return False


def get_projectid(filename):
    projectid  = filename.split("/")[-1]
    return projectid.split(".")[0]


def get_runid(filename):
    runid  = filename.split("/")[-1]
    return runid.split(".")[1]
    

def load_settings(provider):
    settings_file = f"{provider}.settings.json"
    if os.path.isfile(settings_file):
        with open(settings_file, 'r') as f:
            return json.load(f)
    else:
        print(f"Settings file {settings_file} not found.")
        return None


def send_response(handler, status, state, message):
    handler.send_response(status)
    handler.send_header('Content-type', 'application/json')
    handler.end_headers()
    response = {
        "status": status,
        "statusState": state,
        "message": message
    }
    handler.wfile.write(json.dumps(response).encode())

    
def load_policies_from_file(policy_filename):

    output = {}

    if len(policy_filename) == 0:
        output["type"] = "NOFILE"
        output["entries"] = []
        output["result"] = "SUCCESS"
        return output
    
    elif not os.path.isfile(policy_filename):
         output["type"] = "ERROR"
         output["entries"] = []
         output["result"] = "ERROR"
         return output

    policy_type = ""
    policy_entries = []

    try:
        policy_file = open(policy_filename)
        lines = policy_file.readlines()
        policy_file.close()

        for line in lines:
            if line.startswith("Type:"):
               policy_type = line.strip().split(":").pop()
               continue
            else:
               policy_parts = line.strip().split("|")

               if len(policy_parts) < 3:
                    continue  #invalid entry

               policy_entry = {}
               policy_entry["object"] = policy_parts.pop(0)
               policy_entry["verb"] = policy_parts.pop(0)
               policy_entry["value"] = "|".join(policy_parts)
               policy_entries.append(policy_entry)
    except:
        
        print("Unable to parse policies")
        policy_file.close()
        output["entries"] = []
        output["type"] = "ERROR"
        output["result"] = "ERROR"
        return output   
    
    output["entries"] =  policy_entries
    output["type"] = policy_type
    output["result"] = "SUCCESS"
    return output


def file_in_policy(policy_map, file_name, file_parent_path, file_size, mtime_epoch_seconds):

    policy_type = policy_map["type"]
    policy_entries = policy_map["entries"]

    # open the policy file and read in the contentws

    try:
        
        lastPolicyResult  = None

        for policy in policy_entries:

            if policy_type == 'ANY' and lastPolicyResult == True:
                return True
            elif policy_type == 'ALL' and lastPolicyResult == False:
                return False
            
            if policy["object"] == "filename" and policy["verb"] == "startwith":
                lastPolicyResult = file_name.startswith(policy["value"])
            elif policy["object"] == "filename" and policy["verb"] == "endswith":
                lastPolicyResult = file_name.endswith(policy["value"])
            elif policy["object"] == "filename" and policy["verb"] == "contains":
                lastPolicyResult = policy["value"] in file_name
            elif policy["object"] == "filename" and policy["verb"] == "matches":
                lastPolicyResult = isMatch(file_name, policy["value"])
            if policy["object"] == "filename" and policy["verb"] == "doesnotmatch":
                lastPolicyResult = not isMatch(file_name, policy["value"])
            if policy["object"] == "filepath" and policy["verb"] == "contains":
               lastPolicyResult = policy["value"] in file_parent_path
            if policy["object"] == "filepath" and policy["verb"] == "matches":
                lastPolicyResult = isMatch(file_parent_path, policy["value"])
            if policy["object"] == "filepath" and policy["verb"] == "doesnotmatch":
                lastPolicyResult = not isMatch(file_parent_path, policy["value"])
            if policy["object"] == "size" and policy["verb"] == "morethan":
                lastPolicyResult = int(file_size) > int(policy["value"])
            if policy["object"] == "size" and policy["verb"] == "lessthan":
                lastPolicyResult = int(file_size) < int(policy["value"])
            if policy["object"] == "mtime" and policy["verb"] == "morethan":
                lastPolicyResult = int(mtime_epoch_seconds) > int(policy["value"])
            if policy["object"] == "mtime" and policy["verb"] == "lessthan":
                lastPolicyResult = int(mtime_epoch_seconds) < int(policy["value"])

        return policy_type == 'ALL'

    except:
        
        return False


def generate_xml_from_file_objects(parse_result, xml_file):
    scanned_count = parse_result.get("scanned_count")
    selected_count = parse_result.get("selected_count")
    total_size = parse_result.get("total_size")
    root = ET.Element("files", scanned=str(scanned_count), selected=str(selected_count), size=str(total_size), bad_dir_count="0", delete_count="0", delete_folder_count="0")

    for file_info in parse_result["filelist"]:
        file_element = ET.SubElement(root, "file")
        for attr_name, attr_value in file_info.items():
            file_element.set(attr_name, str(attr_value))
    
    tree = ET.ElementTree(root)
    tree.write(xml_file)


def generate_xml(filename,xml_output):
    with open(filename, 'w') as f:
        f.write(xml_output)


def symbolic_to_hex(symbolic):
    permissions = {
        'r': 4,
        'w': 2,
        'x': 1,
        '-': 0
    }

    owner_perms = symbolic[1:4]
    group_perms = symbolic[4:7]
    other_perms = symbolic[7:10]

    def convert_to_hex(perm_str):
        val = 0
        for char in perm_str:
            val += permissions[char]
        return hex(val)[-1]

    return '0x' + ''.join([convert_to_hex(owner_perms), convert_to_hex(group_perms), convert_to_hex(other_perms)])


def send_progress(progressDetails, request_id):
    duration = (int(time.time()) - int(progressDetails["duration"])) * 1000
    run_guid = progressDetails["run_id"]
    job_id = progressDetails["job_id"]
    progress_path = progressDetails["progress_path"]

    num_files_scanned = progressDetails["totalFiles"]
    num_bytes_scanned = progressDetails["totalSize"]
    num_files_processed = progressDetails["processedFiles"]
    num_bytes_processed = progressDetails["processedBytes"]
    status = progressDetails["status"]

    avg_bandwidth=232
    file = open(progress_path, "w")

    xml_str = f"""<update-job-progress duration="{duration}'" avg_bandwidth="{avg_bandwidth}">
        <progress jobid="{job_id}" cur_bandwidth="0" stguid="{run_guid}" requestid="{request_id}">
            <scanning>false</scanning>
            <scanned>{num_files_scanned}</scanned>
            <run-status>{status}</run-status>
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
    return


def restore_ticket_to_csv(ticket_path, current_time):
    csv_file = f'{ticket_path}/sdna-Restore-{current_time}.csv'
    file = open(csv_file, "w")
    for xml_ticket in glob.glob(f'{ticket_path}/*-converted.xml'): 
        tree = ET.parse(xml_ticket)
        root = tree.getroot()
        target_path = ""
        for path in root.iter('target-path'):
            target_path = path.text
            break
        for child in root:
            file_attributes = child.attrib 
            index_id = file_attributes["headers"]
            file.write(f"{index_id},{target_path}\n")
    file.close()
    return csv_file


# def restore_ticket_to_csv(xml_ticket):
#     csv_file = xml_ticket.replace(".xml",".csv")
#     tree = ET.parse(xml_ticket)
#     root = tree.getroot()
#     target_path = ""
#     for path in root.iter('target-path'):
#         target_path = path.text
#         break
#     #index_ids_list = []
#     file = open(csv_file, "w")
#     for child in root:
#         file_attributes = child.attrib 
#         index_id = file_attributes["headers"]
#         #index_ids_list.append(index_id)
#         file.write(f"{index_id},{target_path}\n")

#     file.close()
#     return csv_file
