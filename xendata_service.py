import os
import uuid
import csv
import subprocess
import threading
import winreg
from flask import Flask, request, jsonify
import logging

app = Flask(__name__)
temp_output_dir = "C:\\temp"  # temporary output directory
os.makedirs(temp_output_dir,exist_ok=True)

logging.basicConfig(filename=os.path.join(temp_output_dir, 'service.log'), level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s')

def get_registry_port():
    try:
        reg_path = r"SOFTWARE\Test"  # Update to match your registry key path
        value_name = "Port"
        
        # Open the registry key for local machine
        with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, reg_path) as key:
            port, regtype = winreg.QueryValueEx(key, value_name)
            logging.info(f"Port found in registry: {port}")
            return port
    except FileNotFoundError:
        logging.error("Registry key or value not found.")
        return 8080  
    except Exception as e:
        logging.error(f"Error reading registry: {e}")
        return 8080  

def data_export(filter_value, path_value, recursive, output_file, status_file):
    powershell_script = "D:\\storageDNA\\FileExporter.ps1"
    command = [
        "powershell",
        "-ExecutionPolicy", "Bypass",
        "-File", powershell_script,
        "-filter", filter_value,
        "-p", path_value,
        "-o", output_file
    ]

    if recursive:
        command.append("-r")
    logging.info(f"Command to be executed: {command}")

    result = subprocess.run(command, capture_output=True, text=True)

    if result.returncode == 0:
        with open(status_file, 'w') as f:
            f.write('Success')
    else:
        with open(status_file, 'w') as f:
            f.write('Failed')
            
@app.route('/')
def index():
    return "Hello from StorageDNA Export Services Engine!",200

@app.route('/xen/')
def xen_info():
    return "XenData Export Services. Params include filter(string), path(relative string), recursive(boolean)",200

@app.route('/xen/export/', methods=['GET'])
def xen_export():
    filter_value = request.args.get('filter', '')
    path_value = request.args.get('path', '\\')
    recursive = request.args.get('recursive', 'false').lower() == 'true'
    req_uuid = str(uuid.uuid4())
    output_file = os.path.join(temp_output_dir, f"export-{req_uuid}.csv")
    status_file = os.path.join(temp_output_dir, f"status-{req_uuid}.txt")

    with open(status_file, 'w') as f:
        f.write('InProgress')
    export_thread = threading.Thread(
        target=data_export,
        args=(filter_value, path_value, recursive, output_file, status_file)
    )
    export_thread.start()

    data = {
        "filter": filter_value,
        "path": path_value,
        "recursive": recursive
    }
    return jsonify({'requestParams': data, 'requestId': req_uuid}), 200

@app.route('/xen/export/<req_uuid>/', methods=['GET'])
def xen_export_status(req_uuid):
    export_file_path = os.path.join(temp_output_dir, f"export-{req_uuid}.csv")
    status_file = os.path.join(temp_output_dir, f"status-{req_uuid}.txt")

    if os.path.exists(status_file):
        with open(status_file, 'r') as f:
            status = f.read().strip()
    else:
        status = "RequestNotFound"

    data = {
        'requestResults': [],
        'requestId': req_uuid,
        'requestStatus': status
    }

    if status == 'Success' and os.path.exists(export_file_path):
        with open(export_file_path, encoding='utf-8', errors='ignore') as csvfile:
            csvdata = csv.DictReader(csvfile, delimiter='|')
            data['requestResults'] = [row for row in csvdata]

    return jsonify(data), 200

if __name__ == '__main__':
    port = get_registry_port()
    logging.info(f"Starting Flask app on port {port}")
    app.run(host='0.0.0.0', port=port)
