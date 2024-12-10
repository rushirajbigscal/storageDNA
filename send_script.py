from flask import Flask, request, jsonify
import requests
import os
from action_functions import *  

app = Flask(__name__)

@app.route('/send', methods=['POST'])
def print_payload():
    try:
        payload = request.get_json()
        url = payload["downloadableFile"]
        filename = os.path.join('C:\\temp',get_filename(url))
        print(filename)
        response = requests.get(url)
        print(response.status_code)
        if response.status_code == 200:
            with open(filename, 'wb') as file:
                file.write(response.content)
            print(f"File downloaded at {filename}")
        else:
            print("Failed to download the file.")
        
        print("Request Payload:", payload)
        return jsonify({"message": "Payload received", "Data": payload}), 200
    
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return jsonify({"message": "An error occurred while processing the payload."}), 400

if __name__ == '__main__':
    app.run(host="0.0.0.0",port=5000)
