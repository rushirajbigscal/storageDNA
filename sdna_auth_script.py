import os
import argparse
import requests
from configparser import ConfigParser
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def get_wildfly_cookie():
    url = f'https://{config_map["hostname"]}/login/wildfly'
    header = {
        "X-Server-Select":"api",
        "version":"2"
    }
    payload = {
    "username": config_map["username"],
    "password": config_map["password"],
    }
    
    response = requests.post(url,headers=header,data=payload,verify=False)
    if response.status_code == 200:
        response = response.json()
        return response["result"]

def get_auth_token():
    url = f'https://{config_map["hostname"]}/login'
    header = {
        "X-Server-Select":"api",
        "version":"2"
    }
    payload = {
    "username": config_map["username"],
    "password": config_map["password"],
    }
    
    response = requests.post(url,headers=header,data=payload,verify=False)
    if response.status_code == 200:
        response = response.json()
        return response["result"]
    
def get_all_jobs():
    url = f'https://{config_map["hostname"]}/wildfly/jobs'
    header = {
        "X-Server-Select":"api",
        "version":"2",
        "wildfly-cookie" : config_map["wildfly_cookie"],
        "authorization" : config_map["authorization"]
    }
    params = {"statusfilter":config_map["status_filter"]}
    response = requests.get(url,headers=header,params=params,verify=False)
    print(response.json())
    
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', required = True, help = 'Configuration file path')
    parser.add_argument('-s', '--status_filter', choices= ["all", "active", "failure"], default = "all", help = 'status_filter')
    
    args = parser.parse_args()
    
    cloudTargetPath = args.config
    config_name = "credentials"
    config_map = {}
    if not os.path.exists(cloudTargetPath):
        err= "Unable to find cloud target file: " + cloudTargetPath
        print(err)

    config_parser = ConfigParser()
    config_parser.read(cloudTargetPath)
    if not config_name in config_parser.sections():
        err = 'Unable to find cloud configuration: ' + config_name
        print(err)

    config_info = config_parser[config_name]
    for  key in config_info:
        config_map[key] = config_info[key]
    config_map["status_filter"] = args.status_filter
    config_map["wildfly_cookie"] = get_wildfly_cookie()
    config_map["authorization"] = get_auth_token()
    get_all_jobs()