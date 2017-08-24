'''
Created on Aug 23, 2017

Add key to kms-acls.cml KMS safety valve configuration

Function:
  1. read CM configuration from cm.config
      a. username and password prompted if not provided

@author: jamey
'''

import ConfigParser
from ConfigParser import ParsingError
from cm_api.api_client import ApiResource, ApiException

def getCMConfig(fileName):
    # test to see if file exists.  config.read does not return exception
    config = ConfigParser.ConfigParser()
    try:
        config.readfp(open(fileName))
    except ParsingError:
        print("could not parse " + fileName)
        return
    except:
        print("could not open " + fileName)
        return
    
    config.read(fileName)
    
    try:
        cm_host = config.get('CM_host', 'hostname')
    except:
        print( 'no value for [CM_host]\nhostname: <hostname> ')
        return
    
    try:
        cm_port = config.get('CM_host', 'port')
    except:
        print('no value for [CM_host]\nport: <port>')
    
    try:
        cm_username = config.get('CM_account', 'username')
        cm_password = config.get('CM_account', 'password')
    except:
        cm_username = None
        cm_password = None
        
    return cm_host, cm_port, cm_username, cm_password

def getApiResource(cm_host, cm_port, cm_username, cm_password):
    # get resourceAPI 
    return(ApiResource(
        cm_host, 
        username = cm_username, 
        password = cm_password, 
        version = VERSION))

def main():
    FILENAME = "cm.ini"
    cm_host, cm_port, cm_username, cm_password = getCMConfig(FILENAME)
    
    if cm_username == None or cm_password == None:
        print("no password")
    
#     api = getApiResource(cm_host, cm_port, cm_username, cm_password)

if __name__ == '__main__':
    main()