'''
Created on Aug 23, 2017

Add key to kms-acls.cml KMS safety valve configuration

Function:
  1. read CM configuration from cm.config
      a. username and password prompted if not provided
      
Ref: https://cloudera.github.io/cm_api/docs/python-client/
     https://cloudera.github.io/cm_api/epydoc/5.12.0/index.html
     http://blog.cloudera.com/blog/2012/09/automating-your-cluster-with-cloudera-manager-api/
     https://github.com/cloudera/cm_api/tree/master/python      

@author: jamey
'''

import ConfigParser
from ConfigParser import ParsingError
from cm_api.api_client import ApiResource, ApiException
from sys import argv, exit
from getpass import getpass

def getCMConfig(fileName):
    def promptForPassword():
        # if password is not in configuration file
        cm_username = raw_input('Enter CM username: ')
        cm_password = getpass('Enter CM password: ')
        return(cm_username, cm_password)
    
    # test to see if file exists.  config.read does not return exception
    config = ConfigParser.ConfigParser()
    try:
        config.readfp(open(fileName))
    except ParsingError:
        print("could not parse " + fileName)
        exit()
    except:
        print("could not open " + fileName)
        exit()
    
    config.read(fileName)
    
    try:
        cm_host = config.get('CM_host', 'hostname')
    except:
        print( 'no value for [CM_host]\nhostname: <hostname> ')
        exit()
    
    try:
        cm_port = config.get('CM_host', 'port')
    except:
        print('no value for [CM_host]\nport: <port>')
        exit()
    
    try:
        cm_username = config.get('CM_account', 'username')
        cm_password = config.get('CM_account', 'password')
    except:
        cm_username = None
        cm_password = None
        
    if (cm_username == None or cm_password == None):
        cm_username, cm_password = promptForPassword()
        
    try:
        cluster_name = config.get('CM_cluster', 'cluster_name')
    except:
        print('no value for [CM_cluster]\ncluster_name: <port>')
        
    return cm_host, cm_port, cm_username, cm_password, cluster_name

def getApiResource(cm_host, cm_port, cm_username, cm_password, cm_version):
    # get resourceAPI 
    
    try:
        return(ApiResource(
            cm_host, 
            username = cm_username, 
            password = cm_password, 
            version = cm_version))
    except ApiException:
        print('unable to connect to CM.  Check username / password')
        exit()
    except:
        print('other error in getApiResource')
        exit()

def getCluster(api_resource, cluster_name):
    my_cluster = 'not_found'
    
    try:
        for cluster in api_resource.get_all_clusters():
            if cluster.name == cluster_name:
                my_cluster = cluster
    except ApiException:
        print('ApiException in getCluster.  Check credentials')
        exit()
    except:
        print('other exception in getCluster')
        exit()
    
    if my_cluster == 'not_found':
        print ('found these clusters:')
        for cluster in api_resource.get_all_clusters():
            print('\t' + cluster.name)
        print ('\nbut could not find: ' + cluster_name)
        exit()
    else:
        return my_cluster

def getKMSService(my_cluster):
    KMS_SERVICE_TYPE = 'KMS'
    kms_service = 'not_found'
    
    for service in my_cluster.get_all_services():
        if service.type == KMS_SERVICE_TYPE:
            kms_service = service
    
    if kms_service == 'not_found':
        print('no KMS service found in cluster' + my_cluster.name)
        exit()
    else:
        return(kms_service)

def getKMS_ACL_XML(kms_service):
    kms_acl_xml_name = 'kms-acls.xml_role_safety_valve'
    for kms_rcg in kms_service.get_all_role_config_groups():
        try:
            return kms_rcg.get_config()[kms_acl_xml_name]
        except:
            print('could not find ' + kms_acl_xml_name)
            exit()
            
def getGroupName():
    try:
        group_name = argv[1]
        return(group_name)
    except:
        print('no group name command-line argument')
        exit()

def genNewProperties(new_acl_group):
    new_kms_acl_properties = ('<property><name>key.acl.' +
                           new_acl_group + '_key.READ' + '</name>' +
                           '<value>' + new_acl_group + ' ' + new_acl_group +
                           '</value></property>' + 
                           '<property><name>key.acl.' + 
                           new_acl_group + '_key.DECRYPT_EEK' + '</name>' + 
                           '<value>' + new_acl_group + ' ' + new_acl_group +
                           '</value></property>')
    return(new_kms_acl_properties)

def updateKmsAclXML(kms_service, new_kms_acl_xml_dict):
    for kms_rcg in kms_service.get_all_role_config_groups():
        try:
            kms_rcg.update_config(new_kms_acl_xml_dict)
            return
        except ApiException:
            print('failure in updating kms-acls.xml')
            exit()
        except:
            print('other failure in updateKmsAclXML')

def deployClientConfig(my_cluster):
    try:
        my_cluster.restart(restart_only_stale_services = True, 
                           redeploy_client_configuration = True)
    except ApiException:
        print('failure in redeploying client configuration \n' +
               '\tand restarting stale services')
        exit()
    except:
        print('other failure in deployClientConfig')

def main():
    FILENAME = 'cm.ini'
    CM_VERSION = 15
    kms_acl_xml_name = 'kms-acls.xml_role_safety_valve'
    
    cm_host, cm_port, cm_username, cm_password, cluster_name = getCMConfig(FILENAME)
    api_resource = getApiResource(cm_host, cm_port, cm_username, cm_password, CM_VERSION)
    my_cluster = getCluster(api_resource, cluster_name)
    print('C')
    kms_service = getKMSService(my_cluster)
    print('D')
    kms_acl_xml = getKMS_ACL_XML(kms_service)
    new_acl_group = getGroupName()
    new_kms_acl_properties = genNewProperties(new_acl_group)
    updated_kms_acl_xml = kms_acl_xml + new_kms_acl_properties
    new_kms_acl_xml_dict = {kms_acl_xml_name: updated_kms_acl_xml}
    updateKmsAclXML(kms_service, new_kms_acl_xml_dict) 
    print('updated kms_acls.xml with ' + new_acl_group)
    deployClientConfig(my_cluster)
    print('re-deployed client configuration and restarted +\n' + 
          '\tstale services.')
    print('add_kms_acl_key.py done')

if __name__ == '__main__':
    main()