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

ToDo:
1. pass dictionaries between functions
2. try to get password from environment.  Scope
  a. config.ini
  b. environment CM_USER, CM_PASSWORD
  c. command-line arguments
3. change command 
'''

from ConfigParser import ConfigParser, ParsingError
from cm_api.api_client import ApiResource, ApiException
from sys import argv, exit
from getpass import getpass
import argparse
import os

def getCMConfig(fileName):
    '''
    Get all program parameters
    most come from FILENAME ini (configuration) file
    
    cm_user, cm_password and new_org are determined, in order of precedence
    NOTE:  ini file is mandatory, even if command-line or environment variables 
        are used
    1. command-line argument(s)
       --cm_user
       --cm_password
       --new_cm_org
    2. configuration file
    3. OS environment variable(s)
       $CM_USER
       $CM_PASSWORD
       $NEW_CM_ORG
    4. prompt for username and password
    '''
    def readConfigFile(fileName):
        my_cm_config = {}
        # Read configuration file    
        config = ConfigParser()

        ''''Python doc recommended way to test for existence of config file'''
        try:
            config.readfp(open(fileName))
        except ParsingError:
            print("could not parse " + fileName)
            exit()
        except:
            print("could not open " + fileName)
            exit()
        
        config.read(fileName)
        
        '''Config file read here'''
        try:
            my_cm_config['cm_host'] = config.get('CM_host', 'hostname')
        except:
            print( 'no value for [CM_host]\nhostname: <hostname>')
            exit()
        
        try:
            my_cm_config['cm_port'] = config.get('CM_host', 'port')
        except:
            print('no value for [CM_port]\nport: <port>')
            exit()
            
        try:
            my_cm_config['cluster_name'] = config.get('CM_cluster', 'cluster_name')
        except:
            print('no value for [CM_cluster]\ncluster_name: <cluster>')
            exit()
        
        try:
            my_cm_config['cm_user'] = config.get('CM_account', 'cm_user')
            my_cm_config['cm_pass'] = config.get('CM_account', 'cm_password')
        except:
            my_cm_config['cm_user'] = None
            my_cm_config['cm_pass'] = None
            
        try:
            cluster_name = config.get('CM_cluster', 'cluster_name')
        except:
            print('no value for [CM_cluster]\ncluster_name: <port>')
            
        return my_cm_config
    
    def get_commandline_args():
        """Read in command line arguments
        --new_org is mandatory
        --cm_user and --cm_pass are optional"""
        
        parser=argparse.ArgumentParser()
        
        parser.add_argument('--new_org', help='New organization to add')
        parser.add_argument('--cm_user', help='CM username')
        parser.add_argument('--cm_pass', help='CM password')
        args = parser.parse_args()
        
        return(args.new_org, args.cm_user, args.cm_pass)
    
    def get_environment_variables():
        #return(os.environ['CM_USER'], os.environ['CM_PASS'])
        
        try:
            env_cm_user = os.environ['CM_USER']
            env_cm_pass = os.environ['CM_PASS']
        except:
            return(None, None)
        
        return('env_user', 'env_pass')
    
    def prompt_user_pass():
        """No cm_user and cm_pass provided in ini, cli or env
        therefore prompt for values"""
        prompt_cm_user = raw_input('Enter the CM username: ')
        prompt_cm_pass = getpass('Enter the CM password: ')
        
        return(prompt_cm_user, prompt_cm_pass)
    
    # get values from configuration file
    my_cm_config = readConfigFile(fileName)
    
    # get command-line arguments
    new_org, cli_cm_user, cli_cm_pass = get_commandline_args()
    if new_org == None:
        print("""
        No new organization provided.
        Pass the new organization as an argument '--new_org <new_org>'
        """)
        exit()
    my_cm_config['new_org'] = new_org
    
    # get environment variables
    env_cm_user, env_cm_pass = get_environment_variables()

    ''' establish precedence for cm_user, cm_pass
    determine if a prompt is needed'''
    
    if (cli_cm_user != None and cli_cm_pass != None):
        # check CLI for user and pass
        my_cm_config['cm_user'] = cli_cm_user
        my_cm_config['cm_pass'] = cli_cm_pass
    elif (my_cm_config['cm_user'] != None and my_cm_config['cm_pass'] != None):
        # check the ini for user and pass
        pass
    elif (env_cm_user != None and env_cm_pass != None):
        # check the environment for user and pass
        my_cm_config['cm_user'] = env_cm_user
        my_cm_config['cm_pass'] = env_cm_pass
    else:
        my_cm_config['cm_user'], my_cm_config['cm_pass'] = prompt_user_pass()

    return(my_cm_config)

def getApiResource(cm_config):
    """
    Accepts CM connection, username and password and returns
    CM_API ApiResource
    """
    
    try:
        return(ApiResource(
            cm_config['cm_host'], 
            username = cm_config['cm_user'], 
            password = cm_config['cm_pass'], 
            version = cm_config['cm_version']))
    except ApiException:
        print('unable to connect to CM.  Check username / password')
        exit()
    except:
        print('other error in getApiResource')
        exit()

def getCluster(api_resource, cluster_name):
    """
    Returns the CM_API cluster object matching the passed name
    """
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
    """
    Returns the Java KeyStore KMS service in the passed cluster object 
    """
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
    """
    Get the original (before update) kms-acl.xml value from the passed KMS service
    """
    kms_acl_xml_name = 'kms-acls.xml_role_safety_valve'
    for kms_rcg in kms_service.get_all_role_config_groups():
        try:
            return kms_rcg.get_config()[kms_acl_xml_name]
        except:
            print('could not find ' + kms_acl_xml_name)
            exit()
            
def getGroupName():
    """
    Get the ACL group name command-line argument
    This group name is added to the kms-acl.xml
    """
    try:
        group_name = argv[1]
        return(group_name)
    except:
        print('no group name command-line argument')
        exit()

def genNewProperties(new_acl_group):
    """
    Generate the kms-acl.xml snippet for the two properties created
    based on the group name
    """
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
    """
    Update the KMS service kms-acl.xml property with the new XML snippet
    containing the new group
    """
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
    """
    After the kms-acm.xml configuration has been updated, deploy client
    configuration and restart stale services
    """
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
    CM_VERSION = 15
    FILENAME = 'cm.ini'
    kms_acl_xml_name = 'kms-acls.xml_role_safety_valve'
    
    my_cm_config = getCMConfig(FILENAME)
    
    print(my_cm_config) # jph
    
    my_cm_config['cm_version'] = CM_VERSION
    
    api_resource = getApiResource(my_cm_config)
   
    my_cluster = getCluster(api_resource, my_cm_config['cluster_name'])
 
    kms_service = getKMSService(my_cluster)
    
    kms_acl_xml = getKMS_ACL_XML(kms_service)
     
    new_kms_acl_properties = genNewProperties(my_cm_config['new_org'])
    
    updated_kms_acl_xml = kms_acl_xml + new_kms_acl_properties
     
    new_kms_acl_xml_dict = {kms_acl_xml_name: updated_kms_acl_xml}
     
    updateKmsAclXML(kms_service, new_kms_acl_xml_dict) 
    
    print('updated kms_acls.xml with ' + my_cm_config['new_org'])
    
    deployClientConfig(my_cluster)
    
    print('re-deployed client configuration and restarted +\n' + 
          '\tstale services.')
    
    print( os.path.basename(__file__) + ' completed.\nWait for stale services.')

if __name__ == '__main__':
    main()