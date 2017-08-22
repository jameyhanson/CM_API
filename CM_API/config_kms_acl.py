'''
Created on Aug 20, 2017

Update Key Management Server safety valve for kms-acls.

@author: jamey

Ref: https://cloudera.github.io/cm_api/docs/python-client/
     https://cloudera.github.io/cm_api/epydoc/5.12.0/index.html
     http://blog.cloudera.com/blog/2012/09/automating-your-cluster-with-cloudera-manager-api/
     https://github.com/cloudera/cm_api/tree/master/python
'''

from cm_api.api_client import ApiResource, ApiException
from time import sleep

def getApiResource():
    HOSTNAME = 'jamey-5-10-1.gce.cloudera.com'
    USERNAME = 'api_test'
    PASSWORD = 'test'
    VERSION = 15

    # get resourceAPI 
    return(ApiResource(
        HOSTNAME, 
        username = USERNAME, 
        password = PASSWORD, 
        version = VERSION))

def updateConfig(cluster, service_type, config_dict, deploy_client_configs = False):
            
    for service in cluster.get_all_services():
        if service.type == service_type:
                my_service = service
            
                for my_rcg in my_service.get_all_role_config_groups():
                    my_rcg.update_config(config_dict)
                    print ('updated' + str(config_dict))
                    
        if deploy_client_configs:
            # deploy cluster client configuration if specified
            cluster.deploy_client_config()
            cluster.restart(restart_only_stale_services = True, redeploy_client_configuration = True)
            print ('deployed client configuration')
    return

def getKmsAcls (filename):
    CONFIG = 'kms-acls.xml_role_safety_valve'
    file = open(filename, 'r')
    return {CONFIG: file.read()}
    
def main():
    SERVICE_TYPE = 'KMS'
    KMS_ACLS_XML = 'kms-acls.xml'
    
    config_dict = getKmsAcls(KMS_ACLS_XML) 
    
    api = getApiResource()
    
    # get Cloudera Management Service
    cm = api.get_cloudera_manager()
    cms = cm.get_service()

    # loop through the all clusters 
    for cluster in api.get_all_clusters(view = "full"):
        print (cluster.name + '\t' + 
               cluster.version + '\t' + 
               cluster.fullVersion)
        updateConfig(cluster, SERVICE_TYPE, config_dict)     
    
if __name__ == '__main__':
    main()
