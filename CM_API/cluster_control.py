'''
Created on Aug 20, 2017

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

def controlCM(api, cm_action = None):
    # Start or stop Cloudera Management Service
    cm = api.get_cloudera_manager()
    cms = cm.get_service()

    wait_time = 0
    sleep_interval = 5    
    if cm_action.lower() == 'start':
        cm_endstate = 'STARTED'
        print('starting CM')
        cms.start()
        while cms.serviceState != cm_endstate:
            sleep(sleep_interval)
            wait_time += sleep_interval
            cms = cm.get_service()
            print (str(wait_time) + '\tstarting\t' + cms.serviceState)
        print ('CM started')
        return
    elif cm_action.lower() == 'stop':
        cm_endstate = 'STOPPED'
        print('stopping CM')
        cms.stop()
        while cms.serviceState != cm_endstate:
            sleep(5)
            wait_time += 5
            cms = cm.get_service()    
            print (str(wait_time) + '\tstopping\t' + cms.serviceState)
        print ('CM stopped')
        return
    elif cm_action == None:
        if cms.serviceState == 'STOPPED':
            controlCM(api, 'start')
        elif cms.serviceState == 'STARTED':
            controlCM(api, 'stop')
        else:
            print ('CM is neither STARTED nor STOPPED.  Please check and re-try')
            return
    else:
        print ('cm_action must be start, stop or None')
        return
    
def controlCluster(api, cluster_name, cluster_action = None):
    # Start or stop the Cluster 
    pass 

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
    api = getApiResource()
    
    # get Cloudera Management Service
    cm = api.get_cloudera_manager()
    cms = cm.get_service()
    
    # Toggle CM between start and stop
#     if cms.serviceState == 'STARTED':
#         controlCM(api, 'stop')
#     elif cms.serviceState == 'STOPPED':
#         controlCM(api, 'start')
#     else:
#         print(cms.serviceState)

    service_type = 'KMS'
    config_dict = getKmsAcls ('kms-acls.xml') 
    # {'kms-acls.xml_role_safety_valve': '<property><name>hadoop.kms.acl.DELETE</name><value>gene gene</value></property>'}

    # loop through the all clusters 
    for cluster in api.get_all_clusters(view = "full"):
        print (cluster.name + '\t' + 
               cluster.version + '\t' + 
               cluster.fullVersion)
        updateConfig(cluster, service_type, config_dict)     
         
    # start/stop the cluster
#     clust_cmd = my_clust.start()
#     print ("Active: %s. Success: %s" % (clust_cmd.active, clust_cmd.success))
#     clust_cmd.wait()
#     print ("Active: %s. Success: %s" % (clust_cmd.active, clust_cmd.success))
     

    
if __name__ == '__main__':
    main()
