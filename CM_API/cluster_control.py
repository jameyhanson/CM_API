'''
Created on Aug 20, 2017

@author: jamey

Ref: https://cloudera.github.io/cm_api/docs/python-client/
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

def main():
    api = getApiResource()
    
    # get Cloudera Management Service
    cm = api.get_cloudera_manager()
    cms = cm.get_service()
    
    # Toggle CM between start and stop
    if cms.serviceState == 'STARTED':
        controlCM(api, 'stop')
    elif cms.serviceState == 'STOPPED':
        controlCM(api, 'start')
    else:
        print(cms.serviceState)
    
#     # loop through the cluster and print details
#     for clusters in api.get_all_clusters(view = "full"):
#         my_clust = clusters
#         print (my_clust.name + '\t' + 
#                my_clust.version + '\t' + 
#                my_clust.fullVersion)
#          
#     # start/stop the cluster
#     clust_cmd = my_clust.start()
#     print ("Active: %s. Success: %s" % (clust_cmd.active, clust_cmd.success))
#     clust_cmd.wait()
#     print ("Active: %s. Success: %s" % (clust_cmd.active, clust_cmd.success))
#      
#      
#      
#      
#     # list of running commands
#     for cms_running_commands in cms.get_commands(view = None):
#         print(cms_running_commands)
    
if __name__ == '__main__':
    main()
