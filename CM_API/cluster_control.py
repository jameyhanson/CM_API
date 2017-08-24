'''
Created on Aug 20, 2017

Control the cluster and Cloudera Manager Service

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

def controlCM(api, cm_action = '0'):
    # Start or stop Cloudera Management Service
    # if cm_action is not specified, toggle start / stop
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
    elif cm_action == '0':
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
    controlCM(api)    
         
    # start/stop the cluster
#     clust_cmd = my_clust.start()
#     print ("Active: %s. Success: %s" % (clust_cmd.active, clust_cmd.success))
#     clust_cmd.wait()
#     print ("Active: %s. Success: %s" % (clust_cmd.active, clust_cmd.success))
    
if __name__ == '__main__':
    main()
