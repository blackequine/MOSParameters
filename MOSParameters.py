import math
import time
import paho.mqtt.client as mqtt
################################################################
################################################################
# Base class to manage parameters read from MQTT
# 
class mosParameter:

    #override and call this
    def __init__(self,entityID,mosClient,callbackFn):
        self._callbackLink = None
        self._onConnectLink = None
        self._myTopic = BATTROOT+'/'+entityID.lower()
        self._entityID = entityID
        self._addMosCallback(mosClient,callbackFn)
        self._addMosOnConnectCallback(mosClient,self._onConnect)
        self._doSubscribe()
        
    #do NOT override
    # subscribe to parameter topic on MQTT server
    def _doSubscribe(self):
        mosClient.subscribe(self._myTopic, 0)
        
    #do NOT override
    # call all other onConnects and the do our own subscription
    def _onConnect(self,client, userdata, flags, rc):
        if self._onConnectLink:
            self._onConnectLink(client, userdata, flags, rc)
        self._doSubscribe()
        
    #do NOT override
    # build single linked list to subscribe to all parameter topics
    # in the event of a reconnect.  Each subscription is added to end of list
    def _addMosOnConnectCallback(self, mosClient, callbackFn):
        self._onConnectLink = mosClient.on_connect
        mosClient.on_connect = callbackFn

    #do NOT override
    # build single linked list of message callbacks to read parameter topics
    def _addMosCallback(self, mosClient, callbackFn):
        self._callbackLink = mosClient.on_message
        mosClient.on_message = callbackFn

    #override and call this
    # derived classes supply their own on_message to process their subscription
    # and call this to call all the other on_message call backs
    def _onMessage(self,client, userdata, message):
        if self._callbackLink:
            self._callbackLink(client,userdata,message)

    #do NOT override
    def entityID(self):
        return self._entityID

################################################################
################################################################
class intMosParameter(mosParameter):

    def __init__(self,entityID,mosClient,intValue):
        #v1.07 supplied initial value will be overwritten by persistent value from MQTT
        self._intValue = intValue
        mosParameter.__init__(self,entityID,mosClient,self._onMessage)

    def _onMessage(self,client, userdata, message):
        mosParameter._onMessage(self,client,userdata,message)
        if message.topic == self._myTopic:
            self._intValue = intFromASCBytes(message.payload)

    def value(self):
        return self._intValue;
################################################################
################################################################
class floatMosParameter(mosParameter):

    def __init__(self,entityID,mosClient,floatValue):
        #v1.07 supplied initial value will be overwritten by persistent value from MQTT
        self._floatValue = floatValue
        mosParameter.__init__(self,entityID,mosClient,self._onMessage)

    def _onMessage(self,client, userdata, message):
        mosParameter._onMessage(self,client,userdata,message)
        if message.topic == self._myTopic:
            self._floatValue = floatFromASCBytes(message.payload)

    def value(self):
        return self._floatValue;
################################################################
################################################################
class boolMosParameter(mosParameter):

    def __init__(self,entityID,mosClient,boolValue):
        #v1.07 supplied initial value will be overwritten by persistent value from MQTT
        self._boolValue = boolValue
        mosParameter.__init__(self,entityID,mosClient,self._onMessage)

    def _onMessage(self,client, userdata, message):
        mosParameter._onMessage(self,client,userdata,message)
        if message.topic == self._myTopic:
            self._boolValue = boolFromASCBytes(message.payload)

    def value(self):
        return self._boolValue;
################################################################
################################################################
# Convert bytes object containing ASCII representation of
# a number into an integer.
def intFromASCBytes(byteObject):
    s = stringFromASCBytes(byteObject).split('.')[0]
    return int(s)
################################################################
################################################################
# Convert bytes object containing ASCII representation of
# a boolean into an boolean.
def floatFromASCBytes(byteObject):
    s = stringFromASCBytes(byteObject)
    return float(s)
################################################################
################################################################
# Convert bytes object containing ASCII representation of
# a boolean into an boolean.
def boolFromASCBytes(byteObject):
    s = stringFromASCBytes(byteObject)
    if s.upper() == 'TRUE':
        return True
    return False
################################################################
################################################################
def stringFromASCBytes(byteObject):
    s = ''
    for b in byteObject:
        s+=chr(b)
    return s
################################################################
# Callbacks and other functions
#callbacks for mqtt
def on_connect(client, userdata, flags, rc):
    if rc==0:
        client.connected_flag=True #set flag
    else:
        client.connected_rc=rc
################################################################
################################################################
# MQTT utilities
#
def setupMQTT(mosServer = "piserve", mosPort=1883):
    mosClient = mqtt.Client(gethostname())
    mosClient.connected_flag=False
    mosClient.connected_rc=0
    mosClient.on_connect = on_connect #bind callback
    mosClient.username_pw_set(ZEROUSER,ZEROPASSWD)
    mosClient.loop_start()
    mosClient.connect(mosServer,mosPort)
    timeout=MQTTCONNECTTIMEOUT
    while not mosClient.connected_flag and timeout > 0:
        timeout -= 1
        time.sleep(1)
    return mosClient
def publishMQTT(mosClient,key,value,relays):
    published_value = value
    mosClient.publish(CONTROLROOT+key,'{0:.3f}'.format(published_value),retain=True)
    doLog("{0:7}: ".format(key), end='')
    doLog("{0:>6.3f} {1}".format(published_value, unit))
#Shutdown MQTT
def finishMQTT(mosClient):
    mosClient.loop_stop()
    mosClient.disconnect()
################################################################
################################################################