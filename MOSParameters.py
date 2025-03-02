import math
import time
import paho.mqtt.client as mqtt
import syslog
import signal
from threading import Event
from socket import gethostname
################################################################
################################################################
ZEROUSER="fieldcontrol"
ZEROPASSWD="excrement69Yuk"
MQTTCONNECTTIMEOUT=5 #seconds
################################################################
################################################################
# Base class to manage parameters read from MQTT
# 
class mosParameter:

    #override and call this
    def __init__(self,entityID,mosClient,callbackFn,rootTopic):
        self._callbackLink = None
        self._onConnectLink = None
        self._myTopic = rootTopic+'/'+entityID.lower()
        self._entityID = entityID
        self._addMosCallback(mosClient,callbackFn)
        self._addMosOnConnectCallback(mosClient,self._onConnect)
        self._doSubscribe(mosClient)
        
    #do NOT override
    # subscribe to parameter topic on MQTT server
    def _doSubscribe(self,mosClient):
        mosClient.subscribe(self._myTopic, 0)
        
    #do NOT override
    # call all other onConnects and the do our own subscription
    def _onConnect(self,client, userdata, flags, rc):
        if self._onConnectLink:
            self._onConnectLink(client, userdata, flags, rc)
        self._doSubscribe(client)
        
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

    def __init__(self,entityID,mosClient,intValue,rootTopic):
        #v1.07 supplied initial value will be overwritten by persistent value from MQTT
        self._intValue = intValue
        mosParameter.__init__(self,entityID,mosClient,self._onMessage,rootTopic)

    def _onMessage(self,client, userdata, message):
        mosParameter._onMessage(self,client,userdata,message)
        if message.topic == self._myTopic:
            self._intValue = intFromASCBytes(message.payload)

    def value(self):
        return self._intValue;
################################################################
################################################################
class floatMosParameter(mosParameter):

    def __init__(self,entityID,mosClient,floatValue,rootTopic):
        #v1.07 supplied initial value will be overwritten by persistent value from MQTT
        self._floatValue = floatValue
        mosParameter.__init__(self,entityID,mosClient,self._onMessage,rootTopic)

    def _onMessage(self,client, userdata, message):
        mosParameter._onMessage(self,client,userdata,message)
        if message.topic == self._myTopic:
            self._floatValue = floatFromASCBytes(message.payload)

    def value(self):
        return self._floatValue;
################################################################
################################################################
class boolMosParameter(mosParameter):

    def __init__(self,entityID,mosClient,boolValue,rootTopic):
        #v1.07 supplied initial value will be overwritten by persistent value from MQTT
        self._boolValue = boolValue
        mosParameter.__init__(self,entityID,mosClient,self._onMessage,rootTopic)

    def _onMessage(self,client, userdata, message):
        mosParameter._onMessage(self,client,userdata,message)
        if message.topic == self._myTopic:
            self._boolValue = boolFromASCBytes(message.payload)

    def value(self):
        return self._boolValue;
################################################################
################################################################

class doLog:

    def __init__(self, logToSyslog = True, logToStdErr = True, logErrorsOnly=False):
        self._logErrOnly = logErrorsOnly
        self._stderr = logToStdErr
        self._syslog = logToSyslog

    def log(self, mess = '', end = None, error = False):
        if self._syslog and (not self._logErrOnly or error):
            syslog.syslog(syslog.LOG_ERR if error else syslog.LOG_INFO, mess)
        if self._stderr:
            print(mess,end=end)

    def close(self):
        syslog.closelog()

################################################################
################################################################
# Try to deal with Signals
# - declare an instance of this class
# - class will trap signals, allowing a cleanup before termination
# - have main loop in program continue until isSet() is true
# - use wait(seconds) instead of sleep
class sigExit:
    def __init__(self,logger):
        self._exit = Event()
        self._logger = logger
        for sig in ('TERM', 'HUP', 'INT'):
            signal.signal(getattr(signal, 'SIG'+sig), self._quitter)

    def _quitter(self, signo, _frame):
        self._logger.log("Interrupted by %d, shutting down" % signo,error=True)
        self._exit.set()

    def isSet(self):
        return self._exit.is_set()

    def wait(self,seconds):
        self._exit.wait(seconds)
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
def setupMQTT(rootTopic,mosServer = "piserve", mosPort=1883):
    mosClient = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1,gethostname(),rootTopic)
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
def publishMQTT(mosClient,key,value,rootTopic):
    published_value = value
    mosClient.publish(rootTopic+key,'{0:.3f}'.format(published_value),retain=True)
    #doLog("{0:7}: ".format(key), end='')
    #doLog("{0:>6.3f} {1}".format(published_value, unit))
#Shutdown MQTT
def finishMQTT(mosClient):
    mosClient.loop_stop()
    mosClient.disconnect()
################################################################
################################################################
def __init__(title = "MOSLog"):
  syslog.openlog(ident=title)
