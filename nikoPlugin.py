import time
import os
import threading
import json
import paho.mqtt.client as mqtt


class NikoPlugin():
    def __init__(self, host, username, password, device_update_interval, hysteresis, device_list = {}) -> None:
        #configuration for the client
        self.host = host
        self.username = username
        self.password = password
        self.deviceUpdateInterval = device_update_interval
        self.hysteresis = hysteresis
        self.deviceList = device_list
        self.runlevel = 0

        #threads
        self.getConfigTrigger = threading.Event()
        self.getConfigThread = threading.Thread(target=self.getConfig) 
        self.getConfigThread.daemon = True
        self.getConfigThread.start()

        self.switchDevicesTrigger = threading.Event()
        self.switchDevicesThread = threading.Thread(target=self.switchDevices)
        self.switchDevicesThread.daemon = True
        self.switchDevicesThread.start()

        #initialize client
        self.client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
        self.client.tls_set(tls_version=mqtt.ssl.PROTOCOL_TLS)
        self.client.username_pw_set(username=self.username, password=self.password)

        #connect callback functions
        self.client.on_connect = self.onConnect
        self.client.on_disconnect = self.onDisconnect
        self.client.on_subscribe = self.onSubscribe
        self.client.message_callback_add("config/room", self.configCallback)
        self.client.message_callback_add("system/runlevel", self.systemRunlevelCallback)
        self.client.message_callback_add("data/raw/socket/#", self.readDataFromDevice)

    #callback called when client connects
    def onConnect(self, mqttc, obj, flags, reason_code, properties):
        if reason_code == 0:
            print("Connected to MQTT Broker")
            self.client.subscribe("#")
            self.getConfigTrigger.set()
        else:
            print("Failed to connect to MQTT Broker")
    

    ##callback called when client disconnects
    def onDisconnect(self, mqttc, obj, flags, reason_code, properties):
        print('Client Disconnected')
        self.getConfigTrigger.clear()
            

    #callback for when client subscribes to all available topics
    def onSubscribe(self, mqttc, obj, mid, reason_code_list, properties):
        print("Subscribed: "+str(mid)+" "+str(reason_code_list))


    #callback function for message/data received on the room/config topic
    def configCallback(self, client, userdata, msg):
        print("Data Received from ", msg.topic)
        data = json.loads(msg.payload)
        with open('serverConfig.json', 'w') as f:
            json.dump(obj=data, fp=f, indent=4)

        self.deviceList = data["plugin"]["niko"]["devicelist"]


    #callback function when runlevel message is received on the system/runlevel topic
    def systemRunlevelCallback(self, client, userdata, msg):
        print("Data Received from ", msg.topic)
        self.runlevel = int(msg.payload)
        self.switchDevicesTrigger.set()

    #callback function when data is received from a device
    def readDataFromDevice(self, client, userdata, msg):
        deviceID = str(msg.topic).split('/')[-1]
        data = json.loads(msg.payload)
        for device in self.deviceList:
            print(self.deviceList[device].values())
            if deviceID in self.deviceList[device].values():
                self.client.publish(f'data/room/socket/{device}', json.dumps(data))



    #connect the client to the broker/server based on the configuration provied above
    def connectToBroker(self):
        self.client.connect(self.host, 8883)
        self.client.loop_forever()


    #threaded functions
    def getConfig(self):
        while True:
            if self.getConfigTrigger.is_set():
                time.sleep(self.deviceUpdateInterval)
                for device in self.deviceList:
                    self.client.publish(f"data/room/socket/{device}/get", 'GET')
            else:
                self.getConfigTrigger.wait()

    def switchDevices(self):
        while True:
            if self.switchDevicesTrigger.is_set():
                for device in self.deviceList:
                    id = self.deviceList[device]['id']
                    functionLevel = int(self.deviceList[device]['function_level'])
                    if functionLevel >= self.runlevel + self.hysteresis:
                        self.client.publish(f'room/socket/{device}/set', json.dumps({"id": id, "state": "ON"}))
                    elif functionLevel <= self.runlevel:
                        self.client.publish(f'room/socket/{device}/set', json.dumps({"id": id, "state": "OFF"}))
                self.switchDevicesTrigger.clear()
            else:
                self.switchDevicesTrigger.wait()


    #disconnect client and exit
    def cleanExit(self):
        self.client.disconnect()
        return




if __name__ == '__main__':
    #load default configuration
    with open('default.conf', 'r') as f:
        defaultConfig = json.load(f)
    
    #check if niko object from server exists
    if os.path.exists('serverConfig.json'):
        with open('serverConfig.json', 'r') as f:
            serverConfig = json.load(f)
        
        #initialize plugin with the server provided configuration
        niko = NikoPlugin(host=defaultConfig["mqtt_server"], 
                          username=defaultConfig["mqtt_user"], 
                          password=defaultConfig["mqtt_pass"],
                          device_update_interval=serverConfig["plugin"]["niko"]["device_update_interval"],
                          hysteresis=serverConfig["plugin"]["niko"]["hysteresis"],
                          device_list=serverConfig["plugin"]["niko"]["devicelist"]
                          )

    else:
        #initialize plugin with the default configuration
        niko = NikoPlugin(host=defaultConfig["mqtt_server"], 
                          username=defaultConfig["mqtt_user"], 
                          password=defaultConfig["mqtt_pass"],
                          device_update_interval=defaultConfig["device_update_interval"],
                          hysteresis=defaultConfig["hysteresis"]
                          )
    #start the client
    try:
        niko.connectToBroker()
    except KeyboardInterrupt:
        niko.cleanExit()
