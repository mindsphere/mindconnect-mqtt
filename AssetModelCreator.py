#!/usr/bin/python

# this program will subscribe and send messages to mqtt topics

import paho.mqtt.client as paho
import os
import socket
import ssl
from time import sleep
from random import uniform
import json
import logging
import threading
import datetime
import uuid
import PySimpleGUI as sg
from lib import config_parser

logging.basicConfig(level=logging.DEBUG)


class AssetModelerService:
    def __init__(self, config):
        self.config = config
        self.mqtt_broker_host = config['IOT_HOST']
        self.mqtt_broker_port = 8883
        self.clientId = config['CLIENT_ID']
        self.caPath = config['CA_PATH']
        self.certPath = config['DEVICE_CERT_PATH']
        self.keyPath = config['DEVICE_KET_PATH']
        self.tenant = config['TENANT_ID']
        self.user_name = ''

        self.configure_topics()

        self.mqttc = paho.Client(self.clientId)
        self.connected_flag = False

        self.create_window()
        self.connect_device()
        self.start_device_connection()

    def create_window(self):
        font = ("Courier New", 12)
        background = '#b3d9ff'
        sg.SetOptions(background_color=background, element_background_color=background,
                      text_element_background_color=background, window_location=(20, 5),
                      margins=(5, 5),
                      text_color= 'Black',
                      input_text_color='Black',
                      button_color=('Black', 'gainsboro'))

        self.layout = [
            [sg.Text("Asset Model and Instance Creator", key="TITLE")],
            [sg.Button("Create Model")],
            [sg.Text("Model Creation Response: ")],
            [sg.Multiline("", size=(100, 15), key="-M-RESPONSE-", font=font)],
            [sg.Button("Create Instance")],
            [sg.Text("Instance Creation Response: ")],
            [sg.Multiline("", size=(100, 15), key="-I-RESPONSE-", font=font)]
        ]

        # Create the window
        self.window = sg.Window("Asset Modeler", self.layout, resizable=False)

    def on_connect(self, client, userdata, flags, rc):
        print("Connection returned result: " + str(rc))
        # Subscribing in on_connect() means that if we lose the connection and
        # reconnect then subscriptions will be renewed.
        self.connected_flag = True

        if self.model_subscribe_topic:
            client.subscribe([(self.model_subscribe_topic, 1), (self.instance_subscribe_topic, 1)])
        else:
            client.subscribe(self.instance_subscribe_topic, 1)

    def on_message(self, client, userdata, msg):
        print("received message on topic: " + msg.topic)
        payload_json_str = msg.payload.decode('utf8')
        print("received payload: " + payload_json_str)

        cmd_data = json.loads(payload_json_str)
        threading.Thread(target=self.execute_command, args=[cmd_data, msg.topic]).start()
        # Start a new thread as on return of this method the receive is ack'd. If not done same message is read
        # multiple times. executeCommand(cmd_data)

    def execute_command(self, cmd_message, topic):
        if "ms" in topic:
            self.execute_model_response(cmd_message)
        elif "ip" in topic:
            self.execute_instance_response(cmd_message)
        else:
            print("Invalid Response received")

    def execute_model_response(self, cmd_message):
        print("Displaying Model Response")
        serialized_response = json.dumps(cmd_message, indent=1)
        previous_text = self.window["-M-RESPONSE-"].DefaultText
        self.window['-M-RESPONSE-'].update(serialized_response + "\n ------------------ \n" + previous_text)


    def execute_instance_response(self, cmd_message):
        print("Displaying Instance Response")
        serialized_response = json.dumps(cmd_message, indent=1)
        previous_text = self.window["-I-RESPONSE-"].DefaultText
        self.window['-I-RESPONSE-'].update(serialized_response + "\n ------------------ \n" + previous_text)


    def configure_topics(self):
        platform = self.config["PLATFORM"]
        if platform == 'AWS':
            self.instance_subscribe_topic = "tc/"+self.tenant+"/"+self.clientId+"/i/amo_v3/ip"
            self.model_subscribe_topic = "tc/"+self.tenant+"/"+self.clientId+"/i/amo_v3/ms"
            self.model_publish_topic = "tc/"+self.tenant+"/"+self.clientId+"/o/amo_v3/m"
            self.instance_publish_topic = "tc/"+self.tenant+"/"+self.clientId+"/o/amo_v3/i"
        elif platform == 'AZURE':
            self.instance_subscribe_topic = "devices/"+self.clientId+"/messages/devicebound/#"
            self.model_subscribe_topic = ""
            self.model_publish_topic = "devices/"+self.clientId+"/messages/events/amo_v3=m"
            self.instance_publish_topic = "devices/"+self.clientId+"/messages/events/amo_v3=i"
            self.user_name = self.mqtt_broker_host + "/" + self.clientId

    # def on_log(client, userdata, level, msg):
    #    print(msg.topic+" "+str(msg.payload))
    def connect_device(self):
        self.mqttc.on_connect = self.on_connect
        self.mqttc.on_message = self.on_message
        # mqttc.on_log = on_log
        if self.user_name:
            self.mqttc.username_pw_set(self.user_name)

        self.mqttc.tls_set(self.caPath, certfile=self.certPath, keyfile=self.keyPath,
                           cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLSv1_2,
                           ciphers=None)

        self.mqttc.connect(self.mqtt_broker_host, self.mqtt_broker_port, keepalive=60)

        # mqttc.loop_forever()
        self.mqttc.loop_start()

    def start_device_connection(self):

        while 1 == 1:
            #sleep(5)
            event, values = self.window.read()
            if self.connected_flag:
                if event == "Create Model":
                    print("Creating Model")
                    model = {
                        "id": str(uuid.uuid4()),
                        "data": {
                            "externalId": "JetPumpModel",
                            "typeModel": {
                                "aspectTypes": [
                                    {
                                        "id": self.tenant +".dht11Aspect",
                                        "name": "dht11Aspect",
                                        "description": "Variables of a Humidity Sensor",
                                        "category": "dynamic",
                                        "scope": "private",
                                        "variables": [
                                            {
                                                "name": "temperature",
                                                "dataType": "DOUBLE",
                                                "unit": "C"
                                            },
                                            {
                                                "name": "humidity",
                                                "dataType": "DOUBLE",
                                                "unit": "rh"
                                            }
                                        ]
                                    }
                                ],
                                "assetTypes": [
                                    {
                                        "id": self.tenant + ".jetPumpType",
                                        "name": "JetPumpType",
                                        "description": "Asset type for individual jet pump in a line",
                                        "variables": [],
                                        "parentTypeId": "core.basicasset",
                                        "aspects": [
                                            {
                                                "name": "dht11Aspect",
                                                "aspectTypeId": self.tenant +".dht11Aspect"
                                            }
                                        ]
                                    }
                                ]
                            },
                            "instanceModel": {
                                "assets": [
                                    {
                                        "referenceId": "ParentJetPump",
                                        "typeId": self.tenant + ".jetPumpType",
                                        "name": "${assetName}"
                                    },
                                    {
                                        "referenceId": "ChildJetPump1",
                                        "typeId": self.tenant + ".jetPumpType",
                                        "name": "${childAssetName1}",
                                        "parentReferenceId": "ParentJetPump"
                                    },
                                    {
                                        "referenceId": "ChildJetPump2",
                                        "typeId": self.tenant + ".jetPumpType",
                                        "name": "${childAssetName2}",
                                        "parentReferenceId": "ChildJetPump1"
                                    }
                                ]
                            },
                            "mappingModel": {
                                "mappings": [
                                    {
                                        "dataPointId": "temperature",
                                        "assetReferenceId": "ParentJetPump",
                                        "aspectName": "dht11Aspect",
                                        "variableName": "temperature"
                                    },
                                    {
                                        "dataPointId": "humidity",
                                        "assetReferenceId": "ParentJetPump",
                                        "aspectName": "dht11Aspect",
                                        "variableName": "humidity"
                                    }
                                ]
                            }
                        }
                    }

                    serialized = json.dumps(model, sort_keys=True, indent=3)
                    print(serialized)

                    self.mqttc.publish(self.model_publish_topic, json.dumps(model), qos=0)
                    print('sent to model creation topic : ' + self.model_publish_topic)

                elif event == "Create Instance":
                    print("Creating Instance")
                    instance = {
                        "id": str(uuid.uuid4()),
                        "data": {
                            "modelExternalId": "JetPumpModel",
                            "parameterization": {
                                "values": [
                                    {
                                        "name": "assetName",
                                        "value": "ESP-JetPumpParent"
                                    },
                                    {
                                        "name": "childAssetName1",
                                        "value": "ESP-JetPumpChild1"
                                    },
                                    {
                                        "name": "childAssetName2",
                                        "value": "ESP-JetPumpChild2"
                                    }
                                ]
                            }
                        }
                    }
                    serialized = json.dumps(instance, sort_keys=True, indent=3)
                    print(serialized)
                    self.mqttc.publish(self.instance_publish_topic, json.dumps(instance), qos=0)
                    print('sent to model creation topic : ' + self.instance_publish_topic)

                elif event == sg.WIN_CLOSED:
                    print("Closing window, operations completed.")
                    break
            else:
                print("Not Connect to MQTT Broker, waiting for connection !!!")

        self.window.close()


env = "AWS_PROD"

print("Loading Config file for Environment " + env)
loadedConfig = config_parser.parse(env, 'configs/config.json')
iotService = AssetModelerService(loadedConfig)
