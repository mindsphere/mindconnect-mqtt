#!/usr/bin/python

# this program will subscribe and send messages to mqtt topics

import paho.mqtt.client as paho
import ssl
from time import sleep
from random import uniform
import json
import logging
import threading
import datetime
import uuid
from lib import config_parser
import PySimpleGUI as sg
import jwt
import time

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
        self.model_name = config['MODEL_NAME']
        self.platform = self.config["PLATFORM"]
        self.tenantCertPath = ''
        self.user_name = ''
        self.jwt_token = ''
        self.assel_model_json_file = "example_json/asset_model.json"
        self.instance_json_file = "example_json/instance.json"
        self.timeseries_json_file = "example_json/timeseries.json"

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
            [sg.Button("Publish Model")],
            [sg.Text("Model Creation Response: ")],
            [sg.Multiline("", size=(100, 15), key="-M-RESPONSE-", font=font)],
            [sg.Button("Create Model Instance")],
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

    def getCurrentTimestamp(self):
        date_now = datetime.datetime.now()
        curr_date_time = date_now.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        return curr_date_time

    def configure_topics(self):
        if self.platform == 'AWS':
            self.instance_subscribe_topic = "tc/"+self.tenant+"/"+self.clientId+"/i/amo_v3/ip"
            self.model_subscribe_topic = "tc/"+self.tenant+"/"+self.clientId+"/i/amo_v3/ms"
            self.model_publish_topic = "tc/"+self.tenant+"/"+self.clientId+"/o/amo_v3/m"
            self.instance_publish_topic = "tc/"+self.tenant+"/"+self.clientId+"/o/amo_v3/i"
        elif self.platform == 'AZURE':
            self.instance_subscribe_topic = "devices/"+self.clientId+"/messages/devicebound/#"
            self.model_subscribe_topic = ""
            self.model_publish_topic = "devices/"+self.clientId+"/messages/events/amo_v3=m"
            self.instance_publish_topic = "devices/"+self.clientId+"/messages/events/amo_v3=i"
            self.user_name = self.mqtt_broker_host + "/" + self.clientId
        elif self.platform == 'PVTCLOUD':
            self.instance_subscribe_topic = "tc/"+self.tenant+"/"+self.clientId+"/i/amo_v3/ip"
            self.model_subscribe_topic = "tc/"+self.tenant+"/"+self.clientId+"/i/amo_v3/ms"
            self.model_publish_topic = "tc/"+self.tenant+"/"+self.clientId+"/o/amo_v3/m"
            self.instance_publish_topic = "tc/"+self.tenant+"/"+self.clientId+"/o/amo_v3/i"
            self.user_name = "_CertificateBearer"
            self.tenantCertPath = self.config['TENANT_CERT_PATH']

    def generate_jwt_token(self):
        print("Generating JWT Token for RabbitMQ MQTT Broker...")

        device_private_key = open(self.keyPath, 'r').read()

        device_cert_file = open(self.certPath, 'r')
        device_cert_lines = device_cert_file.readlines()
        device_cert_lines = device_cert_lines[:-1]
        device_cert_lines = device_cert_lines[1:]
        device_cert_single_line = "".join(device_cert_lines).replace('\n', '').replace('\r', '')
        #print("device_cert_single_line", device_cert_single_line)

        tenant_cert_file = open(self.tenantCertPath, 'r')
        tenant_cert_lines = tenant_cert_file.readlines()
        tenant_cert_lines = tenant_cert_lines[:-1]
        tenant_cert_lines = tenant_cert_lines[1:]
        tenant_cert_single_line = "".join(tenant_cert_lines).replace('\n', '').replace('\r', '')
        #print("tenant_cert_single_line", tenant_cert_single_line)

        #print("Private Key from File" + device_private_key)
        private_key_int = device_private_key.encode('utf-8')

        iat = int(round(time.time()))
        exp = int(round(iat + 1500))
        jti = str(uuid.uuid4())

        claim = {
            "jti": jti,
            "iss": self.clientId,
            "sub": self.clientId,
            "aud": ["MQTTBroker"],
            "iat": iat,
            "nbf": iat,
            "exp": exp,
            "schemas": ["urn:siemens:mindsphere:v1"],
            "ten": self.tenant
        }

        headers = {
            "alg": "RS256",
            "x5c": [device_cert_single_line, tenant_cert_single_line],
            "typ": "JWT"
        }

        encoded = jwt.encode(claim, private_key_int, algorithm="RS256", headers=headers)
        # print("token: ", encoded)
        logging.debug("jwt token for secret : " + str(encoded))
        self.jwt_token = str(encoded)

    # def on_log(client, userdata, level, msg):
    #    print(msg.topic+" "+str(msg.payload))
    def connect_device(self):
        self.mqttc.on_connect = self.on_connect
        self.mqttc.on_message = self.on_message
        # mqttc.on_log = on_log
        if self.platform == 'PVTCLOUD':
            self.generate_jwt_token()
            self.mqttc.username_pw_set(self.user_name, self.jwt_token)
            print("Connecting to Rabbitmq via JWT token.")
            self.mqttc.tls_set(self.caPath, cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLSv1_2,
                               ciphers=None)
        else:
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
            curr_date_time = self.getCurrentTimestamp()
            print(curr_date_time + " Connected !!!")
            event, values = self.window.read()
            if self.connected_flag:
                if event == "Publish Model":
                    print("Publishing Model")
                    asset_model_file = open(self.assel_model_json_file, 'r')
                    asset_model_contents = asset_model_file.read()
                    asset_model_contents = asset_model_contents.replace("<tenantId>", self.tenant)
                    asset_model_contents = asset_model_contents.replace("<uuid>", str(uuid.uuid4()))

                    model = json.loads(asset_model_contents)

                    serialized = json.dumps(model, sort_keys=True, indent=3)
                    print(serialized)

                    self.mqttc.publish(self.model_publish_topic, json.dumps(model), qos=0)
                    print('sent to model creation topic : ' + self.model_publish_topic)

                elif event == "Create Model Instance":
                    print("Creating Instance")
                    instance_file = open(self.instance_json_file, 'r')
                    instance_contents = instance_file.read()
                    instance_contents = instance_contents.replace("<model_name>", self.model_name)
                    instance_contents = instance_contents.replace("<uuid>", str(uuid.uuid4()))
                    instance = json.loads(instance_contents)

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

env = "AWS"

print("Loading Config file for Environment " + env)
loadedConfig = config_parser.parse(env, 'configs/mqtt-config.json')
iotService = AssetModelerService(loadedConfig)
