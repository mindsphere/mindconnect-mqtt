#!/usr/bin/python
# **************************************#
#  MQTT in Python                       #
# **************************************#
# Date: July 05, 2023                   #
# **************************************#
# Load necessary libraries
import base64

import paho.mqtt.client as paho
import os
import ssl
from time import sleep
from random import uniform, randint
import json
import logging
from time import sleep
import datetime
from lib import config_parser
import json
import os
import logging, sys
import requests
import schedule
import uuid

logging.basicConfig(level=logging.DEBUG)

class IotService:

    # Instance created status file
    instance_file_name = "instance.conf"
    instance_exist = False

    http_token = ""

    # **************************************#
    # Hardware Pin Objects:
    # Onboard LED on off indicating running state of the Device

    # Firmware LED details

    # DHT Sensor Setup

    # Buzzer Setup

    # Display Setup

    # Motor Setup

    # Light Control pin definition

    # Infrared Sensor

    def __init__(self, config):
        self.config = config

        # MQTT Connection details
        self.awshost = config['IOT_HOST']
        self.awsport = 8883
        self.tenant = config['TENANT_ID']
        self.device_name = config['DEVICE_NAME']
        self.clientId = config['CLIENT_ID']
        self.caPath = config['CA_PATH']
        self.certPath = config['DEVICE_CERT_PATH']
        self.keyPath = config['DEVICE_KET_PATH']
        self.instance_name = self.device_name + "_DataOwner"
        self.model_name = config['MODEL_NAME']

        # Publish and Subscribe topic details
        self.subscribe_topic = "tc/"+self.tenant+"/"+self.clientId+"/i/cmd_v3/c"
        self.publish_topic = "tc/"+self.tenant+"/"+self.clientId+"/o/cmd_v3/u"

        self.timeseries_publish_topic = "tc/"+self.tenant+"/"+self.clientId+"/o/mc_v3/ts"
        self.event_publish_topic = "tc/"+self.tenant+"/"+self.clientId+"/o/mc_v3/e"
        self.file_publish_topic = "tc/"+self.tenant+"/"+self.clientId+"/o/mc_v3/f"

        self.token_req_publish_topic = "tc/"+self.tenant+"/"+self.clientId+"/o/agm_v3/t"
        self.token_subscribe_topic = "tc/"+self.tenant+"/"+self.clientId+"/i/agm_v3/tr"

        self.instance_publish_topic = "tc/"+self.tenant+"/"+self.clientId+"/o/amo_v3/i"
        self.instance_prog_subscribe_topic = "tc/"+self.tenant+"/"+self.clientId+"/i/amo_v3/ip"

        self.gateway_url = config['GW_URL']
        self.southgate_url = config['SGW_URL']

        # Connection Variable for MQTTClient declaration.
        self.connection = paho.Client(self.clientId, clean_session=False)
        self.connected_flag = False
        self.establish_connection()
        self.create_model_instance()

        schedule.every(60).seconds.do(self.insert_timeseries_callback)
        schedule.every(120).seconds.do(self.infrared_sensor_interrupt)

        schedule.every(30).minutes.do(self.refresh_http_token)

        schedule.every(120).seconds.do(self.upload_to_datalake)
        schedule.every(120).seconds.do(self.execute_file_upload)

        print("Time synchronization after initialization" + self.getCurrentTimestamp())

        self.start_device_connection()

    # **************************************#
    # Callback function, it is the function
    # that will be called when a new msg
    # is received from MQTT broker
    def on_message(self, client, userdata, msg):
        print("Message received in the on message")
        topic_name = msg.topic
        print("topic: " + topic_name)
        payload_json_str = str(msg.payload, 'utf-8')
        print("payload: " + payload_json_str)

        cmd_data = json.loads(payload_json_str)
        try:
            if "cmd_v3" in topic_name:
                print("Message received from Agent Message Box")
                #_thread.start_new_thread(self.executeCommand,  [cmd_data])
                self.executeCommand(cmd_data)
                # Start a new thread as on return of this method the receive is ack'd.
                # If not done same message is read multiple times.
            elif "agm_v3" in topic_name:
                print("Message received from Agent Management")
                self.display_lines("Agent token", "received")
                self.http_token = cmd_data["data"]["access_token"]
                print("Token extracted : " + self.http_token)
            else:
                print("Message received from Asset Modeler")
                self.display_lines("Asset Modeler", "status message", "received.")
                if cmd_data["data"]["status"] == "Success":
                    self.display_lines("Instance creation", "successful.")
                    print("Instance Creation is Successful.")
                    f = open(self.instance_file_name, "a")
                    f.write("CREATED")
                    f.close()
                    self.instance_exist = True
                else:
                    print("Instance Creation is in progress, waiting to complete ...")
                    self.display_lines("Instance creation", "in progress.", "Waiting ...")

        except Exception as e:
            sys.print_exception(e)
            print("Trouble to receive from mqtt : " + str(e))

    def display_lines(self, *argv):
        line = 0
        for arg in argv:
            print(arg)
            if line == 5:
                break
            line += 1

    def executeCommand(self, cmd_message):
        print("Execute Command starting ...!")
        command_payload_json = cmd_message["data"]["payload"]
        command = self.extractJsonObject(command_payload_json)
        commandType = command["commandType"]
        if commandType == "firmware_update":
            self.executeFirmwareUpdate(cmd_message)
        elif commandType == "actuator_control":
            self.executeActuatorCommand(cmd_message)
        elif commandType == "light_control":
            self.execute_light_control_cmd(cmd_message)
        else:
            print("Invalid Command")

    def executeActuatorCommand(self, cmd_message):
        print("Actuator Command Entry point")
        request_id = cmd_message["id"]
        job_id = cmd_message["data"]["jobId"]
        command_payload_json = cmd_message["data"]["payload"]
        command = self.extractJsonObject(command_payload_json)
        execute_command = command["execute_command"]
        response = "Successfully Executed " + execute_command
        status = "EXECUTED"

        self.display_lines("Actuator CMD", "Command : ", execute_command, "Processing...")
        sleep(3)

        if execute_command == 'START':
            print("Executing Actuator Command : START")

        elif execute_command == 'STOP':
            print("Executing Actuator Command : STOP")

        else:
            response = "Invalid Command Provided"
            status = "FAILED"
            print("<<<  wrong data  >>>")
            print("please enter the defined set of commands.")

        cmd_response = {
            "id": str(uuid.uuid4()),
            "requestId": request_id,
            "data": {
                "timestamp": self.getCurrentTimestamp(),
                "jobId": job_id,
                "status": status,
                "response": {"message": response}
            }

        }
        try:
            self.connection.publish(self.publish_topic, json.dumps(cmd_response), qos=0)
        except Exception as Argument:
            sys.print_exception(Argument)
            print("Error publishing data : " + str(Argument))
            print("Connection Lost , trying to connect again.")
            self.establish_connection()

        self.display_lines("Actuator CMD", "Command : ", execute_command, "Processed !!!")
        print("sending command response to topic : " + self.publish_topic)
        print("sending command response payload : " + json.dumps(cmd_response))

    def execute_light_control_cmd(self, cmd_message):
        print("Light Control Command Entry point")
        request_id = cmd_message["id"]
        job_id = cmd_message["data"]["jobId"]
        command_payload_json = cmd_message["data"]["payload"]
        command = self.extractJsonObject(command_payload_json)
        execute_command = command["execute_command"]
        response = "Successfully Executed " + execute_command
        status = "EXECUTED"

        self.display_lines("Light Control CMD", "Command : ", execute_command, "Processing...")
        sleep(5)

        if execute_command == 'ON':
            print("Executing Light Start Command : ON")
        elif execute_command == 'OFF':
            print("Executing Light Off Command : OFF")
        else:
            response = "Invalid Command Provided"
            status = "FAILED"
            print("<<<  wrong data  >>>")
            print("please enter the defined set of commands.")

        cmd_response = {
            "id": str(uuid.uuid4()),
            "requestId": request_id,
            "data": {
                "timestamp": self.getCurrentTimestamp(),
                "jobId": job_id,
                "status": status,
                "response": {"message": response}
            }

        }
        try:
            self.connection.publish(self.publish_topic, json.dumps(cmd_response), qos=0)
        except Exception as Argument:
            sys.print_exception(Argument)
            print("Error publishing data : " + str(Argument))
            print("Connection Lost , trying to connect again.")
            self.establish_connection()

        self.display_lines("Light Ctrl CMD", "Command : ", execute_command, "Processed !!!")
        print("sending command response to topic : " + self.publish_topic)
        print("sending command response payload : " + json.dumps(cmd_response))

    def executeFirmwareUpdate(self, cmd_message):
        request_id = cmd_message["id"]
        job_id = cmd_message["data"]["jobId"]
        command_payload = cmd_message["data"]["payload"]
        print("Executing the Command on Device : " + json.dumps(command_payload))
        self.display_lines("Message Received", "Firmware Update")

        print("LED turned on for Firmware Update")

        for x in range(2):
            sleep(10)
            upgrade_percentage = (x + 1) * 50
            status = "EXECUTING"
            if upgrade_percentage == 100:
                status = "EXECUTED"

            execution_response = {
                "progress": str(upgrade_percentage) + "% upgrade done",
                "overall_status": "healthy"
            }
            serialized = json.dumps(execution_response)

            cmd_response = {
                "id": str(uuid.uuid4()),
                "requestId": request_id,
                "data": {
                    "timestamp": self.getCurrentTimestamp(),
                    "jobId": job_id,
                    "status": status,
                    "response": execution_response
                }

            }
            self.display_lines("Firmware Update", "In Progress", "Sending Update",
                               "Update % : " + str(upgrade_percentage))
            try:
                self.connection.publish(self.publish_topic, json.dumps(cmd_response), qos=0)
            except Exception as Argument:
                sys.print_exception(Argument)
                print("Error publishing data : " + str(Argument))
                print("Connection Lost , trying to connect again.")
                self.establish_connection()
            print("sending command response to topic : " + self.publish_topic)
            print("sending command response payload : " + json.dumps(cmd_response))
        self.display_lines("Firmware Update", "Complete")
        print("Firmware Update Complete Turning OFF LED")

    def create_model_instance(self):
        print("Checking and creating asset model instance.")
        self.display_lines("Checking Asset Model", "instance status.")
        try:
            f = open(self.instance_file_name, "r")
            creation_status = f.readline()
            if creation_status and "CREATED" in creation_status:
                print("The Asset Instance Already exist so skipping Instance Creation.")
                self.display_lines("Asset Instance", "already exist.")
                self.instance_exist = True
                return
        except OSError:  # open failed
            print("The file does not exist so need to create instance.")
            self.display_lines("Asset Instance", "does not exist.", "creating instance")

        instance_json = {
            "id": str(uuid.uuid4()),
            "data": {
                "modelExternalId": self.model_name,
                #"modelParentAssetId" : "677ae757a0044970a80e3c7083376757",
                "parameterization": {
                    "values": [
                        {
                            "name": "assetName",
                            "value": self.instance_name
                        },
                        {
                            "name": "childAssetName1",
                            "value": "Accumulator"
                        },
                        {
                            "name": "childAssetName2",
                            "value": "Reservoir"
                        }
                    ]
                }
            }
        }
        serialized = json.dumps(instance_json)
        print("sending instance creation message : " + serialized)
        print("sending instance creation message to topic : " + self.publish_topic)

        try:
            self.connection.publish(self.instance_publish_topic, serialized, qos=0)
        except Exception as Argument:
            sys.print_exception(Argument)
            print("Error publishing data : " + str(Argument))
            print("Connection Lost , trying to connect again.")
            self.establish_connection()
        print("Sent Instance Message successfully.")

    def executeInvalidCommandResponse(self, cmd_message):
        request_id = cmd_message["id"]
        job_id = cmd_message["data"]["jobId"]
        cmd_response = {
            "id": str(uuid.uuid4()),
            "requestId": request_id,
            "data": {
                "timestamp": self.getCurrentTimestamp(),
                "jobId": job_id,
                "status": "FAILED",
                "response": {"message": "Invalid Command Type Entered."}
            }

        }
        try:
            self.connection.publish(self.publish_topic, json.dumps(cmd_response), qos=0)
        except Exception as Argument:
            sys.print_exception(Argument)
            print("Error publishing data : " + str(Argument))
            print("Connection Lost , trying to connect again.")
            self.establish_connection()
        print("sending command response to topic : " + self.publish_topic)
        print("sending command response payload : " + json.dumps(cmd_response))

    def getCurrentTimestamp(self):
        date_now = datetime.datetime.now()
        curr_date_time = date_now.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        return curr_date_time

    def extractJsonObject(self, command_payload_json):
        # command = json.loads(command_payload_string.replace('\\', ''))
        # command = json.loads(command_payload_string)
        return command_payload_json

    # def on_log(client, userdata, level, msg):
    #    print(msg.topic+" "+str(msg.payload))


    def insert_timeseries_callback(self):
        print("Time Series Timer Called")
        temperature = int(uniform(30.0, 32.0))
        humidity = int(uniform(50.0, 55.0))
        pressure = int(uniform(20.0, 25.0))

        if not self.instance_exist:
            print("The instance is not created so skipping the data ingestion.")
            return

        print(u"Temperature: {:g}\u00b0C, Humidity: {:g}%".format(temperature, humidity))

        self.display_lines("Sending Data",
                           u"Temperature: {:g}C".format(temperature), u"Humidity: {:g}%".format(humidity))

        curr_date_time = self.getCurrentTimestamp()

        ts_data = {
            "timeseries": [
                {
                    "timestamp": curr_date_time,
                    "values": [
                        {
                            "dataPointId": "temperature",
                            "value": temperature,
                            "qualityCode": "0"
                        }
                    ]
                },
                {
                    "timestamp": curr_date_time,
                    "values": [
                        {
                            "dataPointId": "humidity",
                            "value": humidity,
                            "qualityCode": "0"
                        }
                    ]
                }
            ]
        }
        try:
            self.connection.publish(self.timeseries_publish_topic, json.dumps(ts_data), qos=0)
        except Exception as Argument:
            sys.print_exception(Argument)
            print("Error publishing data : " + str(Argument))
            print("Connection Lost , trying to connect again.")
            self.establish_connection()
        print("sending time series data : " + self.timeseries_publish_topic)


    def check_interrupt_timer_callback(self, t):
        print("checking interrupt.")
        self.infrared_sensor_interrupt({})


    def refresh_http_token(self):
        print("Refresh Http Token Called")

        if not self.instance_exist:
            print("The instance is not created so skipping the data ingestion.")
            return

        self.display_lines("Token Expired", "Requesting new token")

        token_req_id = str(uuid.uuid4())
        token_payload = {
            "id": str(token_req_id),
        }

        try:
            self.connection.publish(self.token_req_publish_topic, json.dumps(token_payload), qos=0)
        except Exception as Argument:
            sys.print_exception(Argument)
            print("Error publishing data : " + str(Argument))
            print("Connection Lost , trying to connect again.")
            self.establish_connection()
        print("sent token generation request : " + self.token_req_publish_topic)
        print("Sent token generation payload : " + json.dumps(token_payload))

    def infrared_sensor_interrupt(self):
        print('Infrared Sensor Event Triggered')
        self.display_lines("Motion Sensor", "INTERRUPT !!!  : ", "Shut Down JetPump.")
        sleep(1)
        print("Stopping Actuator due to Interrupt.")

        curr_date_time = self.getCurrentTimestamp()
        severity = randint(2, 4) * 10
        id = uuid.uuid4()
        event_payload = {
            "events": [{
                "id": str(id),
                "correlationId": str(id.hex),
                "timestamp": curr_date_time,
                "severity": str(severity),
                "type": "SensorInterruptEvent",
                "description": "IR Sensor Interrupt Event " + str(id.hex),
                "details": {
                    "utilizedPercentage": 60,
                    "measurements": 5
                }
            }]
        }

        try:
            self.connection.publish(self.event_publish_topic, json.dumps(event_payload), qos=0)
        except Exception as Argument:
            sys.print_exception(Argument)
            print("Error publishing data : " + str(Argument))
            print("Connection Lost , trying to connect again.")
            self.establish_connection()
        print("Sending Event data to topic : " + self.event_publish_topic)
        print("Sent Event data payload : " + json.dumps(event_payload))


    def execute_file_upload(self):
        print('File Upload Event Triggered')
        self.display_lines("File Upload", "Triggered !!!")
        curr_date_time = self.getCurrentTimestamp()
        severity = randint(2, 4) * 10
        unique_id = uuid.uuid4()

        file_content = curr_date_time + " INFO: Connection Successfully Established to broker mindonnect.eu1-int.mindsphere.io\n" + \
                       curr_date_time + " INFO: Subscribed to topic : tc/punint05/punint05_mqttagent/i/cmd_v3/c\n" + \
                       curr_date_time + " INFO: Checking and creating asset model instance.\n" + \
                       curr_date_time + " INFO: The Asset Instance Already exist so skipping Instance Creation.\n" + \
                       curr_date_time + " INTERRUPT : Infrared Sensor Event Triggered\n" + \
                       curr_date_time + " INFO: Sent Event data payload\n" + \
                       curr_date_time + " INFO: File Upload Event Triggered\n" + \
                       curr_date_time + " INFO: Sending File Contents to topic : tc/punint05/punint05_mqttagent/o/mc_v3/f\n" + \
                       curr_date_time + " INFO: Sent File Upload."
        print("Sending File Upload Data: " + file_content)

        encoded_base64_data = base64.b64encode(file_content.encode('ascii'))
        encoded_base64_data = encoded_base64_data[:-2]
        base64_message = encoded_base64_data.decode('ascii')

        file_upload_payload = {
            "file": {
                "name": "Daily Execution Log : " + str(unique_id.hex),
                "creationDate": curr_date_time,
                "content": base64_message
            }
        }

        try:
            self.connection.publish(self.file_publish_topic, json.dumps(file_upload_payload), qos=0)
        except Exception as Argument:
            print("Error publishing data : " + str(Argument))
            print("Connection Lost , trying to connect again.")
            self.establish_connection()
        print("Sending File Contents to topic : " + self.file_publish_topic)
        print("Sent File Upload payload : " + json.dumps(file_upload_payload))

    def upload_to_datalake(self):

        auth_headers = {
            'Content-Type': 'application/json',
            "Authorization": "Bearer " + self.http_token
        }

        asset_response = requests.get(self.gateway_url + '/api/assetmanagement/v3/assets?'
                                                         'filter={\"externalId\":\"'+self.clientId+'\"}',
                                      headers=auth_headers)
        print(asset_response.text)
        asset_response_json = json.loads(asset_response.text)
        assetId = asset_response_json["_embedded"]["assets"][0]["assetId"]
        print("asset id : " + assetId)
        curr_date_time = self.getCurrentTimestamp()

        payload = {
            "paths": [
                {"path": assetId + "/sensor-map-" + curr_date_time + ".obj"},
                {"path": assetId + "/quality-snapshot-" + curr_date_time + ".jpeg"}
            ]}

        response = requests.post(self.southgate_url + '/api/datalake/v3/generateUploadObjectUrls',
                                 data=json.dumps(payload),
                                 headers=auth_headers)
        print(response.text)
        dl_request_response_json = json.loads(response.text)

        sensor_file_signedUrl = ""
        sensor_image_signedUrl = ""

        for obj in dl_request_response_json["objectUrls"]:
            if "sensor-map" in obj["path"]:
                print("Found sensor map upload url")
                sensor_file_signedUrl = obj["signedUrl"]
            else:
                print("Found image upload url")
                sensor_image_signedUrl = obj["signedUrl"]

        sensor_map_file = open("upload_files/sensor-map.obj", 'r')
        file_content = sensor_map_file.read()
        upload_response = requests.put(sensor_file_signedUrl, data=file_content)

        print("sensor map file upload status : " + str(upload_response.status_code))
        image_location = 'upload_files/quality-snapshot-clear.jpeg'

        sensor_image_file = open(image_location, 'rb')
        image_file_content = sensor_image_file.read()
        upload_response = requests.put(sensor_image_signedUrl, data=image_file_content)

        print("sensor image file upload status : " + str(upload_response.status_code))

    def establish_connection(self):
        self.connected_flag = False
        self.display_lines("Welcome !!!", "Connecting to ", "MQTT Broker")
        sleep(2)

        print("Establishing connection to MQTT Broker...")

        if not os.path.isfile(self.certPath) or not os.path.isfile(self.keyPath):
            print("Certificates missing, so terminating !!!")
            exit(1)

        self.connection.on_connect = self.on_connect
        self.connection.on_message = self.on_message
        # mqttc.on_log = on_log
        self.connection.DEBUG = True

        self.connection.tls_set(self.caPath, certfile=self.certPath, keyfile=self.keyPath,
                                cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLSv1_2,
                                ciphers=None)
        print("waiting for connection...")

        try:
            self.connection.connect(self.awshost, self.awsport, keepalive=60)
            self.connection.loop_start()
        except Exception as Argument:
            sys.print_exception(Argument)
            print("Error establishing connection : " + str(Argument))

        print("Connection Successfully Established to broker {}".format(self.awshost))
        self.display_lines("MQTT Connection", "Established", "to MindSphere")

    def on_connect(self, client, userdata, flags, rc):
        print("Connection returned result: " + str(rc))
        # Subscribing in on_connect() means that if we lose the connection and
        # reconnect then subscriptions will be renewed.
        self.connected_flag = True

        client.subscribe([(self.subscribe_topic, 1), (self.instance_prog_subscribe_topic, 1),
                          (self.token_subscribe_topic, 1)])

        print("Subscribed to topic : " + self.subscribe_topic)
        print("Subscribed to topic : " + self.instance_prog_subscribe_topic)
        print("Subscribed to topic : " + self.token_subscribe_topic)

    # **************************************#
    # Main loop
    def start_device_connection(self):
        first = True
        while 1 == 1:
            sleep(1)
            schedule.run_pending()
            if self.connected_flag:
                sleep(10)
                curr_date_time = self.getCurrentTimestamp()
                print(curr_date_time + " Connected !!!")
                if first and self.instance_exist:
                    print("Sending initial token generation request")
                    self.refresh_http_token()
                    first = False


env = "AWS"

print("Loading Config file for Environment " + env)
loadedConfig = config_parser.parse(env, 'configs/mqtt-config.json')
iotService = IotService(loadedConfig)
