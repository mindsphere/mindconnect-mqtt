#!/usr/bin/python
# **************************************#
#  MQTT in Python                       #
# **************************************#
# Date: August 21, 2024                   #
# **************************************#
# Load necessary libraries
import base64
import configparser
import datetime
import json
import logging
import os
import ssl
import sys
import uuid
from random import uniform, randint
from time import sleep
import time
import jwt

import paho.mqtt.client as paho
import requests
import schedule

from lib import config_parser
from lib.AssetModelHelper import ModelState, AssetModelHelper

# RPI imports
from pigpio_dht import DHT11, DHT22
import RPi.GPIO as GPIO
from pigpio_encoder.rotary import Rotary

logging.basicConfig(level=logging.DEBUG)


class IotService:
    # Agent State Config file
    agent_state_conf = "agent.ini"
    instance_exist = False

    # **************************************#
    # Hardware Pin Objects:
    # Onboard LED on off indicating running state of the Device
    GPIO.setmode(GPIO.BCM)

    # Firmware LED details
    led_gpio = 18
    GPIO.setup(led_gpio, GPIO.OUT)
    GPIO.output(led_gpio, GPIO.LOW)

    red_gpio = 21
    GPIO.setup(red_gpio, GPIO.OUT)
    GPIO.output(red_gpio, GPIO.LOW)
    yellow_gpio = 20
    GPIO.setup(yellow_gpio, GPIO.OUT)
    GPIO.output(yellow_gpio, GPIO.LOW)
    green_gpio = 16
    GPIO.setup(green_gpio, GPIO.OUT)
    GPIO.output(green_gpio, GPIO.LOW)

    # DHT Sensor Setup
    dht_gpio = 12
    dht_sensor = DHT11(dht_gpio)

    dht2_gpio = 22
    dht2_sensor = DHT22(dht2_gpio)

    # Buzzer Setup

    # IR Sensor Setup
    IR_PIN = 5
    GPIO.setup(IR_PIN, GPIO.IN, pull_up_down=GPIO.PUD_UP)

    # Motor Setup
    in1 = 24
    in2 = 23
    en = 25

    GPIO.setup(in1, GPIO.OUT)
    GPIO.setup(in2, GPIO.OUT)
    GPIO.setup(en, GPIO.OUT)
    GPIO.output(in1, GPIO.LOW)
    GPIO.output(in2, GPIO.LOW)
    motor = GPIO.PWM(en, 1000)
    motor.start(95)

    def __init__(self, config):
        self.config = config
        # MQTT Connection details
        self.broker_host = config['IOT_HOST']
        self.broker_port = 8883
        self.tenant = config['TENANT_ID']
        self.clientId = config['CLIENT_ID']
        self.caPath = config['CA_PATH']
        self.certPath = config['DEVICE_CERT_PATH']
        self.keyPath = config['DEVICE_KET_PATH']
        self.use_custom_timeseries = True if ('USE_CUSTOM_TIMESERIES' in config
                                              and config['USE_CUSTOM_TIMESERIES'] == 'True') else False
        self.custom_timeseries_topic = config['CUSTOM_TOPIC'] if ('CUSTOM_TOPIC' in config) else None

        self.asset_model_helper = AssetModelHelper(self.tenant)
        self.state_config = configparser.ConfigParser()

        self.http_token = ""

        self.timeseries_json_file = "example_json/timeseries.json"
        self.custom_timeseries_json_file = "example_json/custom_timeseries.json"
        self.event_json_file = "example_json/event.json"
        self.platform = config['PLATFORM'] if ('PLATFORM' in config) else "AWS"
        self.configure_topics()

        self.gateway_url = config['GW_URL']
        self.southgate_url = config['SGW_URL']

        self.my_rotary = Rotary(clk_gpio=13, dt_gpio=19, sw_gpio=26)
        self.my_rotary.setup_rotary(rotary_callback=self.rotary_callback, up_callback=self.up_callback,
                                    down_callback=self.down_callback)
        self.my_rotary.setup_switch(sw_short_callback=self.sw_short)

        # Connection Variable for MQTTClient declaration.
        self.connection = paho.Client(callback_api_version=paho.CallbackAPIVersion.VERSION2,
                                      client_id=self.clientId, clean_session=False)
        self.connection.enable_logger()
        self.connected_flag = False
        self.establish_connection()
        self.check_agent_state()

        schedule.every(60).seconds.do(self.insert_timeseries_callback)

        #schedule.every(120).seconds.do(self.infrared_sensor_interrupt)
        self.ir_interrupted = False
        GPIO.add_event_detect(self.IR_PIN, GPIO.FALLING, callback=self.check_interrupt_timer_callback, bouncetime=500)

        schedule.every(30).minutes.do(self.refresh_http_token)

        schedule.every(120).seconds.do(self.upload_to_datalake)
        schedule.every(120).seconds.do(self.execute_file_upload)

        print("Time synchronization after initialization: " + self.get_current_timestamp())

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
                # _thread.start_new_thread(self.executeCommand,  [cmd_data])
                self.execute_command(cmd_data)
                # Start a new thread as on return of this method the receive is ack'd.
                # If not done same message is read multiple times.
            elif "agm_v3" in topic_name:
                print("Message received from Agent Management")
                self.display_lines("Agent token", "received")
                self.http_token = cmd_data["data"]["access_token"]
                print("Token extracted : " + self.http_token)
            elif "amo_v4" in topic_name:
                print("Message received from Asset Modeler")
                self.display_lines("Asset Modeler", "status message", "received.")

                if "i/amo_v4/mip" in topic_name:
                    print("Received Model Instantiation Response")
                    self.execute_model_instantiation_response(cmd_data)

                elif "i/amo_v4/m" in topic_name:
                    self.print_success("Received Model Response")
                    self.execute_get_model_response(cmd_data)
                else:
                    print("Displaying general Response")
                    serialized_response = json.dumps(cmd_data, indent=1)
                    print(serialized_response)

            else:
                print("Unknown message received, skipping execution.")
        except Exception as e:
            print("Trouble to receive from mqtt : " + str(e))


    def configure_topics(self):
        # Publish and Subscribe topic details

        self.subscribe_topic = "tc/" + self.tenant + "/" + self.clientId + "/i/cmd_v3/c"
        self.publish_topic = "tc/" + self.tenant + "/" + self.clientId + "/o/cmd_v3/u"

        self.timeseries_publish_topic = "tc/" + self.tenant + "/" + self.clientId + "/o/mc_v3/ts"
        self.event_publish_topic = "tc/" + self.tenant + "/" + self.clientId + "/o/mc_v3/e"
        self.file_publish_topic = "tc/" + self.tenant + "/" + self.clientId + "/o/mc_v3/f"

        self.token_req_publish_topic = "tc/" + self.tenant + "/" + self.clientId + "/o/agm_v3/t"
        self.token_subscribe_topic = "tc/" + self.tenant + "/" + self.clientId + "/i/agm_v3/tr"

        self.model_instance_publish_topic = "tc/" + self.tenant + "/" + self.clientId + "/o/amo_v4/mi"
        self.model_instance_prog_subscribe_topic = "tc/" + self.tenant + "/" + self.clientId + "/i/amo_v4/mip"
        self.model_get_publish_topic = "tc/" + self.tenant + "/" + self.clientId + "/o/amo_v4/m"
        self.model_get_subscribe_topic = "tc/" + self.tenant + "/" + self.clientId + "/i/amo_v4/m"

        if self.platform == 'PVTCLOUD':
            self.user_name = "_CertificateBearer"
            try:
                self.tenantCertPath = self.config['TENANT_CERT_PATH']
            except Exception as e:
                print("Configure the `TENANT_CERT_PATH` variable in config. Terminating !!!", e)
                exit(1)


    def check_agent_state(self):
        if not os.path.exists(self.agent_state_conf):
            with open(self.agent_state_conf, 'w') as configfile:
                config_override = configparser.ConfigParser()
                config_override['DEFAULT'] = {
                    'model-synchronization-state': ModelState.INITIATE_MODEL_DOWNLOAD.name,
                    'model-synchronization-sha': '',
                    'model-synchronization-status': ''
                }
                config_override.write(configfile)

                print("Created Agent state config file")
        else:
            print("Agent state config file already present.")
            self.state_config.read(self.agent_state_conf)
            state = self.state_config['DEFAULT']['model-synchronization-state']
            sha = self.state_config['DEFAULT']['model-synchronization-sha']
            status = self.state_config['DEFAULT']['model-synchronization-status']
            print(f'Agent State Config : \n\t model-synchronization-state : '
                  f'{state} \n\t model-synchronization-sha : {sha} \n\t model-synchronization-status : {status}')

            if status == ModelState.MODEL_SYNCHRONIZATION_SUCCESSFUL.name:
                self.instance_exist = True
                GPIO.output(self.red_gpio, GPIO.LOW)
                GPIO.output(self.yellow_gpio, GPIO.HIGH)
            else:
                GPIO.output(self.red_gpio, GPIO.HIGH)
                GPIO.output(self.yellow_gpio, GPIO.LOW)
            self.print_state(state)

    def display_lines(self, *argv):
        line = ""
        for arg in argv:
            line = line + " " + arg
        print(line)

    def print_error(self, message):
        print("\033[91m \t Error : " + message + "\033[0m")

    def print_success(self, message):
        print("\033[92m \t Success : " + message + "\033[0m")

    def print_state(self, message):
        print("\033[94m \t State : " + message + "\033[0m")

    def execute_model_instantiation_response(self, cmd_data):
        print("processing execute_model_instantiation_response")
        serialized_response = json.dumps(cmd_data, indent=1)
        print(serialized_response)

        if cmd_data["data"]["status"] == "Success":
            self.display_lines("Model Instantiation", "successful.")
            self.print_success("*** MODEL INSTANTIATION SUCCESSFUL ***")
            self.update_state_in_agent_state_conf(ModelState.MODEL_SYNCHRONIZATION_SUCCESSFUL)
            self.store_model_sync_status_in_agent_state_conf(ModelState.MODEL_SYNCHRONIZATION_SUCCESSFUL)
            self.instance_exist = True
        elif cmd_data["data"]["status"] == "Failed":
            self.display_lines("Instance creation", "Failed.")
            self.print_error("*** FAILURE IN MODEL INSTANTIATION ***")
            self.print_error(serialized_response)
            print("Error while processing model : ", cmd_data["data"]["errors"])
            self.update_state_in_agent_state_conf(ModelState.MODEL_SYNCHRONIZATION_FAILED)
            self.store_model_sync_status_in_agent_state_conf(ModelState.MODEL_SYNCHRONIZATION_FAILED)
            self.instance_exist = False
        else:
            print("Instance Creation is in progress, waiting to complete ...")
            self.display_lines("Instance creation", "in progress.", "Waiting ...")

    def execute_get_model_response(self, cmd_message):
        print("Displaying Model Response")
        serialized_response = json.dumps(cmd_message, indent=1)

        print(serialized_response)

        model_download_url = cmd_message["data"]["content"]

        response = requests.get(model_download_url)
        print("Model download URL Response : ", response.text)
        model_response = json.loads(response.text)

        self.asset_model_helper.write_cloud_model_response(model_response)

        self.state_config.read(self.agent_state_conf)
        state = self.state_config['DEFAULT']['model-synchronization-state']
        if state == ModelState.INITIATED_MODEL_DOWNLOAD.name:
            self.update_state_in_agent_state_conf(ModelState.MODEL_DOWNLOADED)
        print("Cloud data model save in file." + self.asset_model_helper.cloud_model_file_name)

    def execute_command(self, cmd_message):
        print("Execute Command starting ...!")
        command_payload_json = cmd_message["data"]["payload"]
        command = self.extract_json_object(command_payload_json)
        commandType = command["commandType"]
        if commandType == "firmware_update":
            self.execute_firmware_update(cmd_message)
        elif commandType == "actuator_control":
            self.execute_actuator_command(cmd_message)
        elif commandType == "light_control":
            self.execute_light_control_cmd(cmd_message)
        elif commandType == "file_control":
            self.upload_to_datalake()
            self.send_execution_success_response(cmd_message)
        elif commandType == "model_download":
            self.send_model_download_message()
            sleep(3)
            self.send_execution_success_response(cmd_message)
        else:
            print("Invalid Command")

    def execute_actuator_command(self, cmd_message):
        print("Actuator Command Entry point")
        request_id = cmd_message["id"]
        job_id = cmd_message["data"]["jobId"]
        command_payload_json = cmd_message["data"]["payload"]
        command = self.extract_json_object(command_payload_json)
        execute_command = command["execute_command"]
        response = "Successfully Executed " + execute_command
        status = "EXECUTED"

        self.display_lines("Actuator CMD", "Command : ", execute_command, "Processing...")
        sleep(3)

        if execute_command == 'START':
            self.motor.start(95)
            GPIO.output(self.in1, GPIO.HIGH)
            GPIO.output(self.in2, GPIO.LOW)
            print("Executing Actuator Command : START")

        elif execute_command == 'STOP':
            GPIO.output(self.in1, GPIO.LOW)
            GPIO.output(self.in2, GPIO.LOW)
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
                "timestamp": self.get_current_timestamp(),
                "jobId": job_id,
                "status": status,
                "response": {"message": response}
            }

        }
        try:
            self.connection.publish(self.publish_topic, json.dumps(cmd_response), qos=0)
        except Exception as Argument:
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
        command = self.extract_json_object(command_payload_json)
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
                "timestamp": self.get_current_timestamp(),
                "jobId": job_id,
                "status": status,
                "response": {"message": response}
            }

        }
        try:
            self.connection.publish(self.publish_topic, json.dumps(cmd_response), qos=0)
        except Exception as Argument:
            print("Error publishing data : " + str(Argument))
            print("Connection Lost , trying to connect again.")
            self.establish_connection()

        self.display_lines("Light Ctrl CMD", "Command : ", execute_command, "Processed !!!")
        print("sending command response to topic : " + self.publish_topic)
        print("sending command response payload : " + json.dumps(cmd_response))

    def send_execution_success_response(self, cmd_message):
        print("Sending Execution Completion Response")
        request_id = cmd_message["id"]
        job_id = cmd_message["data"]["jobId"]
        response = "Successfully Executed "

        cmd_response = {
            "id": str(uuid.uuid4()),
            "requestId": request_id,
            "data": {
                "timestamp": self.get_current_timestamp(),
                "jobId": job_id,
                "status": "EXECUTED",
                "response": {"message": response}
            }
        }

        try:
            self.connection.publish(self.publish_topic, json.dumps(cmd_response), qos=0)
        except Exception as Argument:
            print("Error publishing data : " + str(Argument))
            print("Connection Lost , trying to connect again.")
            self.establish_connection()

        print("sending command response to topic : " + self.publish_topic)
        print("sending command response payload : " + json.dumps(cmd_response))


    def execute_firmware_update(self, cmd_message):
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
                    "timestamp": self.get_current_timestamp(),
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
                print("Error publishing data : " + str(Argument))
                print("Connection Lost , trying to connect again.")
                self.establish_connection()
            print("sending command response to topic : " + self.publish_topic)
            print("sending command response payload : " + json.dumps(cmd_response))
        self.display_lines("Firmware Update", "Complete")
        print("Firmware Update Complete Turning OFF LED")

    def execute_invalid_command_response(self, cmd_message):
        request_id = cmd_message["id"]
        job_id = cmd_message["data"]["jobId"]
        cmd_response = {
            "id": str(uuid.uuid4()),
            "requestId": request_id,
            "data": {
                "timestamp": self.get_current_timestamp(),
                "jobId": job_id,
                "status": "FAILED",
                "response": {"message": "Invalid Command Type Entered."}
            }
        }
        try:
            self.connection.publish(self.publish_topic, json.dumps(cmd_response), qos=0)
        except Exception as Argument:
            print("Error publishing data : " + str(Argument))
            print("Connection Lost , trying to connect again.")
            self.establish_connection()
        print("sending command response to topic : " + self.publish_topic)
        print("sending command response payload : " + json.dumps(cmd_response))

    def get_current_timestamp(self):
        date_now = datetime.datetime.now()
        curr_date_time = date_now.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        return curr_date_time

    def extract_json_object(self, command_payload_json):
        # command = json.loads(command_payload_string.replace('\\', ''))
        # command = json.loads(command_payload_string)
        return command_payload_json

    # def on_log(client, userdata, level, msg):
    #    print(msg.topic+" "+str(msg.payload))

    def insert_timeseries_callback(self):
        print("Timeseries timer callback triggered")
        GPIO.output(self.led_gpio, GPIO.HIGH)
        self.insert_standard_timeseries()
        if self.use_custom_timeseries:
            self.insert_custom_timeseries()
        GPIO.output(self.led_gpio, GPIO.LOW)

    def insert_custom_timeseries(self):
        print("Custom Time Series called")
        moisture = int(uniform(60.0, 65.0))

        if not self.instance_exist:
            print("The instance is not created so skipping the data ingestion.")
            return

        print(u"Custom Moisture: {:g}%".format(moisture))

        self.display_lines("Sending Data", u"Moisture: {:g}%".format(moisture))

        curr_date_time = self.get_current_timestamp()

        timeseries_file = open(self.custom_timeseries_json_file, 'r')
        timeseries_contents = timeseries_file.read()
        timeseries_contents = timeseries_contents.replace("<curr_date_time>", curr_date_time)
        timeseries_contents = timeseries_contents.replace("<moisture>", str(moisture))
        timeseries_json = json.loads(timeseries_contents)

        try:
            self.connection.publish(self.custom_timeseries_topic, json.dumps(timeseries_json), qos=0)
        except Exception as Argument:
            print("Error publishing data : " + str(Argument))
            print("Connection Lost , trying to connect again.")
            self.establish_connection()
        print("sending time series data : " + self.custom_timeseries_topic)
        print("Sent timeseries payload : " + json.dumps(timeseries_json))

    def insert_standard_timeseries(self):
        print("Standard Time Series called")
        temperature = int(uniform(30.0, 32.0))
        humidity = int(uniform(50.0, 55.0))
        pressure = int(uniform(20.0, 25.0))

        if not self.instance_exist:
            print("The instance is not created so skipping the data ingestion.")
            return

        try:
            while 1 == 1:
                result = self.dht_sensor.read()
                humidity = round(result["humidity"], 2)
                temperature = round(result["temp_c"], 2)
                valid = result["valid"]
                if valid == True:
                    break
                print("Invalid Value read from Sensor1, reading again in 2 seconds.")
                sleep(2)
        except Exception as Argument:
            print("Error publishing data : " + str(Argument))
            print("Connection Lost , trying to connect again.")

        print(u"Temperature: {:g}\u00b0C, Humidity: {:g}%".format(temperature, humidity))

        self.display_lines("Sending Data",
                           u"Temperature: {:g}C".format(temperature), u"Humidity: {:g}%".format(humidity))

        curr_date_time = self.get_current_timestamp()

        timeseries_file = open(self.timeseries_json_file, 'r')
        timeseries_contents = timeseries_file.read()
        timeseries_contents = timeseries_contents.replace("<curr_date_time>", curr_date_time)
        timeseries_contents = timeseries_contents.replace("<temperature>", str(temperature))
        timeseries_contents = timeseries_contents.replace("<humidity>", str(humidity))
        timeseries_json = json.loads(timeseries_contents)

        try:
            self.connection.publish(self.timeseries_publish_topic, json.dumps(timeseries_json), qos=0)
        except Exception as Argument:
            print("Error publishing data : " + str(Argument))
            print("Connection Lost , trying to connect again.")
            self.establish_connection()
        print("sending time series data : " + self.timeseries_publish_topic)
        print("Sent timeseries payload : " + json.dumps(timeseries_json))

    def check_interrupt_timer_callback(self, t):
        print("checking interrupt.")
        self.infrared_sensor_interrupt()

    def rotary_callback(self, counter):
        print("General rotation")
        print("Counter value: ", counter)

    def sw_short(self):
        print("Switch pressed")


    def up_callback(self, counter):
        print("Up rotation")
        print("Counter value: ", counter)


    def down_callback(self, counter):
        print("Down rotation")
        print("Counter value: ", counter)

    def refresh_http_token(self):
        print("Refresh Http Token Called")
        print("Refresh Http Token Called")

        token_req_id = str(uuid.uuid4())
        token_payload = {
            "id": str(token_req_id),
        }

        try:
            self.connection.publish(self.token_req_publish_topic, json.dumps(token_payload), qos=0)
        except Exception as Argument:
            print("Error publishing data : " + str(Argument))
            print("Connection Lost , trying to connect again.")
            self.establish_connection()
        print("sent token generation request : " + self.token_req_publish_topic)
        print("Sent token generation payload : " + json.dumps(token_payload))

    def infrared_sensor_interrupt(self):
        print('Infrared Sensor Event Triggered')
        self.display_lines("Motion Sensor", "INTERRUPT !!!  : ", "Shut Down JetPump.")
        sleep(1)
        #print("Stopping Actuator due to Interrupt.")
        #GPIO.output(self.in1, GPIO.LOW)
        #GPIO.output(self.in2, GPIO.LOW)

        curr_date_time = self.get_current_timestamp()
        severity = randint(2, 4) * 10
        id = uuid.uuid4()

        event_file = open(self.event_json_file, 'r')
        event_contents = event_file.read()
        event_contents = event_contents.replace("<uuid>", str(id))
        event_contents = event_contents.replace("<uuid_hex>", str(id.hex))
        event_contents = event_contents.replace("<curr_date_time>", curr_date_time)
        event_contents = event_contents.replace("<severity>", str(severity))
        event_json = json.loads(event_contents)

        try:
            self.connection.publish(self.event_publish_topic, json.dumps(event_json), qos=0)
        except Exception as Argument:
            print("Error publishing data : " + str(Argument))
            print("Connection Lost , trying to connect again.")
            self.establish_connection()
        print("Sending Event data to topic : " + self.event_publish_topic)
        print("Sent Event data payload : " + json.dumps(event_json))

    def execute_file_upload(self):
        print('File Upload Event Triggered')
        self.display_lines("File Upload", "Triggered !!!")
        curr_date_time = self.get_current_timestamp()
        severity = randint(2, 4) * 10
        unique_id = uuid.uuid4()

        file_content = curr_date_time + " INFO: Connection Successfully Established to broker mindconnect.eu1-int.mindsphere.io\n" + \
                       curr_date_time + " INFO: Subscribed to topic : tc/tenant/tenant_mqttagent/i/cmd_v3/c\n" + \
                       curr_date_time + " INFO: Checking and creating asset model instance.\n" + \
                       curr_date_time + " INFO: The Asset Instance Already exist so skipping Instance Creation.\n" + \
                       curr_date_time + " INTERRUPT : Infrared Sensor Event Triggered\n" + \
                       curr_date_time + " INFO: Sent Event data payload\n" + \
                       curr_date_time + " INFO: File Upload Event Triggered\n" + \
                       curr_date_time + " INFO: Sending File Contents to topic : tc/tenant/tenant_mqttagent/o/mc_v3/f\n" + \
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

        if self.http_token == "":
            print("No http token yet so returning")
            return

        auth_headers = {
            'Content-Type': 'application/json',
            "Authorization": "Bearer " + self.http_token
        }

        asset_response = requests.get(self.gateway_url + '/api/assetmanagement/v3/assets?'
                                                         'filter={\"externalId\":\"' + self.clientId + '\"}',
                                      headers=auth_headers)
        print(asset_response.text)
        asset_response_json = json.loads(asset_response.text)
        assetId = asset_response_json["_embedded"]["assets"][0]["assetId"]
        print("asset id : " + assetId)
        curr_date_time = self.get_current_timestamp()

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

        #amol camera
        #image_location = './analytics/quality-snapshot.jpg'
        # try:
        #    self.camera.start_preview()
        #    sleep(3)
        #    self.camera.capture(image_location)
        #    self.camera.stop_preview()
        # except Exception as Argument:
        #    sys.print_exception(Argument)
        #    print("Error capturing Camera Image : " + str(Argument))
        #    print("falling back to default image ")
        #    image_location = "./analytics/fallback_image.jpeg"

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

        if self.platform == 'PVTCLOUD':
            jwt_token = self.generate_jwt_token()
            print("user_name : " + self.user_name)
            print("jwt_token : " + jwt_token)

            self.connection.username_pw_set(self.user_name, jwt_token)
            print("Connecting to Rabbitmq via JWT token.")
            self.connection.tls_set(self.caPath, cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLSv1_2,
                                    ciphers=None)
        else:
            self.connection.tls_set(self.caPath, certfile=self.certPath, keyfile=self.keyPath,
                                cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLSv1_2,
                                ciphers=None)

        print("waiting for connection...")

        try:
            self.connection.connect(self.broker_host, self.broker_port, keepalive=60)
            self.connection.loop_start()
        except Exception as Argument:
            print("Error establishing connection : " + str(Argument))

        print("Connection Successfully Established to broker {}".format(self.broker_host))
        self.display_lines("MQTT Connection", "Established", "to MindSphere")

    def generate_jwt_token(self):
        print("Generating JWT Token for RabbitMQ MQTT Broker...")

        try:
            device_private_key = open(self.keyPath, 'r').read()

            device_cert_single_line = self.get_cert_single_line(self.certPath)
            print("device_cert_single_line", device_cert_single_line)

            tenant_cert_single_line = self.get_cert_single_line(self.tenantCertPath)
            print("tenant_cert_single_line", tenant_cert_single_line)

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

            encoded_jwt = jwt.encode(claim, private_key_int, algorithm="RS256", headers=headers)
            print("jwt token for secret : " + str(encoded_jwt))
            decoded_jwt = jwt.decode(encoded_jwt, private_key_int, algorithms=["RS256"], options={"verify_signature": False})

            serialized = json.dumps(headers, sort_keys=True, indent=3)
            print("Decoded Jwt Headers :\n" + serialized)
            serialized = json.dumps(decoded_jwt, sort_keys=True, indent=3)
            print("Claims :\n" + serialized)

            return encoded_jwt
        except Exception as e:
            print("Error generating JWT for PVTCOUD broker connection. Check the certificates configuration.", e)
            exit(1)

    def get_cert_single_line(self, cert_path):
        cert_file = open(cert_path, 'r')
        cert_lines = cert_file.readlines()
        cert_lines = cert_lines[1:-1]
        device_cert_single_line = "".join(cert_lines).replace('\n', '').replace('\r', '')
        return device_cert_single_line


    def on_connect(self, client, userdata, flags, rc, properties):
        print("Connection returned result: " + str(rc))
        # Subscribing in on_connect() means that if we lose the connection and
        # reconnect then subscriptions will be renewed.
        if rc != 0: return
        self.connected_flag = True

        client.subscribe([(self.subscribe_topic, 1), (self.token_subscribe_topic, 1),
                          (self.model_instance_prog_subscribe_topic, 1), (self.model_get_subscribe_topic, 1)])

        print("Subscribed to topic : " + self.subscribe_topic)
        print("Subscribed to topic : " + self.token_subscribe_topic)
        print("Subscribed to topic : " + self.model_instance_prog_subscribe_topic)
        print("Subscribed to topic : " + self.model_get_subscribe_topic)

    def update_sha_in_agent_state_conf(self):
        sha = self.asset_model_helper.sha256sum()
        self.state_config.read(self.agent_state_conf)
        self.state_config.set('DEFAULT', 'model-synchronization-sha',
                              str(sha))
        with open(self.agent_state_conf, 'w') as configfile:
            self.state_config.write(configfile)

    def update_state_in_agent_state_conf(self, state_enum):
        print("Updating model state to : ", state_enum)
        self.state_config.read(self.agent_state_conf)
        self.state_config.set('DEFAULT', 'model-synchronization-state',
                              str(state_enum.name))
        with open(self.agent_state_conf, 'w') as configfile:
            self.state_config.write(configfile)
        self.print_state(state_enum.name)

    def store_model_sync_status_in_agent_state_conf(self, state_enum):
        print("Updating model state to : ", state_enum)
        self.state_config.read(self.agent_state_conf)
        self.state_config.set('DEFAULT', 'model-synchronization-status',
                              str(state_enum.name))
        with open(self.agent_state_conf, 'w') as configfile:
            self.state_config.write(configfile)
        self.print_state(state_enum.name)


    def monitor_asset_modeler_states(self):
        # print("Monitoring asset modeler states")
        self.state_config.read(self.agent_state_conf)
        state = self.state_config['DEFAULT']['model-synchronization-state']
        state_config_sha = self.state_config['DEFAULT']['model-synchronization-sha']
        if state == ModelState.INITIATE_MODEL_DOWNLOAD.name:
            GPIO.output(self.red_gpio, GPIO.HIGH)
            GPIO.output(self.yellow_gpio, GPIO.LOW)
            self.send_model_download_message()
            self.update_state_in_agent_state_conf(ModelState.INITIATED_MODEL_DOWNLOAD)
        elif state == ModelState.MODEL_DOWNLOADED.name:
            print("Model downloaded so generating local payload")
            self.asset_model_helper.load_cloud_model()
            print("=========\n")
            self.asset_model_helper.load_local_model()
            print("=========\n")
            self.asset_model_helper.populate_reference_ids_in_model()

            print("Model payload generated")

            model = self.asset_model_helper.get_model_payload_to_send()
            serialized = json.dumps(model, sort_keys=True, indent=3)
            print(serialized)
            self.connection.publish(self.model_instance_publish_topic, json.dumps(model), qos=0)

            self.update_state_in_agent_state_conf(ModelState.UPDATE_MODEL_TRIGGERED)
            print('sent to model instantiation topic : ' + self.model_instance_publish_topic)
        elif state == ModelState.MODEL_SYNCHRONIZATION_SUCCESSFUL.name:
            print("Success: Model in synchronized state")
            self.update_sha_in_agent_state_conf()
            self.send_model_download_message()
            self.update_state_in_agent_state_conf(ModelState.MODEL_SYNCHRONIZATION_FINISHED)
            GPIO.output(self.red_gpio, GPIO.LOW)
            GPIO.output(self.yellow_gpio, GPIO.HIGH)
        elif state == ModelState.MODEL_SYNCHRONIZATION_FAILED.name:
            print("Failed: Model in synchronized state with failures")
            self.update_sha_in_agent_state_conf()
            self.update_state_in_agent_state_conf(ModelState.MODEL_SYNCHRONIZATION_FINISHED)
            GPIO.output(self.red_gpio, GPIO.HIGH)
            GPIO.output(self.yellow_gpio, GPIO.LOW)
        elif state == ModelState.MODEL_SYNCHRONIZATION_FINISHED.name:
            local_model_file_sha = self.asset_model_helper.sha256sum()
            if state_config_sha != local_model_file_sha:
                print("Local Model file changed so starting Model Synchronization from beginning !!!")
                self.update_state_in_agent_state_conf(ModelState.INITIATE_MODEL_DOWNLOAD)
        else:
            print("Intermediate/Unknown synchronization state !!!")

    def send_model_download_message(self):
        print("Sending cloud model download message")
        print("Getting Asset Model from cloud")
        instance = {
            "id": str(uuid.uuid4())
        }
        serialized = json.dumps(instance, sort_keys=True, indent=3)
        print(serialized)
        self.connection.publish(self.model_get_publish_topic, json.dumps(instance), qos=0)
        print('sent to model get topic : ' + self.model_get_publish_topic)

    # **************************************#
    # Main loop
    def start_device_connection(self):
        first = True
        try:
            while 1 == 1:
                sleep(1)
                try:
                    schedule.run_pending()
                except Exception as e:
                    print("Error running pending schedulers ", e)
                if self.connected_flag:
                    print("Connected !!!")
                    GPIO.output(self.green_gpio, GPIO.HIGH)
                    sleep(10)
                    curr_date_time = self.get_current_timestamp()
                    print(curr_date_time + " Connected !!!")
                    if first:
                        print("Sending initial token generation request")
                        self.refresh_http_token()
                        first = False
                    self.monitor_asset_modeler_states()
        except KeyboardInterrupt:
            print("\nExiting gracefully...")
            self.connection.disconnect()
            self.connection.loop_stop()

        except Exception as e:
            # this catches ALL other exceptions including errors.
            # You won't get any error messages for debugging
            # so only use it once your code is working
            print("Error running main loop " + str(e))
            self.connection.disconnect()
            self.connection.loop_stop()

        finally:
            GPIO.cleanup()

env = "AWS"
#env = "RANCHER_INT"

print("Loading Config file for Environment " + env)
loadedConfig = config_parser.parse(env, 'configs/mqtt-config.json')
iotService = IotService(loadedConfig)
