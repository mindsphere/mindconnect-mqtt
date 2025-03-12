#!/usr/bin/python

# this program will subscribe and send messages to mqtt topics

import os
import socket
import ssl
from time import sleep
from random import uniform
import json
import logging
import datetime
import uuid
from lib import config_parser
import PySimpleGUI as sg
import requests
import base64
import subprocess

logging.basicConfig(level=logging.DEBUG)


class CommandControl:
    def __init__(self, config):
        self.config = config
        self.gateway_url = config['GATEWAY_URL']
        self.app_id = config['APP_ID']
        self.app_secret = config['APP_SECRET']
        self.token = ''
        self.job_id = ''
        self.command_id = ''
        self.tenant_id = ''
        self.get_bearer_token()

        self.create_window()
        self.start_read_loop()

    def get_bearer_token(self):
        print("Acquiring access token...")
        url = self.config['GATEWAY_URL'] + "/api/technicaltokenmanager/v3/oauth/token"
        split_ids = self.app_id.split("-")
        self.tenant_id = split_ids[0]

        body = {
            "grant_type": "client_credentials",
            "appName": split_ids[1],
            "appVersion": split_ids[2],
            "hostTenant": split_ids[0],
            "userTenant": split_ids[0]
        }

        secret_base64 = base64.b64encode(bytes(self.app_id + ":" + self.app_secret, 'utf-8'))
        headers = {
            "Content-Type": "application/json",
            "X-SPACE-AUTH-KEY": "Bearer " + secret_base64.decode("utf-8")
        }

        serialized = json.dumps(body, sort_keys=True, indent=3)
        print("Token Request Body : " + serialized)
        print("url" + url)
        try:
            response = requests.post(url, data=serialized, headers=headers)
            print("Token Response Text : ", response.text)
            oauth_response = json.loads(response.text)
            self.token = oauth_response["access_token"]
            print("App Token : ", self.token)
        except Exception:
            print("Cannot acquire access token. Exiting")
            exit(-1)


    def create_window(self):
        font = ("Courier New", 12)
        legend_font = ("Arial", 12)
        button_font = ("Arial", 11)
        background = '#b3d9ff'
        #sg.theme('DarkTeal7')
        sg.theme('DarkTeal9')
        sg.SetOptions(
            # background_color=background,
            # element_background_color=background,
            # text_element_background_color=background,
            window_location=(200, 2),
            margins=(5, 5),
            # text_color= 'Black',
            # input_text_color='Black',
            # button_color=('Black', 'gainsboro')
        )

        self.layout = [
            [sg.Text("MindConnect JARVIS", key="TITLE", font=("Arial", 13, 'bold'))],
            [sg.Text("Enter Client Id: ", font=legend_font), sg.InputText(key='-CLIENT-ID-'),
             sg.Button("Generate Certificate", font=button_font)],
            [sg.HSeparator()],
            [sg.Text("Control Commands : ", font=legend_font), sg.Button("JetPump On", font=button_font),
             sg.Button("JetPump Off", font=button_font),
             sg.Button("Trigger Download Model", font=button_font)],
            [sg.HSeparator()],
            [sg.Text("Configure your Own Data Format: ", font=legend_font),
             sg.Button("Create Topic and Data Mapping", font=button_font)],
            [sg.HSeparator()],
            [sg.Text("Response: ", font=legend_font)],
            [sg.Multiline("", size=(80, 15), key="-DEL-JOB-RESPONSE-", font=font)],
            [sg.Text("Command Execution Status: ", font=legend_font), sg.Button("Check Status", font=button_font),
             sg.Button("Check Command Response", font=button_font)],
            [sg.Multiline("", size=(80, 15), key="-EXEC-STATUS-", font=font)]
        ]

        # Create the window
        self.window = sg.Window("InsightsHub", self.layout, resizable=False)

    def create_custom_topic_and_mapping(self):
        if self.clientId == "":
            self.window['-DEL-JOB-RESPONSE-'].update("Please enter client id !")
            return

        print("Fetching asset for mapping")
        headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer " + self.token
        }

        agents_url = self.config['GATEWAY_URL'] + "/api/agentmanagement/v3/agents?filter={%22mqttClientId%22:%22" + self.clientId + "%22}"

        response = requests.get(agents_url, headers=headers)
        print("Get Agent Response Text : ", response.text)
        assets_response = json.loads(response.text)
        agent_asset_id = assets_response["content"][0]["entityId"]

        assets_url = self.config['GATEWAY_URL'] + "/api/assetmanagement/v3/assets?basicFieldsOnly=true&filter={%22parentId%22:%22" + agent_asset_id + "%22}"

        response = requests.get(assets_url, headers=headers)
        print("Get Assets Response Text : ", response.text)
        assets_response = json.loads(response.text)
        data_asset_id = ""
        try:
            data_asset_id = assets_response["_embedded"]["assets"][0]["assetId"]
        except Exception as e:
            print("Error getting data asset id ", e)

        if data_asset_id == "":
            print("Error in fetching data asset so returning")
            return

        asset_aspects_url = self.config['GATEWAY_URL'] + "/api/assetmanagement/v3/assets/" + data_asset_id + "/aspects"

        response = requests.get(asset_aspects_url, headers=headers)
        print("Get Assets Response Text : ", response.text)
        aspects_response = json.loads(response.text)

        try:
            aspectName = ""
            found = False
            if "aspects" in aspects_response["_embedded"]:
                for aspect in aspects_response["_embedded"]["aspects"]:
                    aspectName = aspect["name"]
                    if "variables" in aspect:
                        for variable in aspect["variables"]:
                            if variable["name"] == "moisture":
                                found = True
                                break
                    if found:
                        break
        except Exception as e:
            print("Error finding aspect and variable 'moisture' for mapping", e)
            self.window['-DEL-JOB-RESPONSE-'].update("Error finding aspect and variable 'moisture' for mapping")
            return

        if not found:
            print("Error finding aspect and variable 'moisture' for mapping")
            self.window['-DEL-JOB-RESPONSE-'].update("No aspect with variable 'moisture' for mapping")
            return

        print("Create custom topic started")
        url = self.config['GATEWAY_URL'] + "/api/mindconnectmqtt/v3/customTopicRegistrations"
        body = {
            "mqttClientId": self.clientId,
            "topic": "clouddatastore/" + self.clientId + "/ts"
        }

        serialized = json.dumps(body, sort_keys=True, indent=3)
        print("Custom topic body : " + serialized)

        response = requests.post(url, data=serialized, headers=headers)

        print("Create topic Response Text : ", response.text)
        topic_response = json.loads(response.text)
        serialized_response = json.dumps(topic_response, indent=1)
        self.window['-DEL-JOB-RESPONSE-'].update(serialized_response)
        if response.status_code == 201:
            custom_topic_id = topic_response["id"]
        elif response.status_code == 409:
            url = url + "?mqttClientId=" + self.clientId
            response = requests.get(url, headers=headers)
            print("Get response for custom topics " + response.text)
            topic_response = json.loads(response.text)
            if response.status_code == 200:
                custom_topic_id = topic_response[0]["id"]

        print("Custom topic id : ", custom_topic_id)

        print("Create custom mappings for topic")
        mappings_url = (self.config['GATEWAY_URL'] + "/api/mindconnectmqtt/v3/customTopicRegistrations/"
                        + custom_topic_id + "/mappings")

        body = {
            "assetId": data_asset_id,
            "aspectName": aspectName,
            "variableName": "moisture",
            "variableValueExpression": "machines[].sources[].tags[?Key == 'moisture'].Values[][0]",
            "variableTimestampExpression": "machines[].sources[].tags[?Key == 'moisture'].timestamp[]",
            "timeConverter": "fromISO8601"
        }

        serialized = json.dumps(body, sort_keys=True, indent=3)
        print("Custom mappings body : " + serialized)

        response = requests.post(mappings_url, data=serialized, headers=headers)

        print("Create mappings Response Text : ", response.text)
        topic_response = json.loads(response.text)
        serialized_response = json.dumps(topic_response, indent=1)
        self.window['-DEL-JOB-RESPONSE-'].update(serialized_response)


    def submit_delivery_job(self, data):
        print("Started Delivery Job Submit")
        if self.clientId == "":
            self.window['-DEL-JOB-RESPONSE-'].update("Please enter client id !")
            return

        url = self.config['GATEWAY_URL'] + "/api/commanding/v3/deliveryJobs"
        headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer " + self.token
        }
        body = {
            "name": "Delivery Job " + str(uuid.uuid4()),
            "clientIds": [self.clientId],
            "data": data
        }

        serialized = json.dumps(body, sort_keys=True, indent=3)
        print("Delivery Job Body : " + serialized)

        response = requests.post(url, data=serialized, headers=headers)
        print("Delivery Job Response Text : ", response.text)
        job_response = json.loads(response.text)
        serialized_response = json.dumps(job_response, indent=1)
        self.window['-DEL-JOB-RESPONSE-'].update(serialized_response)
        self.job_id = job_response["id"]
        print("Instance Id : ", self.job_id)

    def get_current_timestamp(self):
        date_now = datetime.datetime.now()
        curr_date_time = date_now.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        return curr_date_time

    def submit_generate_agent_certificate(self):
        print("Started Agent Certificate Submit")
        if self.clientId == "":
            self.window['-DEL-JOB-RESPONSE-'].update("Please enter client id !")
            return

        url = self.config['GATEWAY_URL'] + "/api/mindconnectmqtt/v3/agentCertificates"
        headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer " + self.token
        }

        certificate_name = self.clientId.split("_")[1]

        body = {
            "name": certificate_name,
            "owner": "apiuser@" + self.tenant_id + ".com"
        }

        serialized = json.dumps(body, sort_keys=True, indent=3)
        print("Agent Certificate Request Body : " + serialized)

        response = requests.post(url, data=serialized, headers=headers)
        print("Create Agent Certificate Response Text : ", response.text)
        job_response = json.loads(response.text)
        serialized_response = json.dumps(job_response, indent=1)
        self.window['-DEL-JOB-RESPONSE-'].update(serialized_response)
        clientId = job_response["clientId"]

        certificate_pem = job_response["credentials"]["certificatePem"].replace('\\n', '\n')
        print(certificate_pem)

        f = open("agent_cert\\" + clientId + ".pem", "w+")
        f.write(certificate_pem)
        f.close()

        private_key = job_response["credentials"]["privateKey"].replace('\\n', '\n')
        print(private_key)

        f = open("agent_cert\\" + clientId + ".key", "w+")
        f.write(private_key)
        f.close()

        print("Certificate created for client id : ", clientId)


    def check_command_status(self):
        print("Started Command Status check.")
        if self.job_id == "":
            self.window['-DEL-JOB-RESPONSE-'].update("Please fire a command first !")
            return

        url = self.config['GATEWAY_URL'] + "/api/commanding/v3/deliveryJobs/" + self.job_id + "/commands"
        headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer " + self.token
        }

        response = requests.get(url, headers=headers)
        print("Command Response Text : ", response.text)
        cmd_response = json.loads(response.text)
        self.command_id = cmd_response["_embedded"]["commands"][0]["id"]

        serialized_response = json.dumps(cmd_response, indent=1)
        self.window['-EXEC-STATUS-'].update(serialized_response)

    def check_command_id_response(self):
        print("Started get command by id")
        if self.job_id == "":
            self.window['-DEL-JOB-RESPONSE-'].update("Please fire a command first !")
            return

        if self.command_id == "":
            self.check_command_status()


        url = self.config[
                  'GATEWAY_URL'] + "/api/commanding/v3/deliveryJobs/" + self.job_id + "/commands/" + self.command_id
        headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer " + self.token
        }

        response = requests.get(url, headers=headers)
        print("Command by id Response Text : ", response.text)
        cmd_response = json.loads(response.text)
        serialized_response = json.dumps(cmd_response, indent=1)
        self.window['-EXEC-STATUS-'].update(serialized_response)

    def start_read_loop(self):
        while 1 == 1:
            # sleep(5)
            event, values = self.window.read()
            self.clientId = values['-CLIENT-ID-']
            if event == "Trigger Download Model":
                print("Executing Model download for device")
                data = {
                    "commandName": "Model Download",
                    "commandType": "model_download",
                    "execute_command": "ON"
                }

                self.submit_delivery_job(data)
                print('sent to execute command')

            elif event == "Generate Certificate":
                print("Entered Client Id : " + self.clientId)
                self.submit_generate_agent_certificate()

            elif event == "Log File Upload Command":
                print("Send Log File upload command")
                data = {
                    "commandName": "FileUpload Control",
                    "commandType": "file_control",
                    "execute_command": "UPLOAD"
                }

                self.submit_delivery_job(data)
                print('sent to execute command')
            elif event == "JetPump On":
                print("Executing JetPump On Flow")
                data = {
                    "commandName": "JetPump Control",
                    "commandType": "actuator_control",
                    "execute_command": "START"
                }

                self.submit_delivery_job(data)
                print('sent to execute command')

            elif event == "JetPump Off":
                print("Executing JetPump Off Flow")
                data = {
                    "commandName": "JetPump Control",
                    "commandType": "actuator_control",
                    "execute_command": "STOP"
                }

                self.submit_delivery_job(data)
                print('sent to execute command')
            elif event == "Check Status":
                print("Checking Delivery Job execution status")
                self.check_command_status()
                print('sent to check status')
            elif event == "Check Command Response":
                print("Checking command response")
                self.check_command_id_response()
                print('sent to check status')

            elif event == "Create Topic and Data Mapping":
                print("Executing Create Configuration Instance")
                self.create_custom_topic_and_mapping()
                print("Finished custom configuration.")
            elif event == sg.WIN_CLOSED:
                print("Closing window, operations completed.")
                break

        self.window.close()


env = "AWS_PROD"

print("Loading Config file for Environment " + env)
loadedConfig = config_parser.parse(env, 'configs/tech-user-config.json')
iotService = CommandControl(loadedConfig)
