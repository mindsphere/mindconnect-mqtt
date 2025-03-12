import json
import os
import uuid
from enum import Enum
import hashlib


class ModelState(Enum):
    INITIATE_MODEL_DOWNLOAD = 1
    INITIATED_MODEL_DOWNLOAD = 2
    MODEL_DOWNLOADED = 3
    UPDATE_MODEL_TRIGGERED = 4
    MODEL_SYNCHRONIZATION_SUCCESSFUL = 5
    MODEL_SYNCHRONIZATION_FAILED = 6
    MODEL_SYNCHRONIZATION_FINISHED = 7


class AssetModelHelper:
    def __init__(self, tenant):
        self.tenant = tenant
        absolute_project_path = os.path.dirname(__file__)
        self.cloud_model_file_name = os.path.join(absolute_project_path,
                                                  "../example_json/cloud_asset_model.json")
        self.local_model_file_name = os.path.join(absolute_project_path,
                                                  "../example_json/local_asset_model.json")
        self.model_create_json_payload = os.path.join(absolute_project_path,
                                                      "../example_json/generated_model_payload.json")

        self.cloud_asset_model, self.local_asset_model, self.local_asset_model_payload = {}, {}, {}
        self.assetTypes, self.aspectTypes, self.instanceModel, self.mappingModel = {}, {}, {}, {}
        self.loc_assetTypes, self.loc_aspectTypes, self.loc_instanceModel, self.loc_mappingModel = {}, {}, {}, {}

    def load_cloud_model(self):
        if os.path.exists(self.cloud_model_file_name):
            cloud_model_file = open(self.cloud_model_file_name)
            self.cloud_asset_model = json.load(cloud_model_file)

        if "typeModel" in self.cloud_asset_model:
            print("Cloud typeModel Exists")
            typeModel = self.cloud_asset_model["typeModel"]
            if "assetTypes" in typeModel:
                self.assetTypes = typeModel["assetTypes"]
                print("Cloud assetTypes Exists")

            if "aspectTypes" in typeModel:
                self.aspectTypes = typeModel["aspectTypes"]
                print("Cloud aspectTypes Exists")

        if "instanceModel" in self.cloud_asset_model and "assets" in self.cloud_asset_model["instanceModel"]:
            self.instanceModel = self.cloud_asset_model["instanceModel"]["assets"]
            print("Cloud instanceModel Exists")

        if "mappingModel" in self.cloud_asset_model and "mappings" in self.cloud_asset_model["mappingModel"]:
            self.mappingModel = self.cloud_asset_model["mappingModel"]["mappings"]
            print("Cloud mappingModel Exists")

    def load_local_model(self):
        if os.path.exists(self.local_model_file_name):
            local_model_file = open(self.local_model_file_name)
            asset_model_contents = local_model_file.read()
            asset_model_contents = asset_model_contents.replace("<tenantId>", self.tenant)
            asset_model_contents = asset_model_contents.replace("<uuid>", str(uuid.uuid4()))
            self.local_asset_model_payload = json.loads(asset_model_contents)

            if "data" in self.local_asset_model_payload:
                self.local_asset_model = self.local_asset_model_payload["data"]

        if "typeModel" in self.local_asset_model:
            print("Local typeModel Exists")
            typeModel = self.local_asset_model["typeModel"]
            if "assetTypes" in typeModel:
                self.loc_assetTypes = typeModel["assetTypes"]
                print("Local assetTypes Exists")

            if "aspectTypes" in typeModel:
                self.loc_aspectTypes = typeModel["aspectTypes"]
                print("Local aspectTypes Exists")

        if "instanceModel" in self.local_asset_model and "assets" in self.local_asset_model["instanceModel"]:
            self.loc_instanceModel = self.local_asset_model["instanceModel"]["assets"]
            print("Local instanceModel Exists")

        if "mappingModel" in self.local_asset_model and "mappings" in self.local_asset_model["mappingModel"]:
            self.loc_mappingModel = self.local_asset_model["mappingModel"]["mappings"]
            print("Local mappingModel Exists")

    def populate_reference_ids_in_model(self):
        print("Merging contents based on local model file")
        print("Mappings : earlier dict: ", self.loc_mappingModel)
        missing_datapoint_mappings = []
        for item in self.loc_mappingModel:
            mapping_found = False
            for item_inner in self.mappingModel:
                if item["dataPointId"] == item_inner["dataPointId"]:
                    item["referenceId"] = item_inner["referenceId"]
                    mapping_found = True
            if not mapping_found:
                item["referenceId"] = str(uuid.uuid4().hex)
                missing_datapoint_mappings.append(item)

        print("Mappings : added reference id: ", self.loc_mappingModel)
        print("Mapping not present in cloud found :", missing_datapoint_mappings)

        print("Instances : earlier dict: ", self.loc_instanceModel)
        for item in self.loc_instanceModel:
            for item_inner in self.instanceModel:
                found = False
                if item["name"] == item_inner["name"]:
                    item["referenceId"] = item_inner["referenceId"]
                    found = True
                if not found:
                    print("Skipping asset instance reference setting, expected in defined payload")
                    # item["referenceId"] = str(uuid.uuid4().hex)

        print("Instances : added reference id: ", self.loc_instanceModel)

        print("Asset Types : earlier dict: ", self.loc_assetTypes)
        for item in self.loc_assetTypes:
            found = False
            for item_inner in self.assetTypes:
                if item["id"] == item_inner["id"]:
                    item["referenceId"] = item_inner["referenceId"]
                    found = True
                    if "aspects" in item:
                        for loc_aspect in item["aspects"]:
                            asp_found = False
                            if "aspects" in item_inner:
                                for cld_aspect in item_inner["aspects"]:
                                    if cld_aspect["aspectTypeId"] == loc_aspect["aspectTypeId"]:
                                        loc_aspect["referenceId"] = cld_aspect["referenceId"]
                                        asp_found = True
                            if not asp_found:
                                loc_aspect["referenceId"] = str(uuid.uuid4().hex)
            if not found:
                item["referenceId"] = str(uuid.uuid4().hex)
                if "aspects" in item:
                    for loc_aspect in item["aspects"]:
                        loc_aspect["referenceId"] = str(uuid.uuid4().hex)

        print("Asset Types : added reference id: ", self.loc_assetTypes)

        print("Aspect Types : earlier dict: ", self.loc_aspectTypes)
        for item in self.loc_aspectTypes:
            found = False
            for item_inner in self.aspectTypes:
                if item["id"] == item_inner["id"]:
                    item["referenceId"] = item_inner["referenceId"]
                    found = True
                    if "variables" in item:
                        for loc_variable in item["variables"]:
                            loc_variable_found = False
                            if "variables" in item_inner:
                                for cld_variable in item_inner["variables"]:
                                    if cld_variable["name"] == loc_variable["name"]:
                                        loc_variable["referenceId"] = cld_variable["referenceId"]
                                        loc_variable_found = True
                            if not loc_variable_found:
                                loc_variable["referenceId"] = str(uuid.uuid4().hex)
            if not found:
                item["referenceId"] = str(uuid.uuid4().hex)
                if "variables" in item:
                    for loc_variable in item["variables"]:
                        loc_variable["referenceId"] = str(uuid.uuid4().hex)

        print("Aspect Types : added reference id: ", self.loc_aspectTypes)

        self.local_asset_model["typeModel"]["assetTypes"] = self.loc_assetTypes
        self.local_asset_model["typeModel"]["aspectTypes"] = self.loc_aspectTypes
        self.local_asset_model["instanceModel"]["assets"] = self.loc_instanceModel
        # self.local_asset_model["mappingModel"]["mappings"] = self.loc_mappingModel
        self.local_asset_model["mappingModel"]["mappings"] = missing_datapoint_mappings
        self.local_asset_model_payload["data"] = self.local_asset_model

        if os.path.exists(self.model_create_json_payload):
            os.remove(self.model_create_json_payload)

        with open(self.model_create_json_payload, 'w') as f:
            json.dump(self.local_asset_model_payload, f, indent=4)

        print("Complete Asset Modeler Payload: ", self.local_asset_model_payload)

    def get_model_payload_to_send(self):
        asset_model_file = open(self.model_create_json_payload, 'r')
        asset_model_contents = asset_model_file.read()
        model = json.loads(asset_model_contents)
        return model

    def sha256sum(self):
        h = hashlib.sha256()
        b = bytearray(128 * 1024)
        mv = memoryview(b)
        with open(self.local_model_file_name, 'rb', buffering=0) as f:
            while n := f.readinto(mv):
                h.update(mv[:n])
        return h.hexdigest()

    def write_cloud_model_response(self, model_response):
        model_file_name = self.cloud_model_file_name
        if os.path.exists(model_file_name):
            os.remove(model_file_name)

        with open(model_file_name, 'w') as f:
            json.dump(model_response, f, indent=4)


# tenant = 'asmten1'
# service = AssetModelHelper(tenant)
#
# service.load_cloud_model()
# print("=========\n")
# service.load_local_model()
# print("=========\n")
# service.populate_reference_ids_in_model()
