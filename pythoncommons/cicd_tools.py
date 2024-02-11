import json
import os
import time
import uuid
from typing import Any, Dict, Tuple, List

import boto3
import yaml
from kubernetes import client, config
from kubernetes import utils
from kubernetes.client import V1Job
from kubernetes.client.rest import ApiException
from pydantic import BaseModel

AWS_SERVER_PUBLIC_KEY = os.environ.get('AWS_SERVER_PUBLIC_KEY', None)
AWS_SERVER_SECRET_KEY = os.environ.get('AWS_SERVER_SECRET_KEY', None)


class DeployConfig(BaseModel):
    order: List[str]


class Deployment(BaseModel):
    config_dir: str


class EventMessage(BaseModel):
    event_type: str


class DeploymentMessage(EventMessage):
    value: Deployment


REGISTERED_EVENT_TYPES = {
    'Deploy': DeploymentMessage,
    'MasterReady': EventMessage
}


class S3Client:
    def __init__(self, region: str, bucket_name: str):
        self.client = boto3.client('s3',
                                   aws_access_key_id=AWS_SERVER_PUBLIC_KEY,
                                   aws_secret_access_key=AWS_SERVER_SECRET_KEY,
                                   region_name=region)
        self.bucket_name = bucket_name

    def list_objects_in_dir(self, folder_path: str) -> dict:
        return self.client.list_objects_v2(Bucket=self.bucket_name, Prefix=folder_path)

    def download_file(self, s3_file_key: str, destination_file_path: str) -> None:
        try:
            self.client.download_file(self.bucket_name, s3_file_key, destination_file_path)
            print(f"File {destination_file_path} has been downloaded from {s3_file_key}")
        except Exception as e:
            print(f"Cannot download file {destination_file_path + s3_file_key} from S3: {str(e)}")

    def download_files_from_dir(self, s3_folder_path: str, local_folder: str) -> None:
        files = self.list_objects_in_dir(s3_folder_path)['Contents']
        print(f'Available files in s3 dir {files}')
        for content in files:
            file_name = content['Key']
            local_file_path = os.path.join(local_folder, os.path.basename(file_name))
            self.download_file(file_name, local_file_path)


class SnsClient:
    def __init__(self, region, topic_arn: str):
        self.client = boto3.client('sns',
                                   aws_access_key_id=AWS_SERVER_PUBLIC_KEY,
                                   aws_secret_access_key=AWS_SERVER_SECRET_KEY,
                                   region_name=region)
        self.topic_arn = topic_arn

    def publish(self, message: EventMessage):
        response = self.client.publish(
            TopicArn=self.topic_arn,
            Message=message.json(),
            MessageGroupId=str(uuid.uuid4()),
            MessageDeduplicationId=str(uuid.uuid4())
        )
        return response


class SqsClient:
    def __init__(self, region: str, queue_url: str, timeout: int):
        self.client = boto3.client('sqs',
                                   aws_access_key_id=AWS_SERVER_PUBLIC_KEY,
                                   aws_secret_access_key=AWS_SERVER_SECRET_KEY,
                                   region_name=region)
        self.queue_url = queue_url
        self.timeout = timeout

    def poll_message(self) -> dict:
        return self.client.receive_message(
            QueueUrl=self.queue_url,
            AttributeNames=[
                'All'
            ],
            MaxNumberOfMessages=1,
            MessageAttributeNames=[
                'All'
            ],
            WaitTimeSeconds=self.timeout
        )

    @staticmethod
    def get_cicd_event(sqs_response: dict) -> [EventMessage, str]:
        messages_body = sqs_response['Messages'][0]['Body']
        raw_message_body = json.loads(messages_body)['Message']
        event_data = json.loads(raw_message_body)
        event_type = EventMessage(**event_data).event_type
        return REGISTERED_EVENT_TYPES[event_type](**event_data), sqs_response['Messages'][0]['ReceiptHandle']

    def delete_message(self, receipt_handle: str) -> None:
        self.client.delete_message(
            QueueUrl=self.queue_url,
            ReceiptHandle=receipt_handle
        )

    @staticmethod
    def is_message_not_parsable(sqs_response: dict) -> bool:
        if 'Messages' not in sqs_response:
            return True

        if len(sqs_response['Messages']) <= 0:
            return True

        return False

    def wait_for_event(self, event_name: str, should_be_deleted=True) -> EventMessage:
        while True:
            response = self.poll_message()
            print(f"Got message: {response}")

            if self.is_message_not_parsable(response):
                continue

            event, receipt_handle = self.get_cicd_event(response)

            if event.event_type == event_name:
                print(f"Received: {event_name}")
                if should_be_deleted:
                    self.delete_message(receipt_handle)
                return event


class ParameterStoreClient:
    def __init__(self, region: str):
        self.client = boto3.client('ssm',
                                   aws_access_key_id=AWS_SERVER_PUBLIC_KEY,
                                   aws_secret_access_key=AWS_SERVER_SECRET_KEY,
                                   region_name=region)

    def get_parameter(self, name: str) -> str:
        return self.client.get_parameter(
            Name=name,
            WithDecryption=False
        )['Parameter']['Value']


class YamlLoader:

    @staticmethod
    def load_yaml(file: str) -> Dict:
        with open(file, 'r') as file:
            manifest_content = yaml.safe_load(file)
        return manifest_content

    @staticmethod
    def parse_yaml(file: str, type: Any) -> Any:
        yaml_data = YamlLoader.load_yaml(file)
        return type(**yaml_data)


class KubernetesManager:
    def __init__(self):
        config.load_kube_config()
        # config.load_incluster_config()
        self.core_v1_api = client.CoreV1Api()
        self.apps_v1_api = client.AppsV1Api()
        self.api_client = client.ApiClient()
        self.networking_v1_api = client.NetworkingV1Api()
        self.batch_v1_api = client.BatchV1Api()
        self.yaml_loader = YamlLoader()

    def read_deployment(self, name: str, namespace: str):
        result = self.apps_v1_api.read_namespaced_deployment(name, namespace)
        return result, False

    def read_stateful_set(self, name: str, namespace: str):
        result = self.apps_v1_api.read_namespaced_stateful_set(name, namespace)
        return result, False

    def read_pv(self, name: str, namespace: str):
        obj = self.core_v1_api.read_persistent_volume(name)
        return obj, False

    def read_pvc(self, name: str, namespace: str):
        obj = self.core_v1_api.read_namespaced_persistent_volume_claim(name, namespace)
        return obj, False

    def read_config_map(self, name: str, namespace: str):
        obj = self.core_v1_api.read_namespaced_config_map(name, namespace)
        return obj, True

    def read_secret(self, name: str, namespace: str):
        obj = self.core_v1_api.read_namespaced_secret(name, namespace)
        return obj, True

    def read_ingress(self, name: str, namespace: str):
        obj = self.networking_v1_api.read_namespaced_ingress(name, namespace)
        return obj, True

    def read_service(self, name: str, namespace: str):
        obj = self.core_v1_api.read_namespaced_service(name, namespace)
        return obj, True

    def read_job(self, name: str, namespace: str):
        obj = self.batch_v1_api.read_namespaced_job_status(name=name, namespace=namespace)
        return obj, False

    def delete_deployment(self, name: str, namespace: str):
        self.apps_v1_api.delete_namespaced_deployment(name, namespace, body=client.V1DeleteOptions())

    def delete_stateful_set(self, name: str, namespace: str):
        self.apps_v1_api.delete_namespaced_stateful_set(name, namespace, body=client.V1DeleteOptions())

    def delete_pv(self, name: str, namespace: str):
        self.core_v1_api.delete_persistent_volume(name, body=client.V1DeleteOptions())

    def delete_pvc(self, name: str, namespace: str):
        self.core_v1_api.delete_namespaced_persistent_volume_claim(name, namespace, body=client.V1DeleteOptions())

    def delete_config_map(self, name: str, namespace: str):
        self.core_v1_api.delete_namespaced_config_map(name, namespace, body=client.V1DeleteOptions())

    def delete_secret(self, name: str, namespace: str):
        self.core_v1_api.delete_namespaced_secret(name, namespace, body=client.V1DeleteOptions())

    def delete_ingress(self, name: str, namespace: str):
        self.networking_v1_api.delete_namespaced_ingress(name, namespace, body=client.V1DeleteOptions())

    def delete_service(self, name: str, namespace: str):
        self.core_v1_api.delete_namespaced_service(name, namespace, body=client.V1DeleteOptions())

    def delete_job(self, name: str, namespace: str):
        self.batch_v1_api.delete_namespaced_job(name, namespace, body=client.V1DeleteOptions())

    def is_deployment_ready(self, obj: Any, manifest_data: Dict) -> bool:
        print(f'Current number of replicas: {obj.status.ready_replicas}')
        return obj.status.ready_replicas == manifest_data['spec']['replicas']

    def is_stateful_set_ready(self, obj: Any, manifest_data: Dict) -> bool:
        print(f'Current number of replicas: {obj.status.available_replicas}')
        return obj.status.available_replicas == manifest_data['spec']['replicas']

    def is_job_finished(self, obj: V1Job, manifest_data: Dict) -> bool:
        succeeded_jobs_count: int = obj.status.succeeded
        print(f'Current number of succeeded: {succeeded_jobs_count}')
        return succeeded_jobs_count == 1

    ready_conditions = {
        'Deployment': is_deployment_ready,
        'StatefulSet': is_stateful_set_ready,
        'Job': is_job_finished
    }

    read_functions = {
        'Deployment': read_deployment,
        'StatefulSet': read_stateful_set,
        'PersistentVolume': read_pv,
        'PersistentVolumeClaim': read_pvc,
        'ConfigMap': read_config_map,
        'Secret': read_service,
        'Ingress': read_ingress,
        'Service': read_service,
        'Job': read_job
    }

    delete_functions = {
        'Deployment': delete_deployment,
        'StatefulSet': delete_stateful_set,
        'PersistentVolume': delete_pv,
        'PersistentVolumeClaim': delete_pvc,
        'ConfigMap': delete_config_map,
        'Secret': delete_service,
        'Ingress': delete_ingress,
        'Service': delete_service,
        'Job': delete_job
    }

    def is_manifest_exist(self, kind: str, name: str, namespace='default') -> bool:
        try:
            self.read_functions[kind](self, name, namespace)
            return True
        except ApiException as e:
            if e.status == 404:
                return False
            else:
                raise

    def rollback_old_manifest(self, kind: str, name: str, namespace: str):
        try:
            self.delete_functions[kind](self, name, namespace)
        except ApiException as e:
            if e.status != 404:
                raise
        self.wait_for_manifest_rollback(kind=kind,
                                        namespace=namespace,
                                        name=name,
                                        timeout_seconds=60)

    def wait_for_manifest_rollback(self,
                                   kind: str,
                                   name: str,
                                   namespace='default',
                                   timeout_seconds=300):
        print(f'Waiting for rollback of {namespace}/{kind}/{name} ...')
        start_time = time.time()
        while time.time() - start_time < timeout_seconds:
            try:
                obj, is_instant_resource = self.read_functions[kind](self, name, namespace)
                if is_instant_resource:
                    print(f'{namespace}/{name} is instant resource')
                    return
            except ApiException as e:
                if e.status == 404:
                    print(f'Resource {namespace}/{name} rolled back')
                    return
            time.sleep(5)

        raise TimeoutError(f"Could not rollback {kind}/{name} in {timeout_seconds} seconds")

    def wait_for_manifest_ready(self,
                                kind: str,
                                name: str,
                                manifest_data: Dict,
                                namespace='default',
                                timeout_seconds=300) -> None:
        print(f'Waiting for {namespace}/{kind}/{name} to be ready ...')
        start_time = time.time()
        while time.time() - start_time < timeout_seconds:
            try:
                obj, is_instant_resource = self.read_functions[kind](self, name, namespace)

                if is_instant_resource:
                    return

                if kind not in self.ready_conditions.keys():
                    return

                if self.ready_conditions[kind](self, obj, manifest_data):
                    return
            except ApiException as e:
                if e.status == 404:
                    time.sleep(5)
                    continue
            time.sleep(5)

        raise TimeoutError(
            f"Could not get ready status for {namespace}/{kind}/{name} in {timeout_seconds} seconds")

    def create_manifest(self, yaml_file: str) -> None:
        kind, namespace, name, manifest_content = self.get_manifest_data(yaml_file)
        utils.create_from_yaml(self.api_client,
                               yaml_file=yaml_file,
                               verbose=True)
        self.wait_for_manifest_ready(kind=kind,
                                     namespace=namespace,
                                     manifest_data=manifest_content,
                                     name=name)

    def get_manifest_data(self, manifest: str) -> Tuple[str, str, str, Dict]:
        manifest_content = self.yaml_loader.load_yaml(file=manifest)
        kind = manifest_content['kind']
        namespace = manifest_content['metadata']['namespace']
        name = manifest_content['metadata']['name']
        return kind, namespace, name, manifest_content

    def perform_rollback_from_config_file(self, rollback_config_filename: str):
        base_config = self.yaml_loader.parse_yaml(rollback_config_filename, DeployConfig)
        for manifest in base_config.order:
            kind, namespace, name, manifest_content = self.get_manifest_data(manifest)
            if self.is_manifest_exist(kind=kind,
                                      namespace=namespace,
                                      name=name):
                self.rollback_old_manifest(kind=kind,
                                           namespace=namespace,
                                           name=name)
