import boto3
from util import wait_until


# Create a new instance
def create_instance(ec2_client):
    print('Requesting for EC2 instance creation')
    response = ec2_client.run_instances(
        ImageId='ami-09e67e426f25ce0d7',
        InstanceType='t2.micro',
        MinCount=1,
        MaxCount=1,
        UserData='#!/bin/bash \n apt update \n apt install -y apache2',
        # TODO: change to your security group ID
        SecurityGroupIds=["sg-050409f434968b4b7"]
    )
    # return the instance ID
    id = response['Instances'][0]['InstanceId']
    print("Successfully created EC2 instance with ID: {0}".format(id))
    return id


def terminate_instance(ec2_client, instance_id):
    print('Requesting termination of instance {0}'.format(instance_id))
    response = ec2_client.terminate_instances(
        InstanceIds=[
            instance_id,
        ]
    )
    state = response['TerminatingInstances'][0]['CurrentState']['Name']
    if state == 'shutting-down':
        print(
            'Request for termination has been initiated successfully. Instance {0} is being shut down'.format(
                instance_id))
        return True
    else:
        print("Couldn't shutdown instance {0}, current state={1}".format(
            instance_id, state))
        return False


# Check  if the instance is in 'running' state
def instance_is_running(ec2_client, instance_id):
    print("waiting for instance {0} to be in a 'running' state...".format(
        instance_id))
    instances = ec2_client.describe_instances(
        Filters=[{'Name': 'instance-state-name',
                  'Values': ['running']}])
    for reservation in instances['Reservations']:
        instance = reservation['Instances'][0]
        if instance['InstanceId'] == instance_id:
            print("Instance {0} is running!".format(instance_id))
            print("Instance description: \n {0}".format(instance))
            return True
    return False


if __name__ == '__main__':
    # set AWS credentials
    # TODO: change to your credentials
    ACCESS_KEY = "EXAMPLE_ASIA6FNSSPX4KMXLQQGQ"
    SECRET_KEY = "EXAMPLE_H7GWbfg56U25W3jXJjaRz4P"
    SESSION_TOKEN = "EXAMPLE_FwoGZXIvYXdzEPn//////////wEaDHk4SbnENfCajc/RxiLFAc2ok4QQpn5uyRYJhVe/UDdxZwNdCrJ90X3Vkb+bucAUSXBi00/TQ9bcVAnnonqDlE7NlxDlPm9Atrha7JRAQpxr3AH0q/WxxUJg+r/WZPstmn1lacb/A5cmA9hfLu9aI/GdCm51l5P3zYijDXSh4fmEuNQGidfa6NLuoe5AOsVm7fLhUcEIQPdVteeE1v3gi3G7ulL2PApzM5Pl08QKv29LnQlPxM6p6Buk/icq2HVH29eNeWj8jwzCtw3WbgNJnSKaEELZ9A="

    # Create EC2 client
    client = boto3.client(
        'ec2',
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        aws_session_token=SESSION_TOKEN,
        region_name="us-east-1"
    )

    instance_id = create_instance(client)
    wait_until(somepredicate=instance_is_running, timeout=120, period=12,
               ec2_client=client, instance_id=instance_id)
    terminate_instance(client, instance_id)
