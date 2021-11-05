import boto3
from botocore.exceptions import ClientError
import os
import requests

def get_buckets(s3_client):
    # Retrieve the list of existing buckets
    print("Retrieving the list of existing buckets")
    response = s3_client.list_buckets()
    # Output the bucket names
    print('Existing buckets:')
    buckets = []
    for bucket in response['Buckets']:
        buckets.append(bucket["Name"])
        print(f'  {bucket["Name"]}')
    return buckets


def create_bucket(bucket_name, s3_client):
    # Create an S3 bucket
    print("Creating bucket {0}".format(bucket_name))
    res = s3_client.create_bucket(Bucket=bucket_name)
    return res


def upload_file(file_name, bucket, s3_client, object_name=None):
    """Upload a file to an S3 bucket
    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    """
    print("Uploading file {0} to bucket {1}".format(file_name, bucket))
    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        raise BaseException(
            "Trying to upload file {0} resulted in an error: {1}".format(file_name, e))
    print("Successfully uploaded file {0} as S3 object {1}!".format(file_name,                                                                 object_name))
    return response


def set_bucket_acl(bucket, s3_client, acl):
    print("Setting the access control list (ACL) permissions for bucket {0} to {1}".format(bucket, acl))
    response = s3_client.put_bucket_acl(ACL=acl, Bucket=bucket)
    return response


def set_object_acl(object_name, bucket, s3_client, acl):
    print("Setting the access control list (ACL) permissions for object {0} in"
          " bucket {1} to {2}".format(object_name, bucket, acl))
    response = s3_client.put_object_acl(ACL=acl, Bucket=bucket, Key=object_name)
    return response


def validate_public_access(summary, name):
    acl_public_read = False
    for grant in summary['Grants']:
        grantee = grant['Grantee']
        permission = grant['Permission']
        if grantee['Type'] == 'Group' and 'AllUsers' in grantee['URI'] and permission == 'READ':
            acl_public_read = True
            break
    if acl_public_read:
        print("Successfully changed the ACL of {0} to public-read!".format(name))
    else:
        raise BaseException(
            "Failed to changed the ACL of {0}".format(name))


def verify_file_download(object_name, file_name, content):
    file_success = False
    if os.path.exists(file_name):
        with open(file_name, 'r') as f:
            contents = f.readlines()[0]
            if content == contents:
                file_success = True
    if file_success:
        print("Successfully downloaded object {0} to file {1}!".format(object_name, file_name))
    else:
        raise BaseException(
            "Failed to download S3 object {0}".format(object_name))


if __name__ == '__main__':
    # set AWS credentials
    # TODO: change to your credentials
    ACCESS_KEY = "EXAMPLE_ASIA6FNSSPX4KMXLQQGQ"
    SECRET_KEY = "EXAMPLE_H7GWbfg56U25W3jXJjaRz4P"
    SESSION_TOKEN = "EXAMPLE_FwoGZXIvYXdzEPn//////////wEaDHk4SbnENfCajc/RxiLFAc2ok4QQpn5uyRYJhVe/UDdxZwNdCrJ90X3Vkb+bucAUSXBi00/TQ9bcVAnnonqDlE7NlxDlPm9Atrha7JRAQpxr3AH0q/WxxUJg+r/WZPstmn1lacb/A5cmA9hfLu9aI/GdCm51l5P3zYijDXSh4fmEuNQGidfa6NLuoe5AOsVm7fLhUcEIQPdVteeE1v3gi3G7ulL2PApzM5Pl08QKv29LnQlPxM6p6Buk/icq2HVH29eNeWj8jwzCtw3WbgNJnSKaEELZ9A="
    # Create S3 client
    client = boto3.client(
        's3',
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        aws_session_token=SESSION_TOKEN,
        region_name="us-east-1"
    )
    # Create S3 bucket
    bucket_name = "shecodes-exercise-4"
    create_bucket(bucket_name, client)
    if bucket_name not in get_buckets(client):
        raise BaseException("Failed to create the new bucket {}!".format(bucket_name))
    else:
        print("Successfully created bucket {0}!".format(bucket_name))
    # Create a file
    file_name = "shecodes.txt"
    file_content = "Exercise 4 - S3!"
    with open(file_name, "w") as f:
        f.write(file_content)
    # Upload the file to S3
    object_name = "shecodes_object"
    upload_file(file_name, bucket_name, client, object_name)
    url = f'https://{bucket_name}.s3.amazonaws.com/{object_name}'

    # Download the file
    downloaded_file_name = "shecodes_downloaded.txt"
    print("Downloading {0} from bucket {1}...".format(object_name, bucket_name))
    client.download_file(bucket_name, object_name, downloaded_file_name)
    verify_file_download(object_name, downloaded_file_name, file_content)
    # Make the bucket public
    set_bucket_acl(bucket_name, client, "public-read")
    bucket_summary = client.get_bucket_acl(Bucket=bucket_name)
    validate_public_access(bucket_summary, bucket_name)
    # Make the object public
    set_object_acl(object_name, bucket_name, client, "public-read")
    object_summary = client.get_object_acl(Bucket=bucket_name, Key=object_name)
    validate_public_access(object_summary, object_name)
    # Download the public file from the url
    url = f'https://{bucket_name}.s3.amazonaws.com/{object_name}'
    r = requests.get(url, allow_redirects=True)
    # Save the content to file
    downloaded_url_name = "shecodes_downloaded_public.txt"
    open(downloaded_url_name, 'wb').write(r.content)
    verify_file_download(object_name, downloaded_url_name, file_content)
    # Delete the object
    client.delete_object(Bucket=bucket_name, Key=object_name)
    # Delete the bucket
    client.delete_bucket(Bucket=bucket_name)
    if bucket_name not in get_buckets(client):
        print("Bucket {0} deleted with success!".format(bucket_name))
    else:
        raise BaseException("Failed to delete the bucket {}".format(bucket_name))
