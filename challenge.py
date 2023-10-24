"""
Challenge Problem

- Create a temporary bucket
- Upload a file
- Copy a file from an existing bucket
- Get the files and print them
- Download  a file
- Generete a presigned URL
- Delete the files
- Delete the bucket
"""

import boto3
import os

from botocore.exceptions import ClientError

ACESS_KEY = 'ACCESS'
SECRET_KEY = 'SECRET'
PRI_BUCKET_NAME = 'arturo-burigo-existing-bucket'
TRANSIENT_BUCKET_NAME = 'arturo-bucket-challenge'
F1 = 'arturo1.txt'
F2 = 'Screens.png'
Dir = '/Users/arturoburigo/projects/aws'
Dwn_dir = '/Users/arturoburigo/projects/aws/aws-download'


def upload_file(bucket, directory, file, s3, s3path=None):
    file_path = directory + '/' + file
    remote_path = s3path
    if remote_path is None:
        remote_path = file
    try:
        s3.Bucket(bucket).upload_file(file_path, remote_path)
    except ClientError as ce:
        print('error', ce)


def download_file(bucket, directory, local_name, key_name, s3):
    file_path = directory + '/' + local_name
    try:
        s3.Bucket(bucket).download_file(key_name, file_path)
    except ClientError as ce:
        print('error', ce)


def delete_files(bucket, keys, s3):
    objects = []
    for key in keys:
        objects.append({'Key': key})
    try:
        s3.Bucket(bucket).delete_objects(Delete={'Objects': objects})
    except ClientError as ce:
        print('error', ce)


def list_objects(bucket, s3):
    try:
        response = s3.meta.client.list_objects(Bucket=bucket)
        objects = []
        for content in response['Contents']:
            objects.append((content['Key']))
        print(bucket, 'contains', len(objects), 'files')
        return objects
    except ClientError as ce:
        print('error', ce)


def copy_file(source_bucket, destination_bucket, source_key, destination_key, s3):
    try:
        source = {
            'Bucket': source_bucket,
            'Key': source_key
        }
        s3.Bucket(destination_bucket).copy(source, destination_key)
    except ClientError as ce:
        print('error', ce)


def generate_download_file(bucket, key, expiration_time, s3):
    try:
        response = s3.meta.client.generate_presigned_url('get_object',
                                                         Params={'Bucket': bucket,
                                                                 "Key": key}, ExpiresIn=expiration_time)
        print(response)
    except ClientError as ce:
        print('error', ce)


def delete_bucket(bucket, s3):
    try:
        s3.Bucket(bucket).delete()
    except ClientError as ce:
        print('error', ce)


def createBucket(name, s3):
    try:
        s3.create_bucket(Bucket=name)

    except ClientError as ce:
        print('error', ce)


def main():
    access = os.getenv(ACESS_KEY)
    secret = os.getenv(SECRET_KEY)
    print(access, secret)
    s3 = boto3.resource('s3', aws_access_key_id=access, aws_secret_access_key=secret)

    createBucket(TRANSIENT_BUCKET_NAME, s3)

    upload_file(TRANSIENT_BUCKET_NAME, Dir, F1, s3)
    copy_file(PRI_BUCKET_NAME, TRANSIENT_BUCKET_NAME, F2, F2, s3)
    list_objects(TRANSIENT_BUCKET_NAME, s3)
    download_file(PRI_BUCKET_NAME, Dwn_dir, F2, F2, s3)
    generate_download_file(TRANSIENT_BUCKET_NAME, F2, 40, s3)
    delete_files(TRANSIENT_BUCKET_NAME, keys=[F1, F2], s3=s3)
    delete_bucket(TRANSIENT_BUCKET_NAME, s3)


if __name__ == '__main__':
    main()
