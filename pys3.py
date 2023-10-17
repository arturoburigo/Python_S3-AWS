import boto3
import os

from botocore.exceptions import ClientError

ACESS_KEY = 'ACCESS'
SECRET_KEY = 'SECRET'
PRI_BUCKET_NAME = 'arturo-bucket'
TRANSIENT_BUCKET_NAME = 'arturo-bucket02'
F1 = 'arturo1.txt'
F2 = 'arturo2.txt'
F3 = 'arturo3.txt'
Dir = ''
Dwn_dir = ''


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
        response = s3.meta.client.list_obkects(Bucket=bucket)
        objects = []
        for content in response['Contents']:
            objects.append((content['Key']))
        print(bucket, 'contains', len(objects), 'files')
        return objects
    except ClientError as ce:
        print('error', ce)


def copy_file(source_bucket, destination_bucket, source_key, destination_key,s3):
    try:
        source = {
            'Bucket': source_bucket,
            'Key': source_key
        }
        s3.Bucket(destination_bucket).copy(source, destination_key)
    except ClientError as ce:
        print('error', ce)

def main():
    access = os.getenv(ACESS_KEY)
    secret = os.getenv(SECRET_KEY)
    s3 = boto3.resource('s3', aws_access_key_id=access, aws_secret_access_key=secret)
    print(s3.ServiceResource)

    # createBucket(PRI_BUCKET_NAME, s3)

    upload_file(PRI_BUCKET_NAME, Dir, F1, s3)
    upload_file(PRI_BUCKET_NAME, Dir, F2, s3)
    upload_file(PRI_BUCKET_NAME, Dir, F3, s3)

    download_file(PRI_BUCKET_NAME, Dwn_dir, F3, F3, s3)
    delete_files(PRI_BUCKET_NAME, [F1, F2, F3], s3)


def createBucket(name, s3):
    try:
        s3.create_bucket(Bucket=name)

    except ClientError as ce:
        print('error', ce)


if __name__ == '__main__':
    main()
