import os
import sys
import time
import boto3

DicomFolder = '/DICOM'
region = 'us-west-2'
now = time.time()
cutoff = now - (3 * 86400)

def bucket(bucketEnv):
        s3 = boto3.resource("s3")
        bucket = s3.bucket(bucketEnv)
        exists = True
    try:
        s3.meta.client.head_bucket(Bucket=bucketEnv)
    except botocore.exceptions.ClientError as e:
        # If a client error is thrown, then check that it was a 404 error.
        # If it was a 404 error, then the bucket does not exist.
        error_code = int(e.response['Error']['Code'])
        if error_code == 404:
            exists = False
        return exists

def iqcenv():
    try:
        os.environ[iqcenv]
        env = os.environ[iqcenv]
        print (os.environ[iqcenv])
        return env
    except KeyError:
        print ("environment: dev, stage, or prod, not set")
        sys.exit(1)

def delCandidate(bucket):
    s3 = boto3.resource("s3")
    client = boto3.client('s3', 'us-west-2')
    bucket = s3.bucket(bucket)
    buckets = s3.buckets.all()

    arr = []
    for files, dirs, root in os.walk('DicomFolder'):
        for name in files:
            filepath = os.path.join(root, name)
            stat = os.stat(filepath)
            tstat = stat.st_ctime
            if tstat < cutoff:
                print (filepath, tstat)

def main():
    env = iqcenv()
    if env = null:
        print ("environment: dev, stage, or prod, not set")
        sys.exit(1)
    bucketEnv = "iqc"env
    exists = bucket(bucketEnv)
    if exist = false:
        print ("bucket does not exist")
        sys.exit(1)
    delCandidate(bucket)

if __name__ == "__main__":
    main()