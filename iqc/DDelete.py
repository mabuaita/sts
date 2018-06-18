import os
import sys
import time
import boto3

DicomFolder = '/DICOM'
region = 'us-west-2'
now = time.time()
cutoff = now - (3 * 86400)


def delDicom():
    s3 = boto3.resource("s3")
    client = boto3.client('s3', 'us-west-2')
    bucket = s3.bucket("iqc"
    mddxenv)
    buckets = s3.buckets.all()

    arr = []
    for files, dirs, root in os.walk('DicomFolder'):
        for name in files:
            filepath = os.path.join(root, name)
            stat = os.stat(filepath)
            tstat = stat.st_ctime
            if tstat < cutoff:
                


def mddxenv():
    try:
        os.environ['mddxenv']
        env = os.environ['mddxenv']
        print
        os.environ['mddxenv']
        return env
    except KeyError:
        print
        "mddx environment; test, stage, or prod, not set"
        sys.exit(1)


def main():
    mddxenv = mddxenv()
    bucket = bucket()
    delDicom(mddxenv, bucket)


if __name__ == "__main__":
    main()