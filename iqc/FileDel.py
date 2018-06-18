import sys
import time
#from time import gmtime, strftime
import datetime
from datetime import datetime, timedelta, timezone
import boto3
import botocore
import os

DicomFolder = '/DICOM/'
region = 'us-west-2'
now = time.time()
cutoff = now - (3 * 86400)
#print (strftime("%Y-%m-%d %H:%M:%S", gmtime()))
print (time.ctime())

def bucket(bucketEnv):
	s3 = boto3.resource("s3")
	bucket = s3.Bucket(bucketEnv)
	exists = True
	try:
		s3.meta.client.head_bucket(Bucket=bucketEnv)
	except botocore.exceptions.ClientError as e:
		# If a client error is thrown, then check that it was a 404 error.
		# If it was a 404 error, then the bucket does not exist.
		error_code = int(e.response['Error']['Code'])
		if error_code == 404:
			exists = "false"
		return exists

def iqcenv():
	try:
#		os.environ[iqcenv]
		env = os.environ["iqcenv"]
		return env
	except KeyError:
		print ("environment: dev, stage, or prod, not set")
		sys.exit(1)

def delCandidate(bucket):
	s3 = boto3.resource("s3")
	client = boto3.client('s3', 'us-west-2')
	bucket = s3.Bucket(bucket)
	buckets = s3.buckets.all()
	epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)

	arr = []
	for root, dirs, files in os.walk(DicomFolder):
		for name in files:
			filepath = os.path.join(root, name)
			stat = os.stat(filepath)
			tstat = stat.st_ctime
			modified = epoch + timedelta(seconds=tstat)
			if tstat < cutoff:
				print (filepath, modified)

def main():
#	env = str(os.environ[iqcenv])
#	print (os.environ[iqcenv])
	env = iqcenv()
	if env == None:
		print ("environment: dev, stage, or prod, not set")
		sys.exit(1)
	bucketEnv = ('iqc%s' % env)
	exists = bucket(bucketEnv)
	if exists == "false":
		print ("bucket does not exist")
		sys.exit(1)
	delCandidate(bucketEnv)

if __name__ == "__main__":
	main()
