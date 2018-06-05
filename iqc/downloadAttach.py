# Test pyodbc connection. Result is 42.
# Note parameters in connection string, <PARAMETER>.

import pyodbc
import sys
import os
import re
import boto3
from botocore.exceptions import ClientError
import botocore.session

conn = pyodbc.connect('DRIVER=FreeTDS;SERVER=DBHost;PORT=1433;DATABASE=MDDXIT;UID=sa;PWD=aiHwZ2!Qp2Xf;TDS_Version=8.0;')
s3 = boto3.resource('s3')
exist = True
#for bucket in s3.buckets.all():
#    print(bucket.name)

with conn:

   cursor = conn.cursor()
   cursor.execute("select FileName, BucketPath from dbo.[CustomFieldAttachmentToBeTransfered] where IsTransfered = 'false'")
   rows = cursor.fetchall()
   for row in rows:
      print row.FileName, row.BucketPath
      dst = os.path.join(row.BucketPath, row.FileName)
      dst2 = re.sub('iqcstage/.*?', '', dst)
      print dst2
      try:
		src = 'iqcstage/IlDocs/IlDocs/' + row.FileName
      except ClientError as ce:
                print(ce.response)
                print(ce.response['Error']['Code'])
		if ce.response['Error']['Code'] == "404":
                        exist = False
                else:
			raise ce
      print src
      try:
		s3.Object('iqcstage',dst2).copy_from(CopySource=src)
      except ClientError as ce:
                print(ce.response)
                print(ce.response['Error']['Code'])
		if ce.response['Error']['Code'] == "NoSuchKey":
                        exist = False
			print("ERROR: %s Does not exist in S3, even though it's in the database." % (src))
                        continue	
                else:
                	raise ce
      if 'exist' == "True":
		cursor.execute("UPDATE dbo.[CustomFieldAttachmentToBeTransfered] SET IsTransfered = 'true' where FileName = '%s'" %(row.FileName))
      else:
                cursor.execute("UPDATE dbo.[CustomFieldAttachmentToBeTransfered] SET IsTransfered = 'false' where FileName = '%s'" %(row.FileName))
      #s3.Object(src).delete()
