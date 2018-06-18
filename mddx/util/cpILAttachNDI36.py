# coding=utf8
# Test pyodbc connection. Result is 42.
# Note parameters in connection string, <PARAMETER>.

import pyodbc
import sys
import os
import os.path
import re
import string
import boto3
import zipfile
from botocore.exceptions import ClientError
import botocore.session
import binascii
import shutil
import psutil

#for pid in psutil.pids():
#	p = psutil.Process(pid)
#	if p.name() == "python" and len(p.cmdline()) > 1 and "cpILAttachNDI.py" in p.cmdline()[1]:
#		print ("CXXXX")
#		sys.exit(0)
#
string_types = str
conn = pyodbc.connect('DRIVER=FreeTDS;SERVER=DBHost;PORT=1433;DATABASE=MDDXIT;UID=sa;PWD=aiHwZ2!Qp2Xf;TDS_Version=8.0;', autocommit=True)
resource = boto3.resource('s3')
bucket = resource.Bucket('iqctest')
client = boto3.client('s3', 'us-west-2')
exist = True
AttachPath = '/attachments/'
#    print(bucket.name)


def CRC32_from_file(mergeFile):
    buf = open(mergeFile,'rb').read()
    buf = (binascii.crc32(buf) & 0xFFFFFFFF)
    return "%08X" % buf

def file_size(mergFile):
    size = os.stat(mergeFile)
    return size.st_size

with conn:

	cursor = conn.cursor()
	while True:
		cursor.execute("select CaseId from dbo.[PendingTransferQue] where IsAttachmentTransferred is NULL or IsAttachmentTransferred = 0")
		row = cursor.fetchone()
		print (row)
		if row is not None:
			CaseNo = row[0]
			print (CaseNo)
			cursor.execute("select * from ( select c.CopyToCl, c.CustomFieldID, caseFileUpload.[FileName], caseFileUpload.FileServerName ,CASE WHEN (tblCase.Method_of_Entry=8 AND c.IsNDIEnabled=1) THEN 1  ELSE 0 END as 'NDIEnabled' From dbo.[CaseFileUpload] caseFileUpload join dbo.[CustomField] c on caseFileUpload.CustomFieldID = c.CustomFieldID JOIN [Case] as tblCase ON caseFileUpload.CaseID=tblCase.Case_Id where caseFileUpload.CaseID = '%d'  and caseFileUpload.CaseType = 'UploadItf' and caseFileUpload.FileServerName is not NULL and c.CopyToCl = 1) as caseData where caseData.NDIEnabled=1" % (CaseNo))
			rows = cursor.fetchall()
			print (rows), (row), (CaseNo), ("NDI")
#		cursor.execute("select caseData.NDIEnabled from ( select c.CopyToCl, c.CustomFieldID, caseFileUpload.[FileName], caseFileUpload.FileServerName ,CASE WHEN (tblCase.Method_of_Entry=8 AND c.IsNDIEnabled=1) THEN 1  ELSE 0 END as 'NDIEnabled' From dbo.[CaseFileUpload] caseFileUpload join dbo.[CustomField] c on caseFileUpload.CustomFieldID = c.CustomFieldID JOIN [Case] as tblCase ON caseFileUpload.CaseID=tblCase.Case_Id where caseFileUpload.CaseID = '%d'  and caseFileUpload.CaseType = 'UploadItf' and caseFileUpload.FileServerName is not NULL and c.CopyToCl = 1) as caseData" % (CaseNo))
#		NDIType = row[0]
			NDIType = 2
			while True:
				cursor.execute("select DownloaderBucket from  CaseFileNamesInBucket where caseid = '%d' " % (CaseNo))
				attachBucket = cursor.fetchone()[0]
				if attachBucket is None:
					break
				if len(attachBucket.split()) == 0:
					break
				if rows == []:
					attachZip = attachBucket
				else:
					attachZip = str.replace(str(attachBucket), '.zip', '_attachment.zip')
				print ("key"), (attachBucket), (attachZip)
				srcZipFile = str(CaseNo) + '.zip'
				cursor.execute("select a.AccountName from CaseDetailsByfnFarthestFields cf join [Account] a on cf.AccountID = a.AccountID where cf.Case_Id = '%d' " % (CaseNo))
				AccountName = cursor.fetchone()[0]
				cursor.execute("select cf.Trial_id from CaseDetailsByfnFarthestFields cf join [Account] a on cf.AccountID = a.AccountID where cf.Case_Id = '%d' " % (CaseNo))
				CaseID = cursor.fetchone()[0]
				srcZipPath = os.path.join('/tmp/', str(attachBucket))
				mergeFile = os.path.join('/tmp/', str(attachZip))
				dstZip = re.sub(r'.zip', '', mergeFile)
				print ("wow"), (dstZip)
				dstAttach = re.sub(r'.zip', '', attachBucket)
				print (AccountName), (CaseID)
				srcCase = 'autopxfolder/' + str(AccountName) + '/' + str(CaseID) + '/' + str(attachBucket)
				print (rows), (srcZipPath), (srcZipFile), (srcCase)
				print ('iqctest'), (srcCase), (srcZipPath)
				try:
					client.download_file('iqctest', srcCase, mergeFile)
				except Exception as e:
					cursor.execute("UPDATE dbo.[PendingTransferQue] SET IsAttachmentTransferred = 1 where CaseId = '%d'" %(CaseNo))
					break

				if rows == []:
					print ("no NDI attach")
					break

				try:
					os.stat(dstZip)
				except:
					os.mkdir(dstZip)
				zip_attach = zipfile.ZipFile(mergeFile, 'r')
				zip_attach.extractall(dstZip)
				zip_attach.close()
				print (mergeFile), (dstZip)

				for row in rows:
					srcZipFile = zipfile.ZipFile(mergeFile,"a",allowZip64=True)
					src = '/attachments/' + row.FileServerName
					if row.FileName is not None:
						srcOrig = row.FileName
						srcOrig = re.sub(r'~.*?~', '', srcOrig)
					else:
						srcOrig = row.FileServerName
					if not os.path.isfile(src):
						print("ERROR: %s Does not exist in UploadedFiles, even though it's in the database." % (src))
						continue
					dst = 'uiqcfolder' + '/attachments/' + srcOrig
					print (mergeFile)
					if os.path.isfile(mergeFile):
						srcZipFile = zipfile.ZipFile(mergeFile,"a",allowZip64=True)
						print ("NDI merge exists")
						print (src), (dst), (srcZipPath), (srcZipFile), (CaseNo), (row.FileServerName)
						directory = os.path.dirname(dstZip + '/' + dstAttach + '/' + dstAttach + '/')
						try:
							os.stat(directory)
						except:
							os.mkdir(directory)  
							print ("NDI Directory")
						if src.endswith('.zip'):	
							shutil.copy(src,directory + '/' + srcOrig)
#								zip_attach = zipfile.ZipFile(src, 'r')
							print ('1attach'), (src), (directory), (src)
#								zip_attach.extractall(directory)
#								zip_attach.close()
						else:
							shutil.copy(src,directory + '/' + srcOrig)
							print ("NDIAttach"), (src), (directory), (srcOrig)
					files = os.listdir(directory)
					files_attach = []
#					for root, dirnames, files in os.walk(directory):
#						for f in files:
#							fname = os.path.join(root, f)
#							srcZipFile.write(dstZip + '/' + attachBucket + '/' + attachBucket + '/' + os.path.basename(fname))
					srcZipFile.close()
					try:
						client.upload_file(src, 'iqctest', dst)
					except ClientError as ce:
						print(ce.response)
						print(ce.response['Error']['Code'])
						if ce.response['Error']['Code'] == "NoSuchKey":
							exist = False
							print("ERROR: %s Does not exist in S3, even though it's in the database." % (src))
							continue	
						else:
							raise ce
				row = []
				rows = []
				break

			cursor.execute("select * from ( select c.CopyToCl, c.CustomFieldID, caseFileUpload.[FileName], caseFileUpload.FileServerName ,CASE WHEN (tblCase.Method_of_Entry=8 AND c.IsNDIEnabled=1) THEN 1  ELSE 0 END as 'NDIEnabled' From dbo.[CaseFileUpload] caseFileUpload join dbo.[CustomField] c on caseFileUpload.CustomFieldID = c.CustomFieldID JOIN [Case] as tblCase ON caseFileUpload.CaseID=tblCase.Case_Id where caseFileUpload.CaseID = '%d'  and caseFileUpload.CaseType = 'UploadItf' and caseFileUpload.FileServerName is not NULL and c.CopyToCl = 1) as caseData where caseData.NDIEnabled=0" % (CaseNo))
			rows = cursor.fetchall()
			if rows == []:
				print ("no legacy attach")
				print (mergeFile)
				dstAttachZip = 'autopxfolder/' + str(AccountName) + '/' + str(CaseID) + '/' + attachZip
				crc32 = CRC32_from_file(mergeFile)
				size = file_size(mergeFile)
				cursor.execute("select Case_Pre_Post_Id from Case_Pre_Post where Case_Id = '%d' and case_type ='post-anon'" %(CaseNo))
				PrePostId = cursor.fetchone()[0]
				cursor.execute("insert into dbo.[CaseFileMD5CheckSum](FileCheckSum, CasePrePostID, FileSize) values ('%s', '%d', '%d')" %(crc32,PrePostId,size))
				print (mergeFile,"crc", crc32, PrePostId, size)
				dstZip = str.replace(str(attachBucket), '.zip', '_attachment')
				if os.path.isdir(dstZip):
					print (dstZip, mergeFile)
					os.rename(mergeFile, mergeFile + "post")
					shutil.make_archive(dstZip, 'zip', dstZip)
					crc32 = CRC32_from_file(mergeFile)
					size = file_size(mergeFile)
					cursor.execute("select Case_Pre_Post_Id from Case_Pre_Post where Case_Id = '%d' and case_type ='post-anon'" %(CaseNo))
					PrePostId = cursor.fetchone()[0]
					cursor.execute("insert into dbo.[CaseFileMD5CheckSum](FileCheckSum, CasePrePostID, FileSize) values ('%s', '%d', '%d')" %(crc32,PrePostId,size))
					print ("MDInoLeg",mergeFile,"crc", crc32, PrePostId, size)
					client.upload_file(mergeFile, 'iqctest', dstAttachZip)
				else:
					crc32 = CRC32_from_file(mergeFile)
					size = file_size(mergeFile)
					cursor.execute("select Case_Pre_Post_Id from Case_Pre_Post where Case_Id = '%d' and case_type ='post-anon'" %(CaseNo))
					PrePostId = cursor.fetchone()[0]
					cursor.execute("insert into dbo.[CaseFileMD5CheckSum](FileCheckSum, CasePrePostID, FileSize) values ('%s', '%d', '%d')" %(crc32,PrePostId,size))                           
					print ("noNSInoLeg",mergeFile,"crc", crc32, PrePostId, size)
					client.upload_file(mergeFile, 'iqctest', dstAttachZip)
				cursor.execute("UPDATE dbo.[PendingTransferQue] SET IsAttachmentTransferred = 1 where CaseId = '%d'" %(CaseNo))
				break
			else:
				print ("Legacy row")
				print (mergeFile)
				cursor.execute("select DownloaderBucket from  CaseFileNamesInBucket where caseid = '%d' " % (CaseNo))
				attachBucket = cursor.fetchone()[0]
				if attachBucket is None:
					break
			#if (len(attachBucket.split(' ')) == 0:
				if len(attachBucket.split()) == 0:
					print ("bucket break")
					break
				attachZip = str.replace(str(attachBucket), '.zip', '_attachment.zip')
				print (rows), (row), ('legacy'), (mergeFile)
				
				print (attachBucket), (attachZip)
				srcZipFile = str(CaseNo) + '.zip'
				cursor.execute("select a.AccountName from CaseDetailsByfnFarthestFields cf join [Account] a on cf.AccountID = a.AccountID where cf.Case_Id = '%d' " % (CaseNo))
				AccountName = cursor.fetchone()[0]
				cursor.execute("select cf.Trial_id from CaseDetailsByfnFarthestFields cf join [Account] a on cf.AccountID = a.AccountID where cf.Case_Id = '%d' " % (CaseNo))
				CaseID = cursor.fetchone()[0]
				srcZipPath = os.path.join('/tmp/', str(attachBucket))
				mergeFile_attch = os.path.join('/tmp/', str(attachZip))
				dstZip = re.sub(r'.zip', '', mergeFile_attch)
				dstZip1 = re.sub(r'.zip', '', mergeFile_attch)
				directory = os.path.dirname(dstZip1 + '/' + 'attachments' + '/')
				print (mergeFile, mergeFile_attch,dstZip, attachZip, dstZip1, "ddddstzip")
				try:
					os.stat(dstZip)
				except:
					os.mkdir(dstZip)
				try:
					os.stat(directory)
				except:
					os.mkdir(directory)
				zip_attach = zipfile.ZipFile(mergeFile, 'r')
				zip_attach.extractall(dstZip)
				zip_attach.close()
				mergeFile = mergeFile_attch
				srcCase = 'autopxfolder/' + str(AccountName) + '/' + str(CaseID) + '/' + str(attachBucket)
				print (rows), (srcZipPath), (srcZipFile), (srcCase)
				print ('iqctest'), (srcCase), (srcZipPath)

				for row in rows:
					srcZipFile = zipfile.ZipFile(mergeFile,"a",allowZip64=True)
					src = '/attachments/' + row.FileServerName
					if row.FileName is not None:
						srcOrig = row.FileName
						srcOrig = re.sub(r'~.*?~', '', srcOrig)
					else:
						srcOrig = row.FileServerName
					if not os.path.isfile(src):
						print("ERROR: %s Does not exist in UploadedFiles, even though it's in the database." % (src))
						continue
					dst = 'uiqcfolder' + '/attachments/' + srcOrig
					print ('legacy attach')
					directory = os.path.dirname(dstZip + '/')
					print (src,row.FileServerName,'legacy2')
					shutil.copy(src,directory + '/' + 'attachments' + '/' + srcOrig)
					try:
						client.upload_file(src, 'iqctest', dst)
					except ClientError as ce:
						print(ce.response)
						print(ce.response['Error']['Code'])
						if ce.response['Error']['Code'] == "NoSuchKey":
							exist = False
							print("ERROR: %s Does not exist in S3, even though it's in the database." % (src))
							continue	
						else:
							raise ce
				shutil.make_archive(dstZip, 'zip', dstZip)
				print ("last", mergeFile,mergeFile_attch,dstZip1)
				if os.path.isfile(mergeFile):
					dstAttachZip = 'autopxfolder/' + str(AccountName) + '/' + str(CaseID) + '/' + attachZip                                
					crc32 = CRC32_from_file(mergeFile)
					size = file_size(mergeFile)
					cursor.execute("select Case_Pre_Post_Id from Case_Pre_Post where Case_Id = '%d' and case_type ='post-anon'" %(CaseNo))
					PrePostId = cursor.fetchone()[0]
					cursor.execute("insert into dbo.[CaseFileMD5CheckSum](FileCheckSum, CasePrePostID, FileSize) values ('%s', '%d', '%d')" %(crc32,PrePostId,size))
					print (mergeFile,"crc", crc32, PrePostId, size) 
					client.upload_file(mergeFile, 'iqctest', dstAttachZip)
					cursor.execute("UPDATE dbo.[PendingTransferQue] SET IsAttachmentTransferred = 1 where CaseId = '%d'" %(CaseNo))
					break
			cursor.execute("UPDATE dbo.[PendingTransferQue] SET IsAttachmentTransferred = 1 where CaseId = '%d'" %(CaseNo))
		if row is None:
			break
conn.close()

#def zipdir(path, ziph):
#    # ziph is zipfile handle
#    for root, dirs, files in os.walk(path):
#        for file in files:
#            ziph.write(os.path.join(root, file))
#
#if __name__ == '__main__':
#    zipf = zipfile.ZipFile('Python.zip', 'w', zipfile.ZIP_DEFLATED)
#    zipdir('tmp/', zipf)
#    zipf.close()
