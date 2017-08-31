#!python3

## Imports
import boto3
import argparse
import datetime
from dateutil import parser as dateParser
import sys
from botocore.exceptions import ClientError, ProfileNotFound

## Constants
DEFAULTDESTPREFIX = 'PIT-Restore-{src}-{y:04d}-{M:02d}-{d:02d}-{h:02d}:{m:02d}'
# Errors
DATETIMEFORMATERROR = 1
DATETIMEFUTUREERROR = 2
BUCKET404ERROR = 3
BUCKET403ERROR = 4
ILLEGALENDPOINTERROR = 5

## Arg parsing
argParser = argparse.ArgumentParser(description='Script for restoring a point in time for a given bucket in S3')
argParser.add_argument('source', type=str, help='Source bucket')
argParser.add_argument('date', type=str, help='date to restore to in yyyy-MM-dd format')
argParser.add_argument('--source-prefix', type=str, default='', help='Source prefix - defaults to empty string (everything in bucket)')
argParser.add_argument('--time', type=str, default='00:00', help='time to restore to in UTC formatted as HH:MM 24 hour')
argParser.add_argument('--dest', type=str, default='source-bucket', help='Destination bucket, will be created if it does not exist - defaults to same bucket')
argParser.add_argument('--dest-prefix', type=str, default=DEFAULTDESTPREFIX, help='destination prefix - defaults to PIT-Restore-dd/MM/yyy-HH:MM')
argParser.add_argument('--access-key', type=str, default='', help='AWS Access Key, if not using default AWS API profile')
argParser.add_argument('--secret-key', type=str, default='', help='AWS Secret Key, if not using default AWS API profile')
argParser.add_argument('--session-token', type=str, default='', help='AWS Session Token, if required')
argParser.add_argument('--profile', type=str, default='default', help='AWS API Profile to use - uses default profile by default')
# https://github.com/boto/boto3/issues/125
argParser.add_argument('--endpoint-region', type=str, default='', help='AWS S3 endpoint region - defaults to us-east-1 (NOTE: Boto3 bug #125 means that us-east-1 won\'t create a bucket for you if it does not exist)')
args = vars(argParser.parse_args())

## Arg processing
source = args['source']
dest = args['dest']
sourcePrefix = args['source_prefix']
destPrefix = args['dest_prefix']
endpointRegion = args['endpoint_region']
accessKey = args['access_key']
secretKey = args['secret_key']
sessionToken = args['session_token']
profile = args['profile']

# Credentials
session = None
if (accessKey == ''):
	try:
		session = boto3.Session(profile_name=profile)
		print('Using AWS API profile: ' + profile, file=sys.stdout)
	except ProfileNotFound as err:
		print('AWS API profile not found: ' + profile, file=sys.stdout)

if not session:
	if (accessKey == ''):
		accessKey = input('Enter your AWS access key: ')
	if (secretKey == ''):
		secretKey = input('Enter your AWS secret key: ')
	if (sessionToken == ''):
		sessionToken = input('Enter your AWS session token (optional - leave blank for no token): ')
	if (sessionToken == ''):
		session = boto3.Session(aws_access_key_id=accessKey, aws_secret_access_key=secretKey)
	else:
		session = boto3.Session(aws_access_key_id=accessKey, aws_secret_access_key=secretKey, aws_session_token=sessionToken)
s3 = session.client('s3')

# Datetime
try:
	dt = dateParser.parse(args['date'] + 'T' + args['time'] + ' UTC')
except ValueError as err:
	print('Date / Time format error. Ensure date is formatted yyyy-MM-dd and time is formatted HH:MM in 24 hour UTC format, if supplied', file=sys.stderr)
	print(err, file=sys.stderr)
	sys.exit(DATETIMEFORMATERROR)
if dt > datetime.datetime.now(datetime.timezone.utc):
	print('Date / Time specified is in the future - note that time is in UTC', file=sys.stderr)
	print(dt, file=sys.stderr)
	sys.exit(DATETIMEFUTUREERROR)

## Processing
mostRecent = {}
toDel = []

# Get versions
print('Fetching all object versions', file=sys.stdout)
try:
	versions = s3.list_object_versions(Bucket=source, Prefix=sourcePrefix, MaxKeys=sys.maxsize)
except ClientError as err:
	if err.response['Error']['Code'] == 'AccessDenied':
		print('Access denied to bucket: ' + source)
		sys.exit(BUCKET403ERROR)
	elif err.response['Error']['Code'] == 'NoSuchBucket':
		print('No such bucket: ' + source)
		sys.exit(BUCKET404ERROR)
	else:
		raise err

print('Done', file=sys.stdout)

# Find most recent objects
print('Finding which versions are most recent to PIT')
for version in versions['Versions']:
	if version['LastModified'] < dt:
		if version['Key'] not in mostRecent:
			mostRecent[version['Key']] = (version['VersionId'], version['LastModified'])
		else:
			if mostRecent[version['Key']][1] < version['LastModified']:
				mostRecent[version['Key']] = (version['VersionId'], version['LastModified'])
print('Done')

# Find objects that were deleted before PIT
print('Deleting objects that were deleted before PIT', file=sys.stdout)
for version in versions['DeleteMarkers']:
	if version['LastModified'] < dt:
		if version['Key'] in mostRecent:
			if version['LastModified'] > mostRecent[version['Key']][1]:
				toDel += version['Key']
for key in toDel:
	if key in mostRecent:
		del mostRecent[key]
print('Done', file=sys.stdout)

# Put the files
totalObjects = len(mostRecent)
nObjects = 0
print('Putting output - {} objects to put'.format(totalObjects))
if destPrefix == DEFAULTDESTPREFIX:
	destPrefix = destPrefix.format(src=source, y=dt.year, M=dt.month, d=dt.day, h=dt.hour, m=dt.minute)
try:
	if (endpointRegion == ''):
		# https://github.com/boto/boto3/issues/125
		# While this doesn't throw in error - it doesn't seem to create the bucket either
		s3.create_bucket(Bucket=dest, ACL='private', CreateBucketConfiguration={})
	else:
		s3.create_bucket(Bucket=dest, ACL='private', CreateBucketConfiguration={'LocationConstraint':endpointRegion})
except ClientError as err:
	if err.response['Error']['Code'] == 'IllegalLocationConstraintException':
		print('Illegal endpoint region: ' + endpointRegion, file=sys.stderr)
		sys.exit(ILLEGALENDPOINTERROR)
for version in mostRecent:
	nObjects += 1
	s3.copy_object(ACL='private', Bucket=dest, CopySource={'Bucket':source, 'Key':version, 'VersionId':mostRecent[version][0]}, Key=destPrefix + '/' + version)
	print('{0:9d} / {1:9d} :: {2}'.format(nObjects,totalObjects,version), end='\r', flush=True, file=sys.stdout)
print('\nDone', file=sys.stdout)
