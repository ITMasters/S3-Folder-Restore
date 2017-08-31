#!python3

## Imports
import boto3
import argparse
import datetime
from dateutil import parser as dateParser
import sys
from botocore.exceptions import ClientError, ProfileNotFound

## Constants
# Defaults
DEFAULTSOURCEPREFIX = ''
DEFAULTTIME = '00:00'
DEFAULTDEST = 'source-bucket'
DEFAULTDESTPREFIX = 'PIT-Restore-{src}-{y:04d}-{M:02d}-{d:02d}-{h:02d}:{m:02d}'
DEFAULTACCESSKEY = ''
DEFAULTSECRETKEY = ''
DEFAULTSESSIONTOKEN = ''
DEFAULTPROFILE = 'default'
DEFAULTENDPOINTREGION = ''
# Errors / Exits
SUCCESS = 0
DATETIMEFORMATERROR = 1
DATETIMEFUTUREERROR = 2
BUCKET404ERROR = 3
BUCKET403ERROR = 4
ILLEGALENDPOINTERROR = 5
INVALIDACCESSKEYERROR = 6

## DATE SHOULD BE DATETTIME
## HANDLE CREDENTIALS
def restore(source, dt, sourcePrefix=DEFAULTSOURCEPREFIX, dest=DEFAULTDEST, destPrefix=DEFAULTDESTPREFIX, accessKey=DEFAULTACCESSKEY, ecretKey=DEFAULTSECRETKEY, sessionToken=DEFAULTSESSIONTOKEN, profile=DEFAULTPROFILE, endpointRegion=DEFAULTENDPOINTREGION, silent=True):
	if silent == True:
		printOutput = None
	else:
		printOutput = sys.stdout
	printErrOutput = sys.stderr

	try:
		session = boto3.Session(profile_name=profile)
	except ProfileNotFound as err:
		if (sessionToken == ''):
			session = boto3.Session(aws_access_key_id=accessKey, aws_secret_access_key=secretKey)
		else:
			session = boto3.Session(aws_access_key_id=accessKey, aws_secret_access_key=secretKey, aws_session_token=sessionToken)

	if (endpointRegion==''):
		s3 = session.client('s3', region_name='us-east-1')
	else:
		s3 = session.client('s3', region_name=endpointRegion)

	## Processing
	mostRecent = {}
	toDel = []

	# Get versions
	print('Fetching all object versions', file=printOutput)
	try:
		versions = s3.list_object_versions(Bucket=source, Prefix=sourcePrefix, MaxKeys=sys.maxsize)
	except ClientError as err:
		if err.response['Error']['Code'] == 'AccessDenied':
			print('Access denied to bucket: ' + source, file=printErrOutput)
			return (BUCKET403ERROR)
		elif err.response['Error']['Code'] == 'NoSuchBucket':
			print('No such bucket: ' + source, file=printErrOutput)
			return (BUCKET404ERROR)
		elif err.response['Error']['Code'] == 'InvalidAccessKeyId':
			print('Invalid Access Key: ' + accessKey, file=printErrOutput)
			return (INVALIDACCESSKEYERROR)
		else:
			raise err
	print('Done', file=printOutput)

	# Find most recent objects
	print('Finding which versions are most recent to PIT', file=printOutput)
	for version in versions['Versions']:
		if version['LastModified'] < dt:
			if version['Key'] not in mostRecent:
				mostRecent[version['Key']] = (version['VersionId'], version['LastModified'])
			else:
				if mostRecent[version['Key']][1] < version['LastModified']:
					mostRecent[version['Key']] = (version['VersionId'], version['LastModified'])
	print('Done', file=printOutput)

	# Find objects that were deleted before PIT
	print('Deleting objects that were deleted before PIT', file=printOutput)
	for version in versions['DeleteMarkers']:
		if version['LastModified'] < dt:
			if version['Key'] in mostRecent:
				if version['LastModified'] > mostRecent[version['Key']][1]:
					toDel += version['Key']
	for key in toDel:
		if key in mostRecent:
			del mostRecent[key]
	print('Done', file=printOutput)

	# Put the files
	totalObjects = len(mostRecent)
	nObjects = 0
	print('Putting output - {} objects to put'.format(totalObjects), file=printOutput)
	if dest == DEFAULTDEST:
		dest = source
	if destPrefix == DEFAULTDESTPREFIX:
		destPrefix = destPrefix.format(src=source, y=dt.year, M=dt.month, d=dt.day, h=dt.hour, m=dt.minute)
	try:
		if (endpointRegion == ''):
			s3.create_bucket(Bucket=dest, ACL='private')
		else:
			s3.create_bucket(Bucket=dest, ACL='private', CreateBucketConfiguration={'LocationConstraint':endpointRegion})
	except ClientError as err:
		if err.response['Error']['Code'] == 'IllegalLocationConstraintException':
			print('Illegal endpoint region: ' + endpointRegion, file=printErrOutput)
			return (ILLEGALENDPOINTERROR)
	for version in mostRecent:
		nObjects += 1
		s3.copy_object(ACL='private', Bucket=dest, CopySource={'Bucket':source, 'Key':version, 'VersionId':mostRecent[version][0]}, Key=destPrefix + '/' + version)
		print('{0:9d} / {1:9d} :: {2}'.format(nObjects,totalObjects,version), end='\r', flush=True, file=printOutput)
	print('\nDone', file=printOutput)
	return SUCCESS

if __name__ == '__main__':
	printOutput = sys.stdout
	printErrOutput = sys.stdout

	## Arg parsing
	argParser = argparse.ArgumentParser(description='Script for restoring a point in time for a given bucket in S3')
	argParser.add_argument('source', type=str, help='Source bucket')
	argParser.add_argument('date', type=str, help='date to restore to in yyyy-MM-dd format')
	argParser.add_argument('--source-prefix', type=str, default=DEFAULTSOURCEPREFIX, help='Source prefix - defaults to empty string (everything in bucket)')
	argParser.add_argument('--time', type=str, default=DEFAULTTIME, help='time to restore to in UTC formatted as HH:MM 24 hour')
	argParser.add_argument('--dest', type=str, default=DEFAULTDEST, help='Destination bucket, will be created if it does not exist - defaults to same bucket')
	argParser.add_argument('--dest-prefix', type=str, default=DEFAULTDESTPREFIX, help='destination prefix - defaults to PIT-Restore-dd/MM/yyy-HH:MM')
	argParser.add_argument('--access-key', type=str, default=DEFAULTACCESSKEY, help='AWS Access Key, if not using default AWS API profile')
	argParser.add_argument('--secret-key', type=str, default=DEFAULTSECRETKEY, help='AWS Secret Key, if not using default AWS API profile')
	argParser.add_argument('--session-token', type=str, default=DEFAULTSESSIONTOKEN, help='AWS Session Token, if required')
	argParser.add_argument('--profile', type=str, default=DEFAULTPROFILE, help='AWS API Profile to use - uses default profile by default')
	argParser.add_argument('--endpoint-region', type=str, default=DEFAULTENDPOINTREGION, help='AWS S3 endpoint region - defaults to us-east-1')
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
			print('Using AWS API profile: ' + profile, file=printOutput)
		except ProfileNotFound as err:
			print('AWS API profile not found: ' + profile, file=printOutput)

	if not session:
		if (accessKey == ''):
			accessKey = input('Enter your AWS access key: ')
		if (secretKey == ''):
			secretKey = input('Enter your AWS secret key: ')
		if (sessionToken == ''):
			sessionToken = input('Enter your AWS session token (optional - leave blank for no token): ')

	# Datetime
	try:
		dt = dateParser.parse(args['date'] + 'T' + args['time'] + ' UTC')
	except ValueError as err:
		print('Date / Time format error. Ensure date is formatted yyyy-MM-dd and time is formatted HH:MM in 24 hour UTC format, if supplied', file=printErrOutput)
		print(err, file=printErrOutput)
		sys.exit(DATETIMEFORMATERROR)
	if dt > datetime.datetime.now(datetime.timezone.utc):
		print('Date / Time specified is in the future - note that time is in UTC', file=printErrOutput)
		print(dt, file=printErrOutput)
		sys.exit(DATETIMEFUTUREERROR)

	result = restore(source, dt, sourcePrefix, dest, destPrefix, accessKey, secretKey, sessionToken, profile, endpointRegion, silent=False)
	sys.exit(result)