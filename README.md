# S3-Folder-Restore
Python script for restoring versioned folders or buckets in S3 to a point in time.

## Usage
```
S3-point-in-time-recovery.py 	[-h] [--source-prefix SOURCE_PREFIX] 
                                [--time TIME] [--dest DEST] 
                                [--dest-prefix DEST_PREFIX] 
                                [--access-key ACCESS_KEY] 
                                [--secret-key SECRET_KEY] 
                                [--session-token SESSION_TOKEN] 
                                [--profile PROFILE] 
                                [--endpoint-region ENDPOINT_REGION] 
                                source date 
  
Script for restoring a point in time for a given bucket in S3 
  
positional arguments: 
  source                              Source bucket 
  date                                date to restore to in yyyy-MM-dd format 
  
optional arguments: 
  -h, --help                          show this help message and exit 
  --source-prefix SOURCE_PREFIX       Source prefix - defaults to empty string (everything in bucket) 
  --time TIME                         time to restore to in UTC formatted as HH:MM 24 hour 
  --dest DEST                         Destination bucket, will be created if it does not exist - defaults to same bucket 
  --dest-prefix DEST_PREFIX           destination prefix - defaults to PIT-Restore-dd/MM/yyy-HH:MM 
  --access-key ACCESS_KEY             AWS Access Key, if not using default AWS API profile 
  --secret-key SECRET_KEY             AWS Secret Key, if not using default AWS API profile 
  --session-token SESSION_TOKEN       AWS Session Token, if required 
  --profile PROFILE                   AWS API Profile to use - uses default profile by default 
  --endpoint-region ENDPOINT_REGION   AWS S3 endpoint region - defaults to us-east-1 
                                      (NOTE: Boto3 bug #125 means that us-east-1 won't create a bucket for you if it does not exist 
```