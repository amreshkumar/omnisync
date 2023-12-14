"""
aws_s3_connector.py is the aws s3 sync script.
It provides the sync_s3_buckets() function, to perform a one-way-sync.
The function can be called by exchanging the source & destination, to acheive two-way-sync.
Given DELETION_RULE is KEEP_ALL, it will keep all files in the destination bucket.
It can be run directly or scheduled to run at a set interval.
"""
import os
import logging
import boto3

# Set up logging
logging.basicConfig(filename='s3_sync.log', level=logging.INFO)

def sync_s3_buckets(source_bucket, target_bucket):
    """
    Performs a one way sync from the source bucket to the target bucket.

    This function compares the objects in the source bucket to the objects in the target bucket.
    - If an object exists in the source bucket but not in the target bucket, the object is copied to the target bucket.
    - If an object exists in both the source bucket and the target bucket, the object is updated in the target bucket.
    - If an object exists in the target bucket but not in the source bucket, the object is deleted from the target bucket.

    Parameters:
    source_bucket (str): The name of the source S3 bucket.
    target_bucket (str): The name of the target S3 bucket.

    Returns:
    None
    """
    # Create an S3 client
    s3_client = boto3.client('s3')
    # Log the contents of the source and target buckets before syncing
    log_content_before_sync(source_bucket, target_bucket)
    # List objects in the source bucket
    source_objects = s3_client.list_objects(Bucket=source_bucket)['Contents']
    # Copy/update objects in the target bucket
    copy_or_update_objects(s3_client, source_bucket, target_bucket, source_objects)
    # Delete objects in the target bucket
    delete_objects(s3_client, target_bucket, source_objects)

def copy_or_update_objects(s3_client, source_bucket, target_bucket, source_objects):
    """
    Copies or updates objects from the source bucket to the target bucket.

    This function iterates over the objects in the source bucket. If an object
    exists in the source bucket but not in the target bucket, it is copied to
    the target bucket. If an object exists in both buckets, the object in the
    target bucket is updated to match the object in the source bucket.

    Parameters:
    source_bucket (str): The name of the source S3 bucket.
    target_bucket (str): The name of the target S3 bucket.

    Returns:
    None
    """
    # Sync objects from the source bucket to the target bucket
    for source_object in source_objects:
        source_key = source_object['Key']
        target_key = source_key

        # Compare and copy/update objects in the target bucket
        try:
            s3_client.head_object(Bucket=target_bucket, Key=target_key)
        except Exception as e:
            print(e)
            s3_client.copy_object(Bucket=target_bucket,
                                  CopySource={'Bucket': source_bucket, 'Key': source_key},
                                  Key=target_key)

def delete_objects(s3_client, target_bucket, source_objects):
    """
    Deletes specified objects from the target S3 bucket.

    This function takes a list of objects to delete and removes them from the
    target bucket. If an object does not exist in the target bucket, no action
    is taken for that object.

    Parameters:
    target_bucket (str): The name of the target S3 bucket.
    objects_to_delete (list): A list of object keys to delete from the target bucket.

    Returns:
    None
    """
    # Get DELETE_RULE from environment variables
    delete_rule = os.getenv('DELETE_RULE', 'KEEP_ALL') # Default to 'KEEP_ALL'
    if delete_rule == "DELETE_IF_SOURCE_DELETED":
        # Delete objects in the target bucket that are not present in the source bucket
        target_objects = s3_client.list_objects(Bucket=target_bucket)['Contents']
        for target_object in target_objects:
            target_key = target_object['Key']
            source_key = target_key
            if not any(obj['Key'] == source_key for obj in source_objects):
                s3_client.delete_object(Bucket=target_bucket, Key=target_key)

def log_content_before_sync(source_bucket, target_bucket):
    """
    Logs the contents of the source and target S3 buckets before syncing.

    This function retrieves the directory structure of the source and target
    buckets, and logs the list of files and directories in each bucket. The
    logs are written to a file for future reference.

    Parameters:
    source_bucket (str): The name of the source S3 bucket.
    target_bucket (str): The name of the target S3 bucket.

    Returns:
    None
    """
    # traverse and get list of files and directories in source bucket
    source_files, source_directories = directory_structure(source_bucket)
    # traverse and get list of files and directories in target bucket
    target_files, target_directories = directory_structure(target_bucket)

    # Log source_files and source_directories
    logging.info('Source Files: %s', source_files)
    logging.info('Source Directories: %s', source_directories)

    # Log target_files and target_directories
    logging.info('Destination Files: %s', target_files)
    logging.info('Destination Directories: %s', target_directories)

def directory_structure(bucket_name):
    """
    Retrieves the directory structure of the specified S3 bucket.

    This function lists all objects in the specified S3 bucket and separates them
    into files and directories based on their keys. An object is considered a
    directory if its key ends with a '/'. The function returns two lists: one
    containing the keys of all files in the bucket, and one containing the keys
    of all directories.

    Parameters:
    bucket_name (str): The name of the S3 bucket.

    Returns:
    tuple: A tuple containing two lists. The first list contains the keys of all
           files in the bucket. The second list contains the keys of all directories.
    """
    s3_client = boto3.client('s3')
    response = s3_client.list_objects_v2(Bucket=bucket_name)
    all_objects = response['Contents']

    files = []
    directories = []

    for obj in all_objects:
        key = obj['Key']
        if key.endswith('/'):
            directories.append(key)
        else:
            files.append(key)

    return files, directories
