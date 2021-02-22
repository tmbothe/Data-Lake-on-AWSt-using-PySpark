import boto3
import pandas as pd
import configparser
import os
import logging
from botocore.exceptions import ClientError


config = configparser.ConfigParser()
config.read_file(open('credentials.cfg'))

def s3_client(key,secret,region_name='us-west-2'):
    """ :type : pyboto3.s3 """
    return boto3.client('s3',aws_access_key_id=key,
                         aws_secret_access_key=secret,
                         region_name=region_name
                      )


def s3_resource(key,secret,region_name='us-west-2'):
    """ :type : pyboto3.s3 """
    return boto3.resource('s3',aws_access_key_id=key,
                         aws_secret_access_key=secret,
                         region_name=region_name
                      )

def create_bucket(bucket_name,key,secret,region = 'us-east-2'):
    """
     create an s3 bucket to a specify region, default tp 'us-east-2'
     :param bucket_name : bucket to create
     :param region : region where the bucket will be create
     :return True if the bucket is created , false otherwise
    """
    try:
        s3_client(key,secret,region_name=region).create_bucket(
            Bucket = bucket_name,
            CreateBucketConfiguration={
                 'LocationConstraint': region
            }
        )  
    except ClientError as e:
        logging.error(e)
        return False
    return True


def delete_bucket(bucket_name,key,secret,region):
    """
    delete a bucket on s3 from a specified region
     :param bucket_name : bucket to delete
     :param region : region where the bucket will be deleted
     :return True if the bucket is created , false otherwise
    """
    try:
        s3_client(key,secret,region).delete_bucket(Bucket=bucket_name)
    except ClientError as e:
        logging.error(e)
        return False
    print(f"Bucket {bucket_name} deleted")
    return True

    

if __name__=='__main__':
    os.environ["AWS_ACCESS_ID"] = config['AWS']['AWS_ACCESS_KEY_ID']
    os.environ["AWS_SECRET_ACCESS_KEY"] = config['AWS']['AWS_SECRET_ACCESS_KEY']
    
    session = boto3.Session(
    aws_access_key_id=config['AWS']['AWS_ACCESS_KEY_ID'],
    aws_secret_access_key=config['AWS']['AWS_SECRET_ACCESS_KEY'],)
    
    bucket_name = config.get('BUCKET','BUCKET_NAME')
    
    print(bucket_name)
    print(f'this is the access {os.environ["AWS_ACCESS_ID"]} ')
    print(f"this is the access {config['AWS']['AWS_SECRET_ACCESS_KEY']} ")
    
    #s3 = boto3.client('s3', region_name='us-west-2')
    #print("[INFO:] Connecting to cloud")
    
    # Retrieves all regions/endpoints that work with S3

    #response = s3.list_buckets()
    #print('Regions:', response)
    
    KEY = config['AWS']['AWS_ACCESS_KEY_ID']
    SECRET = config['AWS']['AWS_SECRET_ACCESS_KEY']
    
        
    mys3 = boto3.client('s3',aws_access_key_id=KEY,
                         aws_secret_access_key=SECRET,
                         region_name='us-west-2'
                      )
        
    #mys3.create_bucket(Bucket=bucket_name,CreateBucketConfiguration={'LocationConstraint': 'us-west-2'})

    #create_bucket(bucket_name=bucket_name,key=KEY,secret=SECRET,region='us-west-2')
    delete_bucket(bucket_name=bucket_name,key=KEY,secret=SECRET,region='us-west-2')
    

