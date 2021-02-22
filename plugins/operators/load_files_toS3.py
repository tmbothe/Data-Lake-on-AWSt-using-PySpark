import boto3
import os
import threading
import sys
import configparser
import threading
from boto3.s3.transfer import TransferConfig
from create_resources import s3_resource

config = configparser.ConfigParser()
config.read_file(open('credentials.cfg'))

def upload_file(bucket_name,key,secret,region,filepath,key_path):
    myconfig = TransferConfig(multipart_threshold=1024 * 25, max_concurrency=10,
                             multipart_chunksize=1024 * 25,use_threads=True)
    s3_resource(key,secret,region).meta.client.upload_file(file_path,bucket_name,key_path,
                                                           ExtraArgs={'ACL': 'public-read', 'ContentType': 'text/pdf'},
                                                           Config=myconfig
                                                           )

    
    
     
    
    
    
    
    
    
    
    
    
    
if __name__=='__main__':
   

    file_path = os.path.join("/home/workspace","airport-codes_csv.csv")
    
    key_path  ='data/airport-codes_csv.csv'
    print(file_path)
    KEY = config['AWS']['AWS_ACCESS_KEY_ID']
    SECRET = config['AWS']['AWS_SECRET_ACCESS_KEY']
    bucket_name = config.get('BUCKET','BUCKET_NAME')
    
    #upload_file(bucket_name,KEY,SECRET,'us-west-2',file_path,key_path)
    
    dem_path = os.path.join("/home/workspace","us-cities-demographics.csv")
    dem_key_path  ="data/us-cities-demographics.csv"
    #upload_file(bucket_name,KEY,SECRET,'us-west-2',dem_path,dem_key_path)
    
    temp_path = os.path.join("/home/workspace","GlobalLandTemperaturesByState.csv")
    temp_key_path  ="data/GlobalLandTemperaturesByState.csv"
    #upload_file(bucket_name,KEY,SECRET,'us-west-2',temp_path,temp_key_path)
    
     
    root_path = os.path.join("/home/workspace","GlobalLandTemperaturesByState.csv")
    temp_key_path  ="data/GlobalLandTemperaturesByState.csv"
    #upload_file(bucket_name,KEY,SECRET,'us-west-2',temp_path,temp_key_path)
    
    imidir = "/home/workspace/sas_data"
    for root,dir,files in os.walk(imidir):
        for file in files:
            if file.endswith(".parquet"):
                root_path = os.path.join(root,file)
                print(file)
                key_path=f"sas_data/{file}"
                upload_file(bucket_name,KEY,SECRET,'us-west-2',root_path,key_path)
                
    

