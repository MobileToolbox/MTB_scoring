# MTB Scoring

# import the required packages
import synapseclient
from pyarrow import fs
import pyarrow.parquet as pq

import pandas as pd
import os
import json
import logging

logging.basicConfig(level=logging.INFO)
logger=logging.getLogger()

def get_synclient(USER_NAME= None, PASS=None):
    """
    -----------------------------------------------------------------------------------------
    
    Get synapse credential
    
    Args:
        USER_NAME: Username
        PASS: Password
        
    Returns:
        results: synapse object
        
    -----------------------------------------------------------------------------------------
    """
    syn = synapseclient.login(USER_NAME, PASS)
    return syn

def get_s3_fs(PROJECT_ID, syn):
    """
    -----------------------------------------------------------------------------------------
    
    Get S3filesystem and bucket path
    
    Args:
        PROJECT_ID: Synapse project ID
        syn: Synapse object
        
    Returns:
        results: S3filesystem and path
        
    -----------------------------------------------------------------------------------------
    """
    
    token = syn.get_sts_storage_token(PROJECT_ID, permission="read_only")
    s3 = fs.S3FileSystem(access_key=token['accessKeyId'], secret_key = token['secretAccessKey'], 
                         session_token = token['sessionToken'])
    
    bucket_path = token['bucket']+'/'+token['baseKey']+'/'
    return s3, bucket_path

def get_data(syn, PROJECT_ID, dataset):
    """
    -----------------------------------------------------------------------------------------
    
    Get data from synapse
    
    Args:
        PROJECT_ID: Synapse project ID
        syn: Synapse object
        
    Returns:
        results: synapse data
        
    -----------------------------------------------------------------------------------------
    """
    
    s3, bucket_path = get_s3_fs(PROJECT_ID, syn)
    dataset_path = bucket_path + dataset
    
    dataset = pq.ParquetDataset(dataset_path, filesystem=s3)
    dfid = dataset.read().to_pandas()
    return dfid


