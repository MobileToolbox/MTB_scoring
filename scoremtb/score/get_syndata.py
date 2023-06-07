# MTB Scoring

# import the required packages
import synapseclient
from pyarrow import fs
import pyarrow.parquet as pq

import pandas as pd
import os
import json
import logging
import re

logging.basicConfig(level=logging.INFO)
logger=logging.getLogger()


def get_s3_fs(entity_id, syn):
    """Get S3filesystem and bucket path
    
    Args:
        entity_id: Synapse entity ID
        syn: Synapse object
        
    Returns:
        results: S3filesystem and path
    """
    
    token = syn.get_sts_storage_token(entity_id, permission="read_only")
    s3 = fs.S3FileSystem(access_key=token['accessKeyId'], secret_key = token['secretAccessKey'], 
                         session_token = token['sessionToken'])
    
    bucket_path = token['bucket']+'/'+token['baseKey']+'/'
    return s3, bucket_path


def parquet_2_df(syn, entity_id, dataset, filters=None):
    """Loading (from synapse) and converting parquet dataset to pandas dataframe
    
    Args:
        syn: Synapse client object
        entity_id: Synapse entity ID
        dataset: data source relative path (like: dataset_metadata)
        
    Returns:
        results: Pandas dataframe with dataset from Synapse.
    """
    
    s3, bucket_path = get_s3_fs(entity_id, syn)
    dataset_path = bucket_path + dataset
    
    dataset = pq.ParquetDataset(dataset_path, filesystem=s3, filters=filters)
    dfid = dataset.read().to_pandas()
    return dfid


