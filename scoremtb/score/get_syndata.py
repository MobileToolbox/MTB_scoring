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


def find_study_assessment_paths(syn, entity_id):
    """Peaks into Parquet folder to determine what assessments and format of Parquet data
    
    Args:
        syn: Synapse object
        entity_id: Synapse id of folder with parquet files
        filters: list of filters passed to load Parquet function

    Return:
        list of assessments and paths to metadata, steps, data, and taskStatus for each assessment
    """

    s3, bucket_path = get_s3_fs(entity_id, syn)
    dataset_list = s3.get_file_info(fs.FileSelector(bucket_path, recursive=False))
    pathnames = [folder.path for folder in dataset_list]
    foldernames = [path.split('/')[-1] for path in pathnames]
    foldernames.remove('dataset_archivemetadata_v1')
    #Determine if "dataset_sharedschema_v1" is in the list of files which means it is old schema
    paths = {}
    if 'dataset_sharedschema_v1' in foldernames:
        # Query the subfolder and find the different assessments.
        assessment_list = s3.get_file_info(fs.FileSelector(bucket_path+'dataset_sharedschema_v1', recursive=False))
        assessment_list = [path.path.split('/')[-1].split('=')[1] for path in assessment_list]
        known_paths = {'meta_path': 'dataset_archivemetadata_v1/',
                        'step_path': 'dataset_sharedschema_v1_steps/',
                        'task_path': 'dataset_sharedschema_v1/',
                        'task_status_path': 'dataset_sharedschema_v1_taskStatus/'}
        paths = {assessmentId: known_paths for assessmentId in assessment_list}
        foldernames = [name for name in foldernames if name not in['dataset_sharedschema_v1_steps/',
                                                                   'dataset_sharedschema_v1/',
                                                                   'dataset_sharedschema_v1_taskStatus/']]
    #Go through remaining files and look for formated filenames according to '_dataset_(.*?)_v1_(.*)'      
    assessmentSearches = [re.search(r'dataset_(.*?)_v1_(.*)', name) for name in foldernames]
    assessmentSearches = [match.group(1) for match in assessmentSearches if match]
    for assessmentId in set(assessmentSearches):
        paths[assessmentId]={'meta_path': 'dataset_archivemetadata_v1',
                            'step_path': f'df_dataset_{assessmentId}_v1_steps',
                            'task_path': f'dataset_{assessmentId}_v1',
                            'task_status_path': None}
        #Remove filenames that have been assigned to specific assessments and types
        foldernames = [name for name in foldernames if assessmentId not in name]
    print(foldernames)
    return paths

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


