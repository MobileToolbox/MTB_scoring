# MTB Scoring

# import the required packages
import synapseclient
from pyarrow import fs
import pyarrow.parquet as pq
from collections import defaultdict

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


def merge_score_columns(df):
    """Merges columns with same name but different extensions
       Fix issue where scores are sometimes stored under scores_finalTheta_double and scores_finalSE_double and
       other times: scores_finalTheta, scores_finalSE
       Likely related to: Glue Bug in BridgeDownstream:  https://sagebionetworks.jira.com/browse/ETL-238


    Args:
        df: data frame with columns names scores_...

    Returns:
        results: Pandas dataframe.
    """
    # Regular expression patterns
    pattern_int = re.compile(r'^(.+)_int$')
    pattern_double = re.compile(r'^(.+)_double$')

    score_cols = [name for name in df.columns if name.startswith('scores_')]

    #Group columns based on extensions _int _double etc.
    grouped_names = defaultdict(list)
    for value in score_cols:
        match_int = pattern_int.match(value)
        match_double = pattern_double.match(value)
        if match_int:
            name = match_int.group(1)
        elif match_double:
            name = match_double.group(1)
        else:
            name = value
        grouped_names[name].append(value)

    #Go through every column base name (e.g finalTheta) and reassign to basename e.g. finalTheta_int to finalTheta
    for name, values in grouped_names.items():
        if len(values)==1 and name==values[0]: #We only have one column and it is correct.
            continue
        if len(values)==1: #There is one column but it has the wrong name
            df.rename(columns={values[0]: name})
        elif len(values)==2:
            df[name] = df[values[0]].combine_first(df[values[1]])
            df = df.drop(values, axis=1)
        else:
            logger.error(f'There are to many score Columns for {name}: {values}')
    return df
