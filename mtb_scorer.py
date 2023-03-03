import argparse
import pandas as pd
import logging
import os

import synapseclient
from synapseclient import File

from scoremtb.score import compute_score as cs
from scoremtb.score import get_syndata as gsyn
from scoremtb.util import util as ut

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()
#home_path = '/home/ssm-user/mtb-scorer/MTB_scoring'
home_path = os.path.join(os.getcwd(), 'MTB_scoring')

def load_data(syn, project_id):
    """
    -----------------------------------------------------------------------------------------
    
    Loading parquet data into pandas dataframe
    
    Args:
        syn: Synapse object
        project_id: parquet file synID
        
    Return:
        metadata pandas dataframe
        
    -----------------------------------------------------------------------------------------
    """
    dfid = gsyn.parquet_2_df(syn, project_id, 'dataset_archivemetadata_v1/')
    stepdata = gsyn.parquet_2_df(syn, project_id, 'dataset_sharedschema_v1_steps/')
    task_data = gsyn.parquet_2_df(syn, project_id, 'dataset_sharedschema_v1/')
    task_status = gsyn.parquet_2_df(syn, project_id, 'dataset_sharedschema_v1_taskStatus/')
    
    #Merge task status
    task_status = task_status.drop(columns=['id', 'index'])
    dfid = dfid.merge(task_status, on = ['recordid', 'assessmentid', 'year', 'month', 'day'], how='left').reset_index(drop=True)
    return dfid, stepdata, task_data

def get_studyreference(syn, table_id):
    """
    -----------------------------------------------------------------------------------------
    
    Loading study level participant information
    
    Args:
        syn: Synapse object
        table_id: participant table synID
        
    Return:
        participant info pandas dataframe
        
    -----------------------------------------------------------------------------------------
    """
    table_syn = syn.tableQuery('SELECT healthCode, participantVersion, studyMemberships FROM '+ table_id)
    
    study_reference = (table_syn.asDataFrame()
                         .sort_values(by=['healthCode', 'participantVersion'])
                         .reset_index(drop=True)
                         .loc[:,['healthCode','studyMemberships']]
                         .rename(columns={"healthCode": "healthcode"})
                         .drop_duplicates(keep='last').reset_index(drop=True)
                      )
    return study_reference


def compute_scores(meta_info, study_df, filename):
    """
    -----------------------------------------------------------------------------------------
    
    Calculating MTB score
    
    Args:
        meta_info: meta info pandas dataframe
        study_df: participant info
        filename: filename info
        
    Return:
        Comnined processed socre for each task
        
    -----------------------------------------------------------------------------------------
    """
    spelling_score = cs.get_score(meta_info[2], meta_info[1], meta_info[0], 'spelling', study_df) #Spelling
    vocab_score = cs.get_score(meta_info[2], meta_info[1], meta_info[0], 'vocabulary', study_df) #vocabulary
    psm_score = cs.get_score(meta_info[2], meta_info[1], meta_info[0], 'psm', study_df) #psm
    
    score_mfs = cs.get_score(meta_info[2], meta_info[1], meta_info[0], 'memory-for-sequences', study_df) #mfs
    score_dccs = cs.get_score(meta_info[2], meta_info[1], meta_info[0], 'dccs', study_df) #dccs
    score_fname = cs.get_score(meta_info[2], meta_info[1], meta_info[0], 'fnameb', study_df) #fname
    
    score_nm = cs.get_score(meta_info[2], meta_info[1], meta_info[0], 'number-match', study_df) #Number Match
    score_flanker = cs.get_score(meta_info[2], meta_info[1], meta_info[0], 'flanker', study_df) #Flanker
    
    stack_merged = cs.combine_scores([score_mfs, score_dccs, score_fname, score_nm, score_flanker, psm_score, spelling_score,
                                      vocab_score], meta_info[0])
    
    if not os.path.exists(home_path): #create directory if not available
        os.mkdir(home_path)
        
    stack_merged.to_csv(os.path.join(home_path, filename +'.csv'), index=False)
    return stack_merged

def upload_score(syn, file_path, git_path, des_synid):
    """
    -----------------------------------------------------------------------------------------
    
    Upload final score to Syanpse
    
    Args:
        syn: synapse object
        file_path: processed file path
        git_path: github repo path
        des_synid: destination synID
        
    Return:
        Comnined processed socre for each task
        
    -----------------------------------------------------------------------------------------
    """
    entity = File(file_path, parent = des_synid)
    syn.store(entity, executed = git_path)
    logger.info('score uploaded sucessfully......')
    
def clean_score(file_path):
    """
    -----------------------------------------------------------------------------------------
    
    Removing processed socre from local
    
    Args:
        file_path: file location
        
    -----------------------------------------------------------------------------------------
    """
    if os.path.exists(file_path):
        os.remove(file_path)
    
def process_scores(args, config):
    """
    -----------------------------------------------------------------------------------------
    
    Processing and uploading MTB scores
    
    Args:
        args: input arguments
        config: configuration object
        
    -----------------------------------------------------------------------------------------
    """
    for key, value in config['project_id'].items():
        try:

            file_path = os.path.join(home_path, 'MTB_' + key + '_scores.csv')
            metadata = load_data(syn, config['project_id'][key])
            study_df = get_studyreference(syn, config['study_id'][key])
            
            compute_scores(metadata, study_df, 'MTB_' + key + '_scores')
            upload_score(syn, file_path, args.git, config['dest_id'][key])
            clean_score(file_path)#cleaning processed score

        except Exception as e:
            logger.info('Error in scoring pipeline')
        

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process MTB score......")
    parser.add_argument("--auth_token", help="Authentication Token", required=True)
    parser.add_argument("--git", help="github path", required=True)
    
    args = parser.parse_args()
    syn = synapseclient.login(authToken = args.auth_token)
    
    config = ut.get_config()
    process_scores(args, config)
    
        