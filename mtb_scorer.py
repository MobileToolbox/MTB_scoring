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


def getStudies(syn, trackingTable='syn50615998'):
    return syn.tableQuery('select * from %s' %trackingTable).asDataFrame()

def studyEntityIds(syn, studyId):
    """Returns synapse entity ids of key components of the study.
    This is inferred from both the templetized structure of projects and the names of entities.

    :param syn:          A synapse object: syn = synapseclient.login()- Must be logged into synapse

    :param studyId:      The synapse Id of the project belonging to a study

    :returns:           A dictionary with keys: projectId, bridgeFileViewId, participantVersionsId, parquetFolderId, scoreFileId
    """
    entities = list(syn.getChildren(studyId))
    entDict =dict()
    entDict['projectId'] = studyId

    try:
        entDict['bridgeFileViewId'] = [ent['id'] for ent in entities if ent['name']=='Bridge Raw Data View' and
                                       ent['type']=='org.sagebionetworks.repo.model.table.EntityView'][0]
    except IndexError:
        entDict['bridgeFileViewId'] = None
        logger.warning(f'Study {studyId} project missing file view in Synapse.')
    try:
        entDict['participantVersionsId'] = [ent['id'] for ent in entities if ent['name']=='Participant Versions' and
                                            ent['type']=='org.sagebionetworks.repo.model.table.TableEntity'][0]
    except IndexError:
        entDict['participantVersionsId'] = None
        logger.warning(f'Study {studyId} project missing participant version table in Synapse.')
    try:
        entDict['parquetFolderId'] = [ent['id'] for ent in entities if ent['name']=='parquet' and
                                      ent['type']=='org.sagebionetworks.repo.model.Folder'][0]
    except IndexError:
        entDict['parquetFolderId'] = None
        logger.warning(f'Study {studyId} project missing parquet folder in Synapse.')

    #Score File is in subfolder. Get the folder and get add file in folder
    entDict['scoreFolderId'] = [ent['id'] for ent in entities if ent['name']=='score' and
                                ent['type']=='org.sagebionetworks.repo.model.Folder'][0]
    try:
        entDict['scoreFileId'] = list(syn.getChildren(entDict['scoreFolderId']))[0]['id']
    except IndexError:
        entDict['scoreFileId'] = None
        logger.warning(f'Study {studyId} project missing score file in Synapse.')

    return entDict


def load_data(syn, project_id):
    """ Loading parquet data into pandas dataframe
    
    Args:
        syn: Synapse object
        project_id: parquet file synID
        
    Return:
        data frames df_metadata, df_stepdata, df_task_data
    """
    df_metadata = gsyn.parquet_2_df(syn, project_id, 'dataset_archivemetadata_v1/')
    df_stepdata = gsyn.parquet_2_df(syn, project_id, 'dataset_sharedschema_v1_steps/')
    df_task_data = gsyn.parquet_2_df(syn, project_id, 'dataset_sharedschema_v1/')
    df_task_status = gsyn.parquet_2_df(syn, project_id, 'dataset_sharedschema_v1_taskStatus/')
    
    #Merge task status
    df_task_status = df_task_status.drop(columns=['id', 'index'])
    df_metadata = df_metadata.merge(df_task_status, on = ['recordid', 'assessmentid', 'year', 'month', 'day'], how='left').reset_index(drop=True)
    return df_metadata, df_stepdata, df_task_data

def get_studyreference(syn, table_id):
    """ Loading study level participant information
    
    Args:
        syn: Synapse object
        table_id: participant table synID
        
    Return:
        participant info pandas dataframe
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


def compute_scores(df_metadata, df_stepdata, df_task_data, study_df, filename):
    """Calculating MTB score
    
    Args:
        meta_info: meta info pandas dataframe
        study_df: participant info
        filename: filename info
        
    Return:
        Combined processed score for each task
    """
    spelling_score = cs.get_score(df_task_data, df_stepdata, df_metadata, 'spelling', study_df) #Spelling
    vocab_score = cs.get_score(df_task_data, df_stepdata, df_metadata, 'vocabulary', study_df) #vocabulary
    psm_score = cs.get_score(df_task_data, df_stepdata, df_metadata, 'psm', study_df) #psm
    
    score_mfs = cs.get_score(df_task_data, df_stepdata, df_metadata, 'memory-for-sequences', study_df) #mfs
    score_dccs = cs.get_score(df_task_data, df_stepdata, df_metadata, 'dccs', study_df) #dccs
    score_fname = cs.get_score(df_task_data, df_stepdata, df_metadata, 'fnameb', study_df) #fname
    
    score_nm = cs.get_score(df_task_data, df_stepdata, df_metadata, 'number-match', study_df) #Number Match
    score_flanker = cs.get_score(df_task_data, df_stepdata, df_metadata, 'flanker', study_df) #Flanker
    
    stack_merged = cs.combine_scores([score_mfs, score_dccs, score_fname, score_nm, score_flanker, psm_score, spelling_score,
                                      vocab_score], df_metadata)
    
    if not os.path.exists(home_path): #create directory if not available
        os.mkdir(home_path)
        
    stack_merged.to_csv(filename, index=False)
    return stack_merged

def upload_score(syn, file_path, git_path, des_synid):
    """ Upload final score to Syanpse
    
    Args:
        syn: synapse object
        file_path: processed file path
        git_path: github repo path
        des_synid: destination synID
        
    Return:
        Combined processed score for each task
    """
    entity = File(file_path, parent = des_synid)
    syn.store(entity, executed = git_path)
    logger.info('score uploaded sucessfully......')

    
def clean_score(file_path):
    """Remove processed score file from from local directory
    
    Args:
        file_path: file location
        
    """
    if os.path.exists(file_path):
        os.remove(file_path)

def process_scores(args, config):
    """ Processing and uploading MTB scores
    
    Args:
        args: input arguments
        config: configuration object
        
    """
    return 

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process MTB score......")
    parser.add_argument("--auth_token", help="Authentication Token", required=True)
    parser.add_argument("--git", help="github path", required=True)
    
    args = parser.parse_args()
    syn = synapseclient.login(authToken = args.auth_token)
    ## How to access authentication toward Synapse using env variables
    #import json, os, synapseclient
    # secrets=json.loads(os.getenv("SCHEDULED_JOB_SECRETS"))
    # auth_token = secrets["PAT"]
    # syn=synapseclient.Synapse()
    # syn.login(authToken=auth_token)

    #Get information about studies in Synapse
    studies = getStudies(syn)
    studies = studies.merge(pd.DataFrame([studyEntityIds(syn, id) for id in studies.id]), left_on='id', right_on='projectId')

    for i, study in studies.iterrows():
        #process_scores(args, studies)

        file_path = os.path.join(home_path, 'MTB_' + study['studyId'] + '_scores.csv')
        df_metadata, df_stepdata, df_task_data = load_data(syn, study['parquetFolderId'])
        study_df = get_studyreference(syn, study['participantVersionsId'])

        compute_scores(df_metadata, df_stepdata, df_task_data, study_df,  file_path)
        upload_score(syn, file_path, args.git, config['dest_id'][key])
        clean_score(file_path)#cleaning processed score
    
        
