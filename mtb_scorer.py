import argparse
import numpy as np
import pandas as pd
import logging
import os

import synapseclient
from synapseclient import File

from scoremtb.score import compute_score as cs
from scoremtb.score import get_syndata as gsyn
from scoremtb.util import util as ut
from scoremtb.compute_response_time import impute_missing_response_timing 

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
    entDict['scoreFolderId'] = [ent['id'] for ent in entities if ent['name']=='scores' and
                                ent['type']=='org.sagebionetworks.repo.model.Folder'][0]
    try:
        entDict['scoreFileId'] = list(syn.getChildren(entDict['scoreFolderId']))[0]['id']
    except IndexError:
        entDict['scoreFileId'] = None
        logger.warning(f'Study {studyId} project missing score file in Synapse.')

    return entDict


def load_data(syn, project_id, filters=None):
    """ Loading parquet data into pandas dataframe
    
    Args:
        syn: Synapse object
        project_id: parquet file synID
        filters: list of filters passed to load Parquet function

    Return:
        data frames df_metadata, df_stepdata, df_task_data
    """
    df_metadata = gsyn.parquet_2_df(syn, project_id, 'dataset_archivemetadata_v1/', filters)
    df_stepdata = gsyn.parquet_2_df(syn, project_id, 'dataset_sharedschema_v1_steps/', filters)
    df_task_data = gsyn.parquet_2_df(syn, project_id, 'dataset_sharedschema_v1/', filters)
    df_task_status = gsyn.parquet_2_df(syn, project_id, 'dataset_sharedschema_v1_taskStatus/', filters)
    
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

    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process MTB score......")
    parser.add_argument("--auth_token", help="Authentication Token", required=True)
    parser.add_argument("--git", help="github path", required=True)
    
    args = parser.parse_args()
    syn = synapseclient.login(authToken = args.auth_token)

    # Get information about studies in Synapse
    studies = getStudies(syn)
    studies = studies.merge(pd.DataFrame([studyEntityIds(syn, id) for id in studies.id]), left_on='id', right_on='projectId')

    # Create storage location of output files
    if not os.path.exists(home_path): #create directory if not available
         os.mkdir(home_path)

    for i, study in studies.iterrows():
        if study['studyId'] != 'mtbwrj':  #TODO remove to work across all data 
            continue
        logger.info(study['studyId']+' '+study['name'])
        study_df = get_studyreference(syn, study['participantVersionsId'])
        assessment_locations = gsyn.find_study_assessment_paths(syn, study['parquetId'])
        scores = []
        for assessmentId in ['dccs', 'spelling','vocabulary', 'psm','memory-for-sequences', 'fnameb', 'number-match', 'flanker']:
            print(study['studyId'], assessmentId)
            filter = [('assessmentid', '=', assessmentId)]
            df_metadata, df_stepdata, df_task_data = load_data(syn, study['parquetFolderId'], filter)
            #Impute reaction time data for dccs data for older studies
            if assessmentId in ['dccs', 'flanker']:
                df_stepdata.to_csv('20230510_imputed_rt/step_data_%s_%s_before.csv' %(study['studyId'], assessmentId)) #TODO remove
                df_missing = impute_missing_response_timing(syn, study['parquetFolderId'], df_stepdata, filter, assessmentId)
                #TODO move this to impute function if the results look good
                df_stepdata['responseTime'] = df_missing['imputed_rt_ms']
                df_stepdata.to_csv('20230510_imputed_rt/step_data_%s_%s_imputed.csv' %(study['studyId'], assessmentId)) #TODO remove
            scores.append(cs.get_score(df_task_data, df_stepdata, df_metadata, assessmentId, study_df))

        stack_merged = cs.combine_scores(scores, df_metadata).sort_values('startDate')
        stack_merged.to_csv(os.path.join(home_path, 'MTB_' + study['studyId'] + '_scores.csv'), index=False)

        #upload_score(syn, file_path, args.git, config['dest_id'][key])
        #clean_score(file_path)#cleaning processed score
