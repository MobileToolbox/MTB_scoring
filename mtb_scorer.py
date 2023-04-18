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

def _load_dccs_interaction_data(syn, project_id, filters='dccs'):
    """ Uses interaction events to impute DCCS reaction times

    Args:
        syn: Synapse object
        project_id: parquet file synID
        filters: list of filters passed to load Parquet function

    return interactiondata data frame
    """
    df_interactions = gsyn.parquet_2_df(syn, project_id, 'dataset_sharedschema_v1_userInteractions/', filters)
    df_controlEvents = gsyn.parquet_2_df(syn, project_id, 'dataset_sharedschema_v1_userInteractions_controlEvent/', filters)

    interactionsData = (df_interactions
                        .merge(df_controlEvents[['id', 'recordid', 'userInteractions.val.controlEvent.val']],
                            left_on=['recordid', 'controlEvent'],
                            right_on=['recordid', 'id'],
                            suffixes=[None, '_controlEvent'],
                            how='left')
                        .drop('id_controlEvent', axis=1))  #Remove duplicated columns
    return interactionsData


def _computeDT(df):
    """ Pull out the last 'uiEnabled' and the first 'touchDown' and compute the time difference

       parameters df data frame for one id and stepidentifier sorted by index

       returns same dataframe with additional ResponseTime_in_milSecond column
    """

    # Find last row where userInteractions is 'uiEnabled' for this stepIdentifier
    idx_uiEnabled =   df[df["userInteractions.val.controlEvent.val"]=="uiEnabled"].index[-1]

    # Return responseTime of 2000 if no touchDown recorded after uiEnabled
    if 'touchDown' not in set(df.loc[idx_uiEnabled:,"userInteractions.val.controlEvent.val"]):
        return  df.assign(ResponseTime_in_milSecond = float('nan'))

    # Find the first row with 'touchDown', but after the last uiEnabled row
    idx_touchDown = (df.loc[idx_uiEnabled:,:]
                     .query('`userInteractions.val.controlEvent.val` == "touchDown"')
                     .index[0])

    dt = int(pd.to_datetime(df.loc[[idx_uiEnabled, idx_touchDown],'timestamp']) #select 2 rows
                      .diff()                  #Compute difference in time 
                      .dt.total_seconds()      #Convert to seconds
                      .iloc[-1]*1000)          #select last row (of 2 rows, first row is NaN) and convert to ms

    return  df.assign(ResponseTime_in_milSecond = dt)


def impute_missing_dccs_timing(syn, project_id, df_stepdata, filters):
    """Imputes response times from interaction data and merges this with stepdata

    Args:
        syn: Synapse object
        project_id: parquet file synID
        df_stepdata: original stepdata data frame
        filters: list of filters passed to load Parquet function

    return updated stepdata data framw with updated timing information
    """
    df = _load_dccs_interaction_data(syn, project_id, filters)

    #Determine deltaT in ms for each id and stepIdentifier and remove duplicates
    timing = df.groupby(['id', 'stepIdentifier']).apply(_computeDT)
    timing = timing[['id', 'recordid', 'stepIdentifier', 'ResponseTime_in_milSecond']].drop_duplicates()

    #Merge new itming information with df_stepdata
    #TODO move the ResponseTime_in_milSecond  to responseTime if happy with results
    return df_stepdata.merge(timing,
                             left_on= ['recordid', 'identifier', 'id'],
                             right_on=['recordid', 'stepIdentifier', 'id', ])

    
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
        logger.info(study['studyId']+' '+study['name'])
        study_df = get_studyreference(syn, study['participantVersionsId'])

        scores = []
        for assessmentId in ['dccs', 'spelling','vocabulary', 'psm','memory-for-sequences', 'fnameb', 'number-match', 'flanker']:
            print(assessmentId)
            filter = [('assessmentid', '=', assessmentId)]
            df_metadata, df_stepdata, df_task_data = load_data(syn, study['parquetFolderId'], filter)
            #Impute reaction time data for dcccs data for older studies
            if assessmentId=='dccs':
                df_steps = impute_missing_dccs_timing(syn, study['parquetFolderId'], df_stepdata, filter)

            scores.append(cs.get_score(df_task_data, df_stepdata, df_metadata, assessmentId, study_df))

        stack_merged = cs.combine_scores(scores, df_metadata).sort_values('startDate')
        stack_merged.to_csv(os.path.join(home_path, 'MTB_' + study['studyId'] + '_scores.csv'), index=False)

        #upload_score(syn, file_path, args.git, config['dest_id'][key])
        #clean_score(file_path)#cleaning processed score

