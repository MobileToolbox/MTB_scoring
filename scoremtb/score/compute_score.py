# MTB Scoring

# import the required packages
import pandas as pd
from scoremtb.util import util as ut
from scoremtb.score import score_commons as sc
import logging

logging.basicConfig(level=logging.INFO)
logger=logging.getLogger()

def get_score(task_data, stepdata, df_metadata, assmnt_val, study_membership):
    """
    -----------------------------------------------------------------------------------------
    
    Compute score for a given assesment (like: PSM/Vocab/Speell/DCCS etc.)
    
    Args:
        task_data: taskdata pandas dataframe downloaded from Synapse
        stepdata: stepdata pandas dataframe downloaded from Synapse
        df_metadata: metadata pandas dataframe downloaded from Synapse
        assmnt_val: Assesment value
        study_membership: study membership info
        
    Return:
        A pandas dataframe with stacked MTB scores for a given assesment
        
    -----------------------------------------------------------------------------------------
    """
    config = ut.get_config()
    assmnt_val = assmnt_val.lower()
    
    if assmnt_val in ['psm', 'vocabulary', 'spelling','3drotation', 'letternumberseries', 'verbalreasoning', 'progressivematrices']:
        score_df = sc.get_svp_score(task_data, df_metadata, assmnt_val, config, study_membership) #TODO rename to indicate that it fetch score
    elif assmnt_val in ['dccs', 'memory-for-sequences', 'fnameb', 'number-match', 'flanker']:
        score_df = sc.get_common_score(stepdata, task_data, df_metadata, assmnt_val, config)   #TODO rename to compute score
    else:
        logger.error(f'We have a unkown assessment that we can\'t score: {assmnt_val}')
        
    score_df = pd.merge(score_df, study_membership, how='inner', on = 'healthcode')
    score_df['score_dict'] = score_df[config[assmnt_val + '_score']].to_dict(orient='records')
    score_df = score_df.drop(columns=config[assmnt_val + '_score'])
    
    stacked_score = ut.stack_score(score_df)
    return stacked_score

def combine_scores(score_list, df_metadata):
    """
    -----------------------------------------------------------------------------------------

    Concatenating assesment scores

    Args: 
        score_list: list of score for each assesment
        df_metadata: metadata pandas dataframe downloaded from Synapse

    Returns:
       scores_merged: A concatenated stacked pandas df with MTB score

    -----------------------------------------------------------------------------------------
    """

    groups = df_metadata[['dataGroups','healthcode', 'recordid']]
    groups = groups.drop_duplicates()
    
    df_combine = pd.concat(score_list).reset_index(drop=True)
    df_combine = ut.filter_study(df_combine)
    
    #Merging datagroups using healthcode & recordId
    scores_merged = df_combine.merge(groups, on=['healthcode', 'recordid'], how='left')
    scores_merged[['participant_id', 'sessionguid']] = scores_merged[['participant_id', 'sessionguid']].astype('str')
    return scores_merged
