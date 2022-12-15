# MTB Scoring

# import the required packages
import pandas as pd
from scoremtb.util import util as ut
from scoremtb.score import score_commons as sc

def get_score(task_data, stepdata, dfid, assmnt_val, study_membership):
    """
    -----------------------------------------------------------------------------------------
    
    Compute score for a given assesment (like: PSM/Vocab/Speell/DCCS etc.)
    
    Args:
        task_data: taskdata pandas dataframe downloaded from Synapse
        stepdata: stepdata pandas dataframe downloaded from Synapse
        dfid: metadata pandas dataframe downloaded from Synapse
        assmnt_val: Assesment value
        study_membership: study membership info
        
    Return:
        A pandas dataframe with stacked MTB scores for a given assesment
        
    -----------------------------------------------------------------------------------------
    """
    config = ut.get_config()
    assmnt_val = assmnt_val.lower()
    
    if assmnt_val == 'psm' or assmnt_val == 'vocabulary' or assmnt_val == 'spelling':
        score_df = sc.get_svp_score(task_data, dfid, assmnt_val, config)
    
    else:
        score_df = sc.get_common_score(stepdata, task_data, dfid, assmnt_val, config)
        
    score_df = pd.merge(score_df, study_membership, how='inner', on = 'healthcode')
    score_df['score_dict'] = score_df[config[assmnt_val + '_score']].to_dict(orient='records')
    score_df = score_df.drop(columns=config[assmnt_val + '_score'])
    
    stacked_score = ut.stack_score(score_df)
    return stacked_score

def combine_scores(score_list, dfid):
    """
    -----------------------------------------------------------------------------------------

    Concatenating assesment scores

    Args: 
        score_list: list of score for each assesment
        dfid: metadata pandas dataframe downloaded from Synapse

    Returns:
       scores_merged: A concatenated stacked pandas df with MTB score

    -----------------------------------------------------------------------------------------
    """

    groups = dfid[['dataGroups','healthcode', 'recordid']]
    groups = groups.drop_duplicates()
    
    df_combine = pd.concat(score_list).reset_index(drop=True)
    df_combine = ut.filter_study(df_combine)
    
    #Merging datagroups using healthcode & recordId
    scores_merged = df_combine.merge(groups, on=['healthcode', 'recordid'], how='left')
    scores_merged[['participant_id', 'sessionguid']] = scores_merged[['participant_id', 'sessionguid']].astype('str')
    return scores_merged
