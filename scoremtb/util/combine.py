# MTB Scoring

# import the required packages
import pandas as pd
import numpy as np
from scoremtb.util import util as ut

def filter_study(df):
    """
    -----------------------------------------------------------------------------------------

    Filter participant info (study-membership)

    Args:
        df: Concatanated dataframe for all events

    Returns:
        df: dataframe with study reference and ID

    -----------------------------------------------------------------------------------------
    """
    start, end, start2, end2, start3, end3 = '|', '=', '=', ':', ':', '|'

    df['study_reference']= df['studyMemberships'].map(lambda x: x[x.find(start) + len(start):x.rfind(end)])
    df['study_id']= df['studyMemberships'].map(lambda x: x[x.find(start2) + len(start2):x.rfind(end2)])
    df = df.drop(columns=['studyMemberships'])
    return df

def combine_scores(score_mfs, score_dccs, score_fname, score_nm, score_flanker, psm_score, spelling_score, 
    vocab_score, dfid):
    """
    -----------------------------------------------------------------------------------------

    Concatenating task scores

    Args: 
       score_mfs: MFS Score; score_dccs: DCCS score; score_fname: Fname score
       score_nm: Number match score, score_flanker: Flanker score,
       psm_score: PSM score; spelling_score: Spelling ; vocab_score: vocab scod
       dfid: metadata downloaded from Synapse

    Returns:
       stack_merged: A stacked dataframe with MTB score

    -----------------------------------------------------------------------------------------
    """

    groups = dfid[['dataGroups','healthcode', 'recordid']]
    groups = groups.drop_duplicates()
    
    df_combine = pd.concat([score_mfs,score_dccs,score_fname,score_nm,score_flanker,psm_score,spelling_score,
                            vocab_score]).reset_index(drop=True)
    df_combine = filter_study(df_combine)
    
    #Merging datagroups using healthcode & recordId
    scores_merged = df_combine.merge(groups, on=['healthcode', 'recordid'], how='left')
    return scores_merged
