
"""
This is the function to combine all the scores

input: 
       table_substudymembership - table_substudymembershipdataset you downloaded from Synaps
       (..)_score - the score you get from previous functions
output:
       A dataframe contains studyMemberships, healthcode and scores
"""
import pandas as pd

def stack_score(df_combine):
    df = df_combine.copy()
    split_score = pd.DataFrame(pd.DataFrame(df['score_dict'].values.tolist()).stack().reset_index(level=1))
    split_score.columns = ['scoreId','score']
    
    df.drop(columns='score_dict',inplace=True)
    df = df.join(split_score).reset_index(drop=True)
    return df

def filter_study(df):
    start, end, start2, end2 = '|', '=', '=', '|'

    df['study']= df['studyMemberships'].map(lambda x: x[x.find(start) + len(start):x.rfind(end)])
    df['id']= df['studyMemberships'].map(lambda x: x[x.find(start2) + len(start2):x.rfind(end2)])
    return df


def Combine(score_mfs,score_dccs,score_fname,score_nm,score_flanker,PSM_score,spelling_score,vocab_score,dfid):
    groups=dfid[['dataGroups','healthcode', 'recordid']]
    groups=groups.drop_duplicates()
    
    df_combine=pd.concat([score_mfs,score_dccs,score_fname,score_nm,score_flanker,PSM_score,
                          spelling_score,vocab_score]).reset_index(drop=True)
    df_combine = filter_study(df_combine)
    
    #Merging datagroups using healthcode & recordId
    scores_merged = df_combine.merge(groups, on=['healthcode', 'recordid'], how='left')
    stack_merged = stack_score(scores_merged)
    return stack_merged, scores_merged
