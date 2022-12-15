# MTB Scoring

# import the required packages
import os
import json
import pandas as pd

def get_config():
    """
    -----------------------------------------------------------------------------------------
    Loading json config
    
    Returns:
        Config object
    -----------------------------------------------------------------------------------------
    """
    
    dir_name = os.path.dirname(os.path.abspath(__file__))
    measure_path = os.path.abspath(os.path.join(dir_name, '../config/config.json'))
    
    file = open(measure_path)
    return json.load(file)

def stack_score(df_combine):
    """
    -----------------------------------------------------------------------------------------

    Get stacked scores type and values

    Args:
         df_combine: Concatanated dataframe for all the events

     Returns:
        df: stacked dataframe with score type and its valuese

    -----------------------------------------------------------------------------------------
    """
    df = df_combine.copy()
    split_score = pd.DataFrame(pd.DataFrame(df['score_dict'].values.tolist()).stack(dropna=False).reset_index(level=1))
    split_score.columns = ['scoreId', 'score']
    
    df.drop(columns='score_dict', inplace=True)
    df = df.join(split_score).reset_index(drop=True)
    return df

def filter_study(df):
    """
    -----------------------------------------------------------------------------------------

    Filter participant info (study-membership)

    Args:
        df: Concatanated pandas dataframe for all the events

    Returns:
        df: pandas dataframe with study reference and ID

    -----------------------------------------------------------------------------------------
    """
    start, end, start2, end2, start3, end3 = '|', '=', '=', ':', ':', '|'

    df['study_reference']= df['studyMemberships'].map(lambda x: x[x.find(start) + len(start):x.rfind(end)])
    df['participant_id']= df['studyMemberships'].map(lambda x: x[x.find(start2) + len(start2):x.rfind(end2)])
    df = df.drop(columns=['studyMemberships'])
    return df
