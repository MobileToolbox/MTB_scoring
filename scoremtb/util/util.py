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

    Get stacked scoreId and values

    Args:
         df_combine: Concatanated dataframe for all events

     Returns:
        df: stacked dataframe with scoreID and its valuese

    -----------------------------------------------------------------------------------------
    """
    df = df_combine.copy()
    split_score = pd.DataFrame(pd.DataFrame(df['score_dict'].values.tolist()).stack(dropna=False).reset_index(level=1))
    split_score.columns = ['scoreId', 'score']
    
    df.drop(columns='score_dict', inplace=True)
    df = df.join(split_score).reset_index(drop=True)
    return df