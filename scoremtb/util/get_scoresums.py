# MTB Scoring

# import the required packages
from scoremtb.util import util as ut
import numpy as np

def get_sums(data, s_items, type):
    """
    -----------------------------------------------------------------------------------------
    
    This is the function to get scoresum 

    Args: 
        data: filtered step data dataframe
        s_items: score item list
        type: Task/Score
    
    Return:
        Score dataframe
    
    -----------------------------------------------------------------------------------------
    """
    config = ut.get_config()
    
    if type == config['MFS_type']:
        data[config['mfs_sum']]=data.loc[:,s_items].sum(axis=1)
        return data
    
    elif type == config["NM_type"]:
        data[config['numberMatch_raw']]=data.loc[:,s_items].sum(axis=1)
        return data
    
    elif type == config["fname_type"]:
        data[config['FNAME_sum']]=data.loc[:,s_items].sum(axis=1)
        return data
    
    elif type == config['DCCS_type']:                              
        data[0][config['dccs_time']]=data[0].loc[:,s_items].sum(axis=1)/1000

        data[1][config['dccs_score']]=data[1].loc[:,s_items].sum(axis=1)
        dccs_time=data[0][['id', config['dccs_time']]]
        dccs_score=data[1][['id', config['dccs_score']]]

        data = dccs_time.merge(dccs_score, on = 'id')
        data[config['DCCS_accuracy']] = data[config['dccs_score']]/data[config['dccs_time']]
        return data
    
    elif type == config['Flanker_type']:                                
        data[0][config['flkr_time']] = data[0].loc[:,s_items].sum(axis=1)/1000

        data[1][config['flkr_score']] = data[1].loc[:,s_items].sum(axis=1)
        flkr_time = data[0][['id', config['flkr_time']]]
        flkr_score = data[1][['id', config['flkr_score']]]

        data = flkr_time.merge(flkr_score, on = 'id')
        data[config['FLANKER_accuracy']] = data[config['flkr_score']]/data[config['flkr_time']]
        return data
    
    elif type == config['PSM_type'] or type == config['spell_type'] or type == config['vocab_type']:
        raise Exception("Please use another function called 'GetScore_SpellingAndVocab' to get the step data for your data ")
    
    else:
        raise Exception("There is no score function named this, please check the score name ")
        
    return 