# MTB Scoring

# import the required packages
from MTB_SCORE.util import util as ut

def Get_Scoresum(data, s_items, type):
    """
    -----------------------------------------------------------------------------------------
    
    This is the function to get scoresum 

    Args: 
        data: data output from Get_Stepdata function 
        s_items: the list output from Get_Score_items function
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
        dccs_time=data[0][[config['id'], config['dccs_time']]]
        dccs_score=data[1][[config['id'], config['dccs_score']]]

        data = dccs_time.merge(dccs_score, on = config['id'])
        data[config['DCCS_accuracy']] = data[config['dccs_score']]/data[config['dccs_time']]
        return data
    
    elif type == config['Flanker_type']:                                
        data[0][config['flkr_time']] = data[0].loc[:,s_items].sum(axis=1)/1000

        data[1][config['flkr_score']] = data[1].loc[:,s_items].sum(axis=1)
        flkr_time = data[0][[config['id'], config['flkr_time']]]
        flkr_score = data[1][[config['id'], config['flkr_score']]]

        data = flkr_time.merge(flkr_score, on = config['id'])
        data[config['FLANKER_accuracy']] = data[config['flkr_score']]/data[config['flkr_time']]
        return data
    
    elif type == config['PSM_type'] or type == config['spell_type'] or type == config['vocab_type']:
        raise Exception("Please use another function called 'GetScore_SpellingAndVocab' to get the step data for your data ")
    
    else:
        raise Exception("There is no score function named this, please check the score name ")
        
    return 