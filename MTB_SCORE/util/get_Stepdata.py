# MTB Scoring

# import the required packages
from MTB_SCORE.util import util as ut

def Get_Stepdata(step_data,type):
    """
    -----------------------------------------------------------------------------------------
    
    Getting step data details
    
    Args: 
        step_data: stepData collected from Synapse
        type: Task/Score

    Return:
        A dataframe contains id, identifier and score 
    
    -----------------------------------------------------------------------------------------
    """
    config = ut.get_config()
    
    if type == config['MFS_type']:
        step_filter = step_data[step_data[config['assessmentid']] == config['mfs_asses_val']]
        step_filter = step_filter[[config['id'], config['identifier'], config['score']]]
        
        mfs_info = step_filter.pivot_table(values = config['score'], index= config['id'], 
                                       columns= config['identifier']).reset_index(config['id']) 
        return mfs_info
    
    elif type == config["NM_type"]:
        step_filter=step_data[step_data[config['assessmentid']] == config['nm_asses_val']]
        
        step_filter = step_filter[[config['id'], config['identifier'], config['score']]]
        nm_info=step_filter.pivot_table(values = config['score'], index = config['id'], 
                                        columns = config['identifier']).reset_index(config['id']) 
        return nm_info
    
    elif type == config["fname_type"]:
        step_filter=step_data[step_data[config['assessmentid']] == config['fname_asses_val']]
        
        step_filter = step_filter[[config['id'], config['identifier'], config['score']]]
        fname_info = step_filter.pivot_table(values = config['score'], index = config['id'],
                                             columns = config['identifier']).reset_index(config['id']) 
        return fname_info
    
    elif type == config['DCCS_type']:                                 
        step_filter = step_data[step_data[config['assessmentid']] == config['dccs_asses_val']]
        
        dccst1 = step_filter[[config['id'], config['identifier'], config['responseTime']]]
        dccst2 = dccst1.pivot_table(values = config['responseTime'], index = config['id'], 
                                  columns = config['identifier']).reset_index(config['id']) 
        dccss1 = step_filter[[config['id'], config['identifier'], config['score']]]
        
        dccss2 = dccss1.pivot_table(values = config['score'], index=config['id'],
                                  columns = config['identifier']).reset_index(config['id']) 
        return (dccst2, dccss2)
    
    elif type == config['Flanker_type']:                               
        step_filter = step_data[step_data[config['assessmentid']] == config['flnkr_asses_val']]
        
        flkrt1 = step_filter[[config['id'], config['identifier'], config['responseTime']]]
        flkrt2 = flkrt1.pivot_table(values='responseTime', index='id', columns='identifier').reset_index('id') 
        flkrs1 = step_filter[[config['id'], config['identifier'], config['score']]]
        
        flkrs2=flkrs1.pivot_table(values = config['score'], index = config['id'], 
                                  columns = config['identifier']).reset_index(config['id']) 
        return (flkrt2, flkrs2)
    
    elif type == config['PSM_type'] or type == config['spell_type'] or type == config['vocab_type']:
        raise Exception("Please use another function called 'GetScore_SpellingAndVocab' to get the step data for your data ")
    
    else:
        raise Exception("There is no score function named this , please check the score name ")
        
    return 

