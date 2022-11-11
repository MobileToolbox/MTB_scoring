# MTB Scoring

# import the required packages
from scoremtb.util import util as ut

def get_stepinfo(step_data, type):
    """
    -----------------------------------------------------------------------------------------
    
    Getting step data info
    
    Args: 
        step_data: stepData collected from Synapse
        type: Task/Score

    Return:
        A dataframe contains id, identifier and score 
    
    -----------------------------------------------------------------------------------------
    """
    config = ut.get_config()
    
    if type == config['MFS_type']:
        step_filter = step_data[step_data['assessmentid'] == config['mfs_asses_val']]
        step_filter = step_filter[['id', 'identifier', 'score']]
        
        mfs_info = step_filter.pivot_table(values='score', index='id', columns='identifier').reset_index('id') 
        return mfs_info
    
    elif type == config["NM_type"]:
        step_filter=step_data[step_data['assessmentid'] == config['nm_asses_val']]
        
        step_filter = step_filter[['id', 'identifier', 'score']]
        nm_info=step_filter.pivot_table(values='score', index='id', columns='identifier').reset_index('id') 
        return nm_info
    
    elif type == config["fname_type"]:
        step_filter=step_data[step_data['assessmentid'] == config['fname_asses_val']]
        
        step_filter = step_filter[['id', 'identifier', 'score']]
        fname_info = step_filter.pivot_table(values='score', index='id', columns='identifier').reset_index('id') 
        return fname_info
    
    elif type == config['DCCS_type']:                                 
        step_filter = step_data[step_data['assessmentid'] == config['dccs_asses_val']]
        
        dccst1 = step_filter[['id', 'identifier', 'responseTime']]
        dccst2 = dccst1.pivot_table(values='responseTime', index='id', columns='identifier').reset_index('id')
        dccss1 = step_filter[['id', 'identifier', 'score']]
        
        dccss2 = dccss1.pivot_table(values='score', index='id', columns='identifier').reset_index('id')
        return (dccst2, dccss2)
    
    elif type == config['Flanker_type']:                               
        step_filter = step_data[step_data['assessmentid'] == config['flnkr_asses_val']]
        
        flkrt1 = step_filter[['id', 'identifier', 'responseTime']]
        flkrt2 = flkrt1.pivot_table(values='responseTime', index='id', columns='identifier').reset_index('id') 
        flkrs1 = step_filter[['id', 'identifier', 'score']]
        
        flkrs2=flkrs1.pivot_table(values='score', index='id', columns='identifier').reset_index('id') 
        return (flkrt2, flkrs2)
    
    elif type == config['PSM_type'] or type == config['spell_type'] or type == config['vocab_type']:
        raise Exception("Please use another function called 'GetScore_SpellingAndVocab' to get the step data for your data ")
    
    else:
        raise Exception("There is no score function named this , please check the score name ")
        
    return 

