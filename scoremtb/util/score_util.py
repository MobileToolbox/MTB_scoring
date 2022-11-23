# MTB Scoring

# import the required packages
import pandas as pd

def get_identifierinfo(step_data, assmnt_val):
    """
    -----------------------------------------------------------------------------------------
    
    Getting identifier (event identifier) info using step data
    
    Args: 
        step_data: stepdata pandas dataframe downloaded from Synapse
        assmnt_val: assesment value (like: dccs, flanker etc.)

    Return:
        A pandas dataframe with identifier specific to given assesment.
    
    -----------------------------------------------------------------------------------------
    """
    if assmnt_val == 'dccs' or assmnt_val == 'flanker':
        step_info_response = get_stepdata(step_data, assmnt_val, 'responseTime')
        
        step_info_score = get_stepdata(step_data, assmnt_val, 'score')
        step_info = (step_info_response, step_info_score)
    
    else:
        step_info = get_stepdata(step_data, assmnt_val, 'score')
        
    return step_info

def get_stepdata(step_data, assmnt_val, score):
    """
    -----------------------------------------------------------------------------------------
    
    Filtering step data to get identifier info
    
    Args: 
        step_data: stepdata pandas dataframe downloaded from Synapse
        assmnt_val: assesment value (like: dccs, flanker etc.)
        score: scoring column name (score/responseTime)

    Return:
        A pandas dataframe with filtered identifier info.
    
    -----------------------------------------------------------------------------------------
    """
    step_filter = step_data[step_data['assessmentid'] == assmnt_val]
    step_filter = step_filter[['id', 'identifier', score]]
    
    step_info = step_filter.pivot_table(values=score, index='id', columns='identifier').reset_index('id') 
    return step_info

def get_scoreitems(assmnt_val, config):
    """
    -----------------------------------------------------------------------------------------
    
    This is a function to get the attributes/identifiers from config.
    
    Args:
        assmnt_val: assesment value (like: dccs, flanker etc.)
        config: configuration file object
        
    Return:
        A list of assesment attributes/identifiers
    
    -----------------------------------------------------------------------------------------
    """
    return config[assmnt_val + '_item']

def process_score(data, assmnt_val, s_items, config):
    """
    -----------------------------------------------------------------------------------------
    
    This is the function to process score 

    Args: 
        data: filtered step data pandas dataframe
        assmnt_val: assesment value (like: dccs, flanker etc.)
        s_items: identifier item list
        config: configuration file object
    
    Return:
        A pandas dataframe with processed MTB score
    
    -----------------------------------------------------------------------------------------
    """
    if assmnt_val == 'dccs' or assmnt_val == 'flanker':
        data[0][config[assmnt_val + '_score'][0]] = data[0].loc[:,s_items].sum(axis=1)/1000

        data[1][config[assmnt_val + '_score'][1]] = data[1].loc[:,s_items].sum(axis=1)
        dccs_time=data[0][['id', config[assmnt_val + '_score'][0]]]
        dccs_score=data[1][['id', config[assmnt_val + '_score'][1]]]

        data = dccs_time.merge(dccs_score, on = 'id')
        data[config[assmnt_val + '_score'][2]] = data[config[assmnt_val + '_score'][1]]/data[config[assmnt_val + '_score'][0]]
    
    elif assmnt_val == 'fnameb':
        data[config[assmnt_val + '_sum']]=data.loc[:,s_items].sum(axis=1)
    
    else:
        data[config[assmnt_val + '_score'][0]]=data.loc[:,s_items].sum(axis=1)
    
    return data
