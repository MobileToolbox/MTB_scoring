# MTB Scoring

# import the required packages
import pandas as pd
from MTB_SCORE.util import util as ut

def GetScore_SpellingAndVocab(task_data, dfid, type):
    """
    -----------------------------------------------------------------------------------------
    
    This is the function to get score 
    
    Args: 
        task_data: taskdata downloaded from Synapse
        dfid: metadata downloaded from Synapse
        type: Task/Score, in this function, there are only three choices: Spelling, Vocab and PSM

    Return:
        Scoring dataframe
    
    -----------------------------------------------------------------------------------------
    """
    config = ut.get_config()
    common_cols = [config['healthcode'], config['recordid'], config['assessmentid'], config['deviceInfo'], config['startDate'], 
                   config['endDate'], config['participantversion'], config['clientinfo'], config['sessionguid'], 
                   config['taskStatus_val']]
    
    if type == config['spell_type']:
        task_filter = task_data[task_data[config['assessmentid']] == config['spell_asses_val']].reset_index()
        
        task_filter[config['spell_THETA']] = task_filter[config['scores_finalTheta_double']]
        task_filter[config['spell_SE']] = task_filter[config['scores_finalSE_double']]
        
        task_filter = task_filter[[config['recordid'], config['steps'], config['spell_THETA'], config['spell_SE']]]
        merge_df = task_filter.merge(dfid, on = config['recordid'], how = 'left')
        
        spelling_score = merge_df[common_cols + [config['spell_THETA'], config['spell_SE']]]
        return spelling_score
    
    elif type == config['vocab_type']: 
        task_filter = task_data[task_data[config['assessmentid']] == config['vocab_asses_val']].reset_index()
        
        task_filter[config['vocab_THETA']] = task_filter[config['scores_finalTheta_double']]
        task_filter[config['vocab_SE']] = task_filter[config['scores_finalSE_double']]
        
        task_filter = task_filter[[config['recordid'], config['steps'], config['vocab_THETA'], config['vocab_SE']]]
        merge_df = task_filter.merge(dfid, on = config['recordid'], how = 'left')
        
        vocab_score = merge_df[common_cols + [config['vocab_THETA'], config['vocab_SE']]]
        return vocab_score
    
    elif type == config['PSM_type']:
        task_filter = task_data[task_data[config['assessmentid']] == config['psm_asses_val']] 
        task_scrd = task_filter.loc[((~task_filter[config['scores_adjacentP1']].isna()) &
                                     (~task_filter[config['scores_adjacentP2']].isna()))]
        
        task_scrd[config['psm_adj']] = task_scrd.loc[:, [config['scores_adjacentP1'], config['scores_adjacentP2']]].sum(axis=1)
        task_scrd[config['psm_exact']] = task_scrd.loc[:, [config['scores_exactM1'], config['scores_exactM2']]].sum(axis=1)

        task_scrd = task_scrd[[config['recordid'], config['steps'], config['psm_adj'], config['psm_exact']]]
        merge_df = task_scrd.merge(dfid, on = config['recordid'], how = 'left')
        
        psm_score = merge_df[common_cols + [config['psm_adj'], config['psm_exact']]]
        return psm_score
    
    else:
        raise Exception("There is no score function named this, please check the score name ")
        
    return 
