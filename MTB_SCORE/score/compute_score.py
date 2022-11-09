# MTB Scoring

# import the required packages
import pandas as pd
from MTB_SCORE.util import util as ut
from MTB_SCORE.util.get_Stepdata import Get_Stepdata
from MTB_SCORE.util.get_score_item import Get_score_items
from MTB_SCORE.util.get_Scoresums import Get_Scoresum
from MTB_SCORE.score.get_scores import Get_Score
from MTB_SCORE.score.get_spellvocab_score import GetScore_SpellingAndVocab


def get_drop_dict(t_type):
    config = ut.get_config()
    
    COL_DROP = {config['vocab_type']: [config['vocab_THETA'], config['vocab_SE']], 
                config['spell_type']: [config['spell_THETA'], config['spell_SE']], 
                config['PSM_type']: [config['psm_adj'], config['psm_exact']], config['MFS_type']: [config['mfs_sum']],
                config['DCCS_type']: [config['dccs_time'], config['dccs_score'], config['DCCS_accuracy']],
                config['fname_type']: [config['FNAME_theta'], config['FNAME_sd']], config['NM_type']: [config['numberMatch_raw']],
                config['Flanker_type']: [config['flkr_time'], config['flkr_score'], config['FLANKER_accuracy']]
               }
    
    drop_col = COL_DROP[t_type]
    return config, drop_col

def score_spell_vocab_psm(task_data, dfid, t_type, study_membership):
    """
    -----------------------------------------------------------------------------------------
    
    Spell/Vocab/PSM score computation
    
    Args:
        task_data: taskdata downloaded from Synapse
        dfid: metadata downloaded from Synapse
        t_type: Task name
        study_membership: study membership info
        
    Return:
        Spell/Vocab/PSM score dataframe
        
    -----------------------------------------------------------------------------------------
    """
    config, drop_col = get_drop_dict(t_type)
    score = GetScore_SpellingAndVocab(task_data, dfid, t_type)
    score = pd.merge(score, study_membership, how = 'inner', left_on = config['healthcode'], right_on = config['healthCode'])
    
    score = score.drop(columns=[config['healthCode']])
    score['score_dict'] = score[drop_col].to_dict(orient='records')
    
    score = score.drop(columns=drop_col)
    return score
    
def score_event_common(task_data, stepdata, dfid, t_type, study_membership):
    """
    -----------------------------------------------------------------------------------------
    
    DCCS/Flanker/MFS/Fname/Number Match score computation
    
    Args:
        task_data: taskdata downloaded from Synapse
        stepdata: stepdata downloaded from Synapse
        dfid: metadata downloaded from Synapse
        t_type: Task name
        study_membership: study membership info
        
    Return:
        DCCS/Flanker/MFS/Fname/Number Match score dataframe
        
    -----------------------------------------------------------------------------------------
    """
    config, drop_col = get_drop_dict(t_type)
    
    step = Get_Stepdata(stepdata, t_type)
    scoreitem = Get_score_items(t_type)
    scoresum = Get_Scoresum(step, scoreitem, t_type)
    
    score = Get_Score(scoresum, task_data, dfid, t_type)
    score = pd.merge(score, study_membership, how='inner', left_on = config['healthcode'], right_on = config['healthCode'])
    score = score.drop(columns=[config['healthCode']])
    score['score_dict'] = score[drop_col].to_dict(orient='records')
    
    score = score.drop(columns=drop_col)
    return score
