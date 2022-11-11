# MTB Scoring

# import the required packages
import pandas as pd
from scoremtb.util import util as ut
from scoremtb.util.get_stepdata import get_stepinfo
from scoremtb.util.get_scoreitems import get_items
from scoremtb.util.get_scoresums import get_sums
from scoremtb.score.get_scores import get_score
from scoremtb.score.get_spellvocab_score import get_score_spell

def get_drop_dict(t_type, config):
    """
    -----------------------------------------------------------------------------------------
    
    Get dictionary of drop column

    Args:
        t_type: Task name
        config: Config file object

    -----------------------------------------------------------------------------------------
    """
    COL_DROP = {config['vocab_type']: [config['vocab_THETA'], config['vocab_SE']], 
                config['spell_type']: [config['spell_THETA'], config['spell_SE']], 
                config['PSM_type']: [config['psm_adj'], config['psm_exact']], config['MFS_type']: [config['mfs_sum']],
                config['DCCS_type']: [config['dccs_time'], config['dccs_score'], config['DCCS_accuracy']],
                config['fname_type']: [config['FNAME_theta'], config['FNAME_sd']], config['NM_type']: [config['numberMatch_raw']],
                config['Flanker_type']: [config['flkr_time'], config['flkr_score'], config['FLANKER_accuracy']]
               }
    
    drop_col = COL_DROP[t_type]
    return drop_col

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
    config = ut.get_config()
    drop_col = get_drop_dict(t_type, config)
    
    score = get_score_spell(task_data, dfid, t_type)
    score = pd.merge(score, study_membership, how = 'inner', on = 'healthcode')
    
    score['score_dict'] = score[drop_col].to_dict(orient='records')
    score = score.drop(columns=drop_col)
    
    stacked_score = ut.stack_score(score)
    return stacked_score
    
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
    config = ut.get_config()
    drop_col = get_drop_dict(t_type, config)
    
    step = get_stepinfo(stepdata, t_type)
    scoreitem = get_items(t_type)
    scoresum = get_sums(step, scoreitem, t_type)
    
    score = get_score(scoresum, task_data, dfid, t_type)
    score = pd.merge(score, study_membership, how='inner', on = 'healthcode')
    
    score['score_dict'] = score[drop_col].to_dict(orient='records')
    score = score.drop(columns=drop_col)
    
    stacked_score = ut.stack_score(score)
    return stacked_score
