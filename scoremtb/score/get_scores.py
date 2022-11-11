# MTB Scoring

# import the required packages
import pandas as pd
from scoremtb.util import util as ut

def get_score(data, task_data, dfid, type):
    """
    -----------------------------------------------------------------------------------------
    
    This is the function to get score 

    Args: 
        data: score sums dataframe
        task_data: taskdata downloaded from Synapse
        dfid: metadata downloaded from Synapse
        type: Task/Score
    
    Return:
        Scoring dataframe
    
    -----------------------------------------------------------------------------------------
    """
    
    config = ut.get_config()
    common_cols = ['healthcode', 'recordid', 'assessmentid', 'deviceInfo', 'startDate', 'endDate', 
                   'participantversion', 'clientinfo', 'sessionguid', 'taskStatus_val']
    
    if type == config['DCCS_type']:
        task_filter = task_data[task_data['assessmentid'] == config['dccs_asses_val']]
        
        data_scored = data.merge(task_filter, left_on = 'id', right_on = 'steps')
        data_score_filter = data_scored[['recordid', config['dccs_time'], config['dccs_score'], config['DCCS_accuracy']]]
        data_score_filter_meta = data_score_filter.merge(dfid, on= 'recordid', how='left')
        
        data_score = data_score_filter_meta[common_cols + [config['dccs_time'], config['dccs_score'], config['DCCS_accuracy']]]
        return data_score
    
    elif type == config['Flanker_type']:
        task_filter = task_data[task_data['assessmentid'] == config['flnkr_asses_val']]  
        
        data_scored = data.merge(task_filter, left_on = 'id', right_on = 'steps')
        data_score_filter = data_scored[['recordid',config['flkr_time'],config['flkr_score'],config['FLANKER_accuracy']]]
        data_score_filter_meta = data_score_filter.merge(dfid, on = 'recordid', how='left')

        data_score=data_score_filter_meta[common_cols + [config['flkr_time'], config['flkr_score'], config['FLANKER_accuracy']]]
        return data_score
    
    elif type == config['MFS_type']:
        task_filter = task_data[task_data['assessmentid'] == config['mfs_asses_val']]   
        
        data_scored=data.merge(task_filter, left_on = 'id', right_on = 'steps')
        data_score_filter = data_scored[['recordid', config['mfs_sum']]]
        data_score_filter_meta = data_score_filter.merge(dfid, on = 'recordid', how='left')

        data_score = data_score_filter_meta[common_cols + [config['mfs_sum']]]
        return data_score
    
    elif type == config['NM_type']: 
        task_filter = task_data[task_data['assessmentid'] == config['nm_asses_val']] 
        
        data_scored = data.merge(task_filter, left_on = 'id', right_on = 'steps')
        data_score_filter = data_scored[['recordid', config['numberMatch_raw']]]
        data_score_filter_meta = data_score_filter.merge(dfid, on = 'recordid', how='left')
        
        data_score=data_score_filter_meta[common_cols + [config['numberMatch_raw']]]
        return data_score
    
    if type == config['fname_type']:
        rawFname = [i for i in range(37)]
        
        theta = config['theta']
        sd = config['sd']
        
        fname_lookup = pd.DataFrame(columns = ['rawFname','theta','sd'])
        fname_lookup['rawFname'] = rawFname
        fname_lookup['theta'] = theta
        fname_lookup['sd'] = sd
        
        task_filter = task_data[task_data['assessmentid'] == config['fname_asses_val']]    
        data_scored = data.merge(task_filter, left_on = 'id', right_on = 'steps')
        data_score_filter = data_scored[['recordid', config['FNAME_sum']]]

        data_score_filter_meta = data_score_filter.merge(dfid, on= 'recordid', how = 'left')
        data_score_filter_meta2 = data_score_filter_meta.merge(fname_lookup,left_on = config['FNAME_sum'], 
                                                               right_on='rawFname', how='left')
        data_score_filter_meta2.rename(columns = {'theta': config['FNAME_theta'], 'sd': config['FNAME_sd']}, inplace = True)

        data_score=data_score_filter_meta2[common_cols + [config['FNAME_theta'], config['FNAME_sd']]]
        return data_score
    
    elif type == config['PSM_type'] or type == config['spell_type'] or type == config['vocab_type']:
        raise Exception("Please use another function called  to get the step data for your data ")
    
    else:
        raise Exception("There is no score function named  , please check the score name ")
        
    return 