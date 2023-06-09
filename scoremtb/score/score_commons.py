# MTB Scoring

# import the required packages
import pandas as pd
from scoremtb.util import score_util as su

def compute_scores_from_stepdata(stepdata, task_data, df_metadata, assessmentId, config):
    """Process score for common assesments (e.g. DCCS, Flanker, MFS, Name Match) by extracting 
    step level score information and combining into a common score.


    Args: 
        stepdata: stepdata pandas dataframe downloaded from Synapse
        task_data: taskdata pandas dataframe downloaded from Synapse
        df_metadata: metadata pandas dataframe downloaded from Synapse
        assessmentId: assesment value 
        config: configuration file object
        
    Return:
        A pandas dataframe with MTB scores for a given common assesment
    """
    step = su.get_identifierinfo(stepdata, assessmentId)
    
    scoreitem =  config[assessmentId + '_item']
    scoresum = su.process_score(step, assessmentId, scoreitem, config)
    
    score_df = evaluate_common_score(scoresum, task_data, df_metadata, assessmentId, config)
    return score_df
    
def evaluate_common_score(data, task_data, df_metadata, assessmentId, config):
    """ Preparing scores for common assesments

    Args: 
        data: pandas dataframe with assesment specific processed score
        task_data: taskdata pandas dataframe downloaded from Synapse
        df_metadata: metadata pandas dataframe downloaded from Synapse
        assessmentId: assesment value (like: dccs, flanker etc.)
        config: configuration file object
    
    Return:
        A pandas dataframe with MTB scores for a given assesment
    
    """
    common_cols = config['common_cols']
    
    if assessmentId=='fnameb':
        score_df = get_fname_score(data, task_data, df_metadata, assessmentId, config)
        return score_df
    
    task_filter = task_data[task_data['assessmentid'] == assessmentId]
    merge_df = data.merge(task_filter, left_on = 'id', right_on = 'steps')
    
    filter_df = merge_df[['recordid'] + config[assessmentId + '_score']]
    merge_meta_df = filter_df.merge(df_metadata, on= 'recordid', how='left')
    
    score_df = merge_meta_df[common_cols + config[assessmentId + '_score']]
    return score_df

def get_fname_score(data, task_data, df_metadata, assessmentId, config):
    """Prepares scores for fnameb assesments

    Args: 
        data: pandas dataframe with processed score for Fnameb assesment
        task_data: taskdata pandas dataframe downloaded from Synapse
        df_metadata: taskdata pandas dataframe downloaded from Synapse
        assessmentId: assesment value (fnameb)
        config: configuration file object
    
    Return:
        A pandas dataframe with MTB scores for fname assesment
    """
    
    common_cols = config['common_cols']
    lookup_df = get_lookup(config)
    
    task_filter = task_data[task_data['assessmentid'] == assessmentId]
    merge_df = data.merge(task_filter, left_on = 'id', right_on = 'steps')
    
    filter_df = merge_df[['recordid'] + [config['fnameb_sum']]]
    merge_meta = filter_df.merge(df_metadata, on= 'recordid', how='left')
    merge_lookup = merge_meta.merge(lookup_df, left_on = config['fnameb_sum'], right_on='rawFname', how='left')
    
    merge_lookup.rename(columns = {'theta': config['fnameb_score'][0], 'sd': config['fnameb_score'][1]}, inplace = True)
    score_df = merge_lookup[common_cols + config['fnameb_score']]
    return score_df

def get_lookup(config):
    """ Preparing default lookup matrix for fnameb assesments

    Args: 
        config: configuration file object
    
    Return:
        A default lookup scoring pandas dataframe 
    """
    rawFname = [i for i in range(37)]
    theta = config['theta']
    sd = config['sd']
    
    lookup_df = pd.DataFrame(columns = ['rawFname', 'theta', 'sd'])
    lookup_df['rawFname'] = rawFname
    
    lookup_df['theta'] = theta
    lookup_df['sd'] = sd
    return lookup_df

def get_computed_score(task_data, df_metadata, assessmentId, config, study_membership):
    """Extrac pre-computed scores from the taskdata dataset

    Args: 
        task_data: taskdata pandas dataframe downloaded from Synapse
        df_metadata: metadata pandas dataframe downloaded from Synapse
        assessmentId: assesment value (like: spelling, vocabulary  etc.)
        config: configuration file object
        study_membership: study membership/participant info
    
    Return:
        A pandas dataframe with MTB scores for a given assesment
    """
    common_cols = config['common_cols'] #Columns to use out of the taskdata dataframe
    score_cols = config['scores_final_cols']
    
    #Filter down the taskdata to specific assessment
    task_filter = task_data[task_data['assessmentid'] == assessmentId].reset_index()
    #Renames score columns TODO (@larssono June-2023) evaluate if this is necessary and remove if not.
    if assessmentId == 'psm':
        task_filter = filter_psm(assessmentId, config, task_filter) 
    elif assessmentId in ['LetterNumberSeriesV1', 'VerbalReasoningV1']:#TODO (@larssono June-2023) remove specific use case.  
        task_filter[config[assessmentId + '_score']] = task_filter[['scores_rawScore']]
    elif assessmentId in ['3DRotationV1', 'ProgressiveMatricesV1']:  
        task_filter[config[assessmentId + '_score']] = task_filter[['scores_startTheta', 'scores_startSE', 'scores_finalTheta', 'scores_finalSE', 'scores_itemCount']]
    else:
        task_filter[config[assessmentId + '_score']] = task_filter[score_cols] 

    task_filter = task_filter[['recordid', 'steps'] + config[assessmentId + '_score']]
    merge_df = task_filter.merge(df_metadata, on = 'recordid', how = 'left')
    score_df = merge_df[common_cols + config[assessmentId + '_score']]
    return score_df


def filter_psm(assessmentId, config, task_filter):
    """This is the function to process score for PSM assesment

    Args: 
        assessmentId: assesment value (PSM)
        config: configuration file object
        task_filter: PSM assesment filtered taskdata pandas dataframe
        
    Return:
        A pandas dataframe with processed MTB score for PSM assesment
    """
    psm_filter = task_filter.loc[((~task_filter[config[assessmentId + '_adjacentp'][0]].isna()) &
                                 (~task_filter[config[assessmentId + '_adjacentp'][1]].isna()))]
        
    psm_filter[config[assessmentId + '_score'][0]] = psm_filter.loc[:, config[assessmentId + '_adjacentp']].sum(axis=1)
    psm_filter[config[assessmentId + '_score'][1]] = psm_filter.loc[:, config[assessmentId + '_exactm']].sum(axis=1)
    return psm_filter

