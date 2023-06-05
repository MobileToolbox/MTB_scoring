# MTB Scoring

# import the required packages
import pandas as pd
from scoremtb.util import score_util as su

def get_common_score(stepdata, task_data, df_metadata, assmnt_val, config):
    """Process score for common assesments 
        (like: DCCS, Flanker, MFS, Name Match etc.)

    Args: 
        stepdata: stepdata pandas dataframe downloaded from Synapse
        task_data: taskdata pandas dataframe downloaded from Synapse
        df_metadata: metadata pandas dataframe downloaded from Synapse
        assmnt_val: assesment value 
        config: configuration file object
        
    Return:
        A pandas dataframe with MTB scores for a given common assesment
    """
    step = su.get_identifierinfo(stepdata, assmnt_val)
    
    scoreitem = su.get_scoreitems(assmnt_val, config)
    scoresum = su.process_score(step, assmnt_val, scoreitem, config)
    
    score_df = evaluate_common_score(scoresum, task_data, df_metadata, assmnt_val, config)
    return score_df
    
def evaluate_common_score(data, task_data, df_metadata, assmnt_val, config):
    """ Preparing scores for common assesments

    Args: 
        data: pandas dataframe with assesment specific processed score
        task_data: taskdata pandas dataframe downloaded from Synapse
        df_metadata: metadata pandas dataframe downloaded from Synapse
        assmnt_val: assesment value (like: dccs, flanker etc.)
        config: configuration file object
    
    Return:
        A pandas dataframe with MTB scores for a given assesment
    
    """
    common_cols = config['common_cols']
    
    if assmnt_val=='fnameb':
        score_df = get_fname_score(data, task_data, df_metadata, assmnt_val, config)
        return score_df
    
    task_filter = task_data[task_data['assessmentid'] == assmnt_val]
    merge_df = data.merge(task_filter, left_on = 'id', right_on = 'steps')
    
    filter_df = merge_df[['recordid'] + config[assmnt_val + '_score']]
    merge_meta_df = filter_df.merge(df_metadata, on= 'recordid', how='left')
    
    score_df = merge_meta_df[common_cols + config[assmnt_val + '_score']]
    return score_df

def get_fname_score(data, task_data, df_metadata, assmnt_val, config):
    """Prepares scores for fnameb assesments

    Args: 
        data: pandas dataframe with processed score for Fnameb assesment
        task_data: taskdata pandas dataframe downloaded from Synapse
        df_metadata: taskdata pandas dataframe downloaded from Synapse
        assmnt_val: assesment value (fnameb)
        config: configuration file object
    
    Return:
        A pandas dataframe with MTB scores for fname assesment
    """
    
    common_cols = config['common_cols']
    lookup_df = get_lookup(config)
    
    task_filter = task_data[task_data['assessmentid'] == assmnt_val]
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

def get_svp_score(task_data, df_metadata, assmnt_val, config, study_membership):
    """This is the function to process score for common assesments 
        (like [SVP]: Spell, Vocab and PSM)

    Args: 
        task_data: taskdata pandas dataframe downloaded from Synapse
        df_metadata: metadata pandas dataframe downloaded from Synapse
        assmnt_val: assesment value (like: spelling, vocabulary  etc.)
        config: configuration file object
        study_membership: study membership/participant info
    
    Return:
        A pandas dataframe with MTB scores for a given assesment
    """
    common_cols = config['common_cols'] #Columns to use out of the taskdata dataframe
    
    #Filter down the taskdata to specific assessment
    task_filter = task_data[task_data['assessmentid'] == assmnt_val].reset_index()
    #Rename score columns
    task_filter = filter_assesment(assmnt_val, config, task_filter)
    
    task_filter = task_filter[['recordid', 'steps'] + config[assmnt_val + '_score']]
    merge_df = task_filter.merge(df_metadata, on = 'recordid', how = 'left')
    score_df = merge_df[common_cols + config[assmnt_val + '_score']]
    return score_df

#TODO move this up to get_svp_score
def filter_assesment(assmnt_val, config, task_filter):
    """This is the function to process score for common assesments 
        (like [SVP]: Spell, Vocab and PSM)

    Args: 
        assmnt_val: assesment value (like: spelling, vocabulary and PSM)
        config: configuration file object
        task_filter: assesment wise filtered taskdata pandas dataframe
        
    Return:
        A pandas dataframe with processed MTB score for a given assesment
    """
    score_cols = config['scores_final_cols']

    ## Fix issue where scores are sometimes stored under scores_finalTheta_double and scores_finalSE_double and
    ## other times: scores_finalTheta, scores_finalSE
    ## Likely related to: Glue Bug in BridgeDownstream:  https://sagebionetworks.jira.com/browse/ETL-238
    score_cols = [col if col in task_filter.columns else col+'_double'  for col in score_cols]
    
    if assmnt_val == 'psm':
        task_filter = filter_psm(assmnt_val, config, task_filter)
    elif assmnt_val in ['letternumberseries', 'verbalreasoning']:  #TODO remove specific use case.
        task_filter[config[assmnt_val + '_score']] = task_filter[['scores_rawScore']]
    else:
        #TODO why are we renaming the score columns.  If we remove this we can simplify the processing code signifantly 
        # and remove a bunch of values out of the config file.
        task_filter[config[assmnt_val + '_score']] = task_filter[score_cols]
    return task_filter


def filter_psm(assmnt_val, config, task_filter):
    """This is the function to process score for PSM assesment

    Args: 
        assmnt_val: assesment value (PSM)
        config: configuration file object
        task_filter: PSM assesment filtered taskdata pandas dataframe
        
    Return:
        A pandas dataframe with processed MTB score for PSM assesment
    """
    psm_filter = task_filter.loc[((~task_filter[config[assmnt_val + '_adjacentp'][0]].isna()) &
                                 (~task_filter[config[assmnt_val + '_adjacentp'][1]].isna()))]
        
    psm_filter[config[assmnt_val + '_score'][0]] = psm_filter.loc[:, config[assmnt_val + '_adjacentp']].sum(axis=1)
    psm_filter[config[assmnt_val + '_score'][1]] = psm_filter.loc[:, config[assmnt_val + '_exactm']].sum(axis=1)
    return psm_filter

