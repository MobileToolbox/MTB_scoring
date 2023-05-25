import numpy as np
import pandas as pd

from scoremtb.score import get_syndata as gsyn
from scoremtb.util import util as ut


WRONG_TIME_MARK_FOR_IMPUTATION = -99999

def _load_interaction_data(syn, project_id, filters='dccs'):
    """ Loads interactions dataframe used to compute reaction times

    Args:
        syn: Synapse object
        project_id: parquet file synID
        filters: list of filters passed to load Parquet function

    return interactiondata data frame
    """
    df_interactions = gsyn.parquet_2_df(syn, project_id, 'dataset_sharedschema_v1_userInteractions/', filters)
    df_controlEvents = gsyn.parquet_2_df(syn, project_id, 'dataset_sharedschema_v1_userInteractions_controlEvent/', filters)

    interactionsData = (df_interactions
                        .merge(df_controlEvents[['id', 'recordid', 'userInteractions.val.controlEvent.val']],
                            left_on=['recordid', 'controlEvent'],
                            right_on=['recordid', 'id'],
                            suffixes=[None, '_controlEvent'],
                            how='left')
                        .drop('id_controlEvent', axis=1))  #Remove duplicated columns
    return interactionsData

def _computeDT(df, items=None):
    """Pull out the last 'uiEnabled' and the first 'touchDown' after this event then
       compute the time difference between these to events (this is assume the input
       data is for a specific user and stepidentifer).

       Args:
          df data frame of item level responses for one record and stepidentifier sorted by index

       returns same dataframe with additional imputed_rt_ms column
    """

    if df.iloc[0].stepIdentifier not in set(items): #Skip inputing response time for non-scored items
        return  df.assign(imputed_rt_ms = np.NaN)

    # Find last row where userInteractions is 'uiEnabled' for this stepIdentifier
    idx_uiEnabled = df[df["userInteractions.val.controlEvent.val"]=="uiEnabled"].index
    #If no uiEnabled not foudn in intarction event return WRONG_TIME_MARK_FOR_IMPUTATION to be replaced median time later in the processing
    if len(idx_uiEnabled)==0:
        return  df.assign(imputed_rt_ms = WRONG_TIME_MARK_FOR_IMPUTATION)
    idx_uiEnabled = idx_uiEnabled[-1]
    #If the touchDown occured before the uiEnabled event return WRONG_TIME_MARK_FOR_IMPUTATION to be replaced median time later in the processing
    if 'touchDown' in set(df.loc[:idx_uiEnabled,"userInteractions.val.controlEvent.val"]):
        return  df.assign(imputed_rt_ms = WRONG_TIME_MARK_FOR_IMPUTATION)

    # Return timeout responseTime of 2000 if no touchDown recorded after uiEnabled
    if 'touchDown' not in set(df.loc[idx_uiEnabled:,"userInteractions.val.controlEvent.val"]):
        return  df.assign(imputed_rt_ms = 2000)

    # Find the first row with 'touchDown', but after the last uiEnabled row
    idx_touchDown = (df.loc[idx_uiEnabled:,:]
                     .query('`userInteractions.val.controlEvent.val` == "touchDown"')
                     .index[0])

    dt = int(pd.to_datetime(df.loc[[idx_uiEnabled, idx_touchDown],'timestamp']) #select 2 rows of relevant events
                      .diff()                  #Compute difference in time 
                      .dt.total_seconds()      #Convert to seconds
                      .iloc[-1]*1000)          #select last row (of 2 rows, first row is NaN) and convert to ms
    return  df.assign(imputed_rt_ms = dt)


def _replace_wrong_timing_with_median(rtSeries):
    """Given a Series of response times replace values with a specific value with median.

    Returns: Pandas series
    """
    idx = (rtSeries==WRONG_TIME_MARK_FOR_IMPUTATION)
    rtSeries = rtSeries.copy()
    rtSeries[idx] = rtSeries[~idx].median()
    return rtSeries

def impute_missing_response_timing(syn, project_id, df_stepdata, filters, assessmentId):
    """Imputes response times from interaction data and merges this with stepdata

    Args:
        syn: Synapse object
        project_id: parquet file synID
        df_stepdata: original stepdata data frame
        filters: list of filters passed to load Parquet function
        assessmentId: used ot determine fields for scoring

    return updated stepdata data framw with updated timing information
    """
    df = _load_interaction_data(syn, project_id, filters)

    # Load the items Used for scoring 
    config = ut.get_config()
    score_items = config[assessmentId+'_item']

    #Determine deltaT in ms for each id and stepIdentifier and remove duplicates
    timing = df.groupby(['id', 'stepIdentifier']).apply(_computeDT, score_items).reset_index(drop=True)
    timing = timing[['id', 'recordid', 'stepIdentifier', 'imputed_rt_ms']].drop_duplicates()

    #For events that are in wrong order (timing was set to WRONG_TIME_MARK_FOR_IMPUTATION9) 
    # replace with median for that administration (id)
    timing['imputed_rt_ms'] = (timing
                                .groupby('id')['imputed_rt_ms']
                                .apply(_replace_wrong_timing_with_median)
                                .values)

    #Merge new itming information with df_stepdata
    #TODO move the imputed_rt_ms  to responseTime if happy with results
    return df_stepdata.merge(timing,
                             left_on= ['recordid', 'identifier', 'id'],
                             right_on=['recordid', 'stepIdentifier', 'id', ])
