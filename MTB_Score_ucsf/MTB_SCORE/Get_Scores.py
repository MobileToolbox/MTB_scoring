"""
This is the function to get score 

input: data - the data output from function GetScoresum
       task_data - taskdata you downloaded from Synapse
       dfid - dfid dataset you downloaded from Synapse
output:
"""
import pandas as pd
def Get_Score(data,task_data, dfid,type):
    if type=='DCCS':
        task_data=task_data[task_data['assessmentid']=='dccs']  
        data_scored=data.merge(task_data, left_on='id', right_on='steps')
        data_score_filter=data_scored[['recordid','dccs_time','dccs_score','DCCS_accuracy','taskStatus']]

        data_score_filter_meta=data_score_filter.merge(dfid, on='recordid', how='left')

        data_score=data_score_filter_meta[['healthcode','dccs_time','dccs_score','DCCS_accuracy', 'recordid','assessmentid',
                                          'deviceInfo', 'startDate', 'endDate', 'participantversion', 'clientinfo']]
        return data_score
    if  type=='Flanker':
        task_data=task_data[task_data['assessmentid']=='flanker']  
        data_scored=data.merge(task_data, left_on='id', right_on='steps')
        data_score_filter=data_scored[['recordid','flkr_time','flkr_score','FLANKER_accuracy','taskStatus']]

        data_score_filter_meta=data_score_filter.merge(dfid, on='recordid', how='left')

        data_score=data_score_filter_meta[['healthcode','flkr_time','flkr_score','FLANKER_accuracy', 'recordid','assessmentid',
                                          'deviceInfo', 'startDate', 'endDate', 'participantversion', 'clientinfo']]
        return data_score
    elif type=='MFS':
        task_data=task_data[task_data['assessmentid']=='memory-for-sequences']                               
        data_scored=data.merge(task_data, left_on='id', right_on='steps')
        data_score_filter=data_scored[['recordid','mfs_sum','taskStatus']]

        data_score_filter_meta=data_score_filter.merge(dfid, on='recordid', how='left')

        data_score=data_score_filter_meta[['healthcode','mfs_sum', 'recordid','assessmentid',
                                          'deviceInfo', 'startDate', 'endDate', 'participantversion', 'clientinfo']]
        return data_score
    elif type=='Number Match': 
        task_data=task_data[task_data['assessmentid']=='number-match']                               
        data_scored=data.merge(task_data, left_on='id', right_on='steps')
        data_score_filter=data_scored[['recordid','numberMatch_raw','taskStatus']]

        data_score_filter_meta=data_score_filter.merge(dfid, on='recordid', how='left')

        data_score=data_score_filter_meta[['healthcode','numberMatch_raw', 'recordid','assessmentid',
                                          'deviceInfo', 'startDate', 'endDate', 'participantversion', 'clientinfo']]
        return data_score
    
    if type=='FNAME':
        rawFname=[i for i in range(37)]
        theta=[-3.532,-3.306,-3.102,-2.916,-2.746,-2.59,-2.445,-2.31,-2.183,-2.061,-1.944,-1.83,-1.717,-1.603,-1.487,-1.368,-1.245,-1.117,
               -0.985,-0.848,-0.707,-0.561,-0.411,-0.256,-0.098,0.066,0.234,0.407,0.587,0.774,0.97,1.176,1.395,1.63,1.886,2.168,2.485]
        sd=[0.583,0.565,0.55,0.54,0.532,0.527,0.523,0.52,0.518,0.516,0.513,0.51,0.506,0.502,0.496,0.489,0.482,0.475,0.469,0.463,0.458,
            0.454,0.451,0.449,0.449,0.45,0.453,0.457,0.462,0.47,0.479,0.491,0.505,0.523,0.546,0.573,0.608]
        fname_lookup=pd.DataFrame(columns=['rawFname','theta','sd'])
        fname_lookup['rawFname']=rawFname
        fname_lookup['theta']=theta
        fname_lookup['sd']=sd
        task_data=task_data[task_data['assessmentid']=='fnameb']    
        data_scored=data.merge(task_data, left_on='id', right_on='steps')
        data_score_filter=data_scored[['recordid','FNAME_sum','taskStatus']]

        data_score_filter_meta=data_score_filter.merge(dfid, on='recordid', how='left')
        
        data_score_filter_meta2=data_score_filter_meta.merge(fname_lookup,left_on='FNAME_sum', right_on='rawFname', how='left')
        data_score_filter_meta2.rename(columns = {'theta':'FNAME_theta', 'sd':'FNAME_sd'}, inplace = True)

        data_score=data_score_filter_meta2[['healthcode','FNAME_theta','FNAME_sd', 'recordid','assessmentid',
                                           'deviceInfo', 'startDate', 'endDate', 'participantversion', 'clientinfo']]
        return data_score
    elif type=='PSM' or type=='Spelling' or type=='Vocab':
        raise Exception("Please use another function called  to get the step data for your data ")
    else:
        raise Exception("There is no score function named  , please check the score name ")
        
    return 