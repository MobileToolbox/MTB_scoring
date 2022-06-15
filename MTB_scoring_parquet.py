# -*- coding: utf-8 -*-
"""
Created on Wed May  4 15:07:54 2022

@author: had717
"""

import synapseclient
import pandas as pd
import boto3
import io
import datetime

dtoday=datetime.date.today()
#######################################################
# Change this to the synapse Id of the Parquet folder
PROJECT_ID = 'syn26381176'
#PROJECT_ID = 'syn27214807'
#######################################################

syn = synapseclient.login('', '')

token = syn.get_sts_storage_token(PROJECT_ID, permission="read_only") # , output_format='bash'


session = boto3.Session( 
          aws_access_key_id = token['accessKeyId'], 
          aws_secret_access_key =  token['secretAccessKey'], 
             aws_session_token = token['sessionToken']) 
buffer=io.BytesIO()
#Then use the session to get the resource
s3 = session.resource('s3')
my_bucket = s3.Bucket(token['bucket'])
print(my_bucket)
object=s3.Object(token['bucket'], 'mtb/mtb_construct/parquet/dataset_weather/taskIdentifier=Vocabulary Form 1/year=2021/month=9/day=16/recordId=48IYe6Jr6yZfQFRrnI1HWfNr/part-00001-134a3a81-d470-44c5-8f66-f6c8e65c69c3.c000.snappy.parquet')



def getS3data(my_bucket, path, token):
    #mfs steps data:
    df1=pd.DataFrame()
    #MFS pilot 2   steps  list evverything in my_bucket including subdirectories
    for objects in my_bucket.objects.filter(Prefix=path):
        buffer=io.BytesIO()
        object=s3.Object(token['bucket'], objects.key)
        object.download_fileobj(buffer)
        #print(objects.key)
       
        df2 = pd.read_parquet(buffer)
        df1=df1.append(df2)
        
         
    buffer.close()
    
    
    return df1

#Get study_membership, healthcode, and other metadata
dfid=getS3data(my_bucket,"mtb/mtb_construct/parquet/dataset_metadata_v1/" , token)

#Get MFS data and score it:
mfs_taskData=getS3data(my_bucket,"mtb/mtb_construct/parquet/dataset_taskData/taskIdentifier=MFS pilot 2/" , token)
mfs_stepsData=getS3data(my_bucket,"mtb/mtb_construct/parquet/dataset_taskData_steps/taskIdentifier=MFS pilot 2/" , token)

mfs1=mfs_stepsData[['id','identifier','score']]

# score mfs:
mfs2=mfs1.pivot_table(values='score', 
                   index='id'
                   ,columns='identifier').reset_index('id') 

mfs_items=[
'MFS_2_1',
'MFS_2_2',
'MFS_2_3',
'MFS_3_1',
'MFS_3_2',
'MFS_3_3',
'MFS_3_4',
'MFS_3_5',
'MFS_3_6',
'MFS_4_1',
'MFS_4_2',
'MFS_4_3',
'MFS_5_1',
'MFS_5_2',
'MFS_5_3',
'MFS_6_1',
'MFS_6_2',
'MFS_6_3',
'MFS_7_1',
'MFS_7_2',
'MFS_7_3',
'MFS_8_1',
'MFS_8_2',
'MFS_8_3',
'MFS_9_1',
'MFS_9_2',
'MFS_9_3',
'MFS_10_1',
'MFS_10_2',
'MFS_10_3'
]


mfs2['mfs_sum']=mfs2.loc[:,mfs_items].sum(axis=1)# Use explicit columns to sum instead of index guessing

#now link status and recordid and externalid    and dates created and ended
mfs_scored=mfs2.merge(mfs_taskData, left_on='id', right_on='steps')
mfs_score_filter=mfs_scored[['partition_recordid','mfs_sum','taskStatus']]

mfs_score_filter_meta=mfs_score_filter.merge(dfid, on='partition_recordid', how='left')



#PSM score----------------------------------------------------------------------------------------------------------------------------


psm_taskData=getS3data(my_bucket,"mtb/mtb_construct/parquet/dataset_taskData/taskIdentifier=Picture Sequence MemoryV1/" , token)

psm_taskData_scrd=psm_taskData.loc[((~psm_taskData['adjacentPairs1'].isna()) &
                                      (~psm_taskData['adjacentPairs2'].isna()) & 
                                      (psm_taskData['taskStatus']=='completed') )]
psm_taskData_scrd['psm.adj.sum'] = psm_taskData_scrd.loc[:,['adjacentPairs1','adjacentPairs2']].sum(axis=1)
psm_taskData_scrd['psm.exact.sum'] = psm_taskData_scrd.loc[:,['exactMatch1','exactMatch2']].sum(axis=1)

psm_taskData_scrd2=psm_taskData_scrd[['partition_recordid','steps','psm.adj.sum','psm.exact.sum']]

psm_taskData_scrd3=psm_taskData_scrd2.merge(dfid, on='partition_recordid', how='left')



#DCCS score------------------------------------------------------------------------------------------------------------------------

dccs_taskData=getS3data(my_bucket,"mtb/mtb_construct/parquet/dataset_taskData/taskIdentifier=Dimensional Change Card Sort/" , token)
dccs_stepsData=getS3data(my_bucket,"mtb/mtb_construct/parquet/dataset_taskData_steps/taskIdentifier=Dimensional Change Card Sort/" , token)

#Get response times
dccst1=dccs_stepsData[['id','identifier','responseTime_int']]

dccst2=dccst1.pivot_table(values='responseTime_int', 
                   index='id'
                   ,columns='identifier').reset_index('id') 

#Get scores
dccss1=dccs_stepsData[['id','identifier','score']]

dccss2=dccss1.pivot_table(values='score', 
                   index='id'
                   ,columns='identifier').reset_index('id') 

dccs_items=[
'MTBDCCS_MIXED_TRIAL_1_c',
'MTBDCCS_MIXED_TRIAL_2_c',
'MTBDCCS_MIXED_TRIAL_3_c',
'MTBDCCS_MIXED_TRIAL_4_c',
'MTBDCCS_MIXED_TRIAL_5_c',
'MTBDCCS_MIXED_TRIAL_6_c',
'MTBDCCS_MIXED_TRIAL_7_c',
'MTBDCCS_MIXED_TRIAL_8_c',
'MTBDCCS_MIXED_TRIAL_9_c',
'MTBDCCS_MIXED_TRIAL_10_c',
'MTBDCCS_MIXED_TRIAL_11_c',
'MTBDCCS_MIXED_TRIAL_12_c',
'MTBDCCS_MIXED_TRIAL_13_c',
'MTBDCCS_MIXED_TRIAL_14_c',
'MTBDCCS_MIXED_TRIAL_15_c',
'MTBDCCS_MIXED_TRIAL_16_c',
'MTBDCCS_MIXED_TRIAL_17_c',
'MTBDCCS_MIXED_TRIAL_18_c',
'MTBDCCS_MIXED_TRIAL_19_c',
'MTBDCCS_MIXED_TRIAL_20_c',
'MTBDCCS_MIXED_TRIAL_21_c',
'MTBDCCS_MIXED_TRIAL_22_c',
'MTBDCCS_MIXED_TRIAL_23_c',
'MTBDCCS_MIXED_TRIAL_24_c',
'MTBDCCS_MIXED_TRIAL_25_c',
'MTBDCCS_MIXED_TRIAL_26_c',
'MTBDCCS_MIXED_TRIAL_27_c',
'MTBDCCS_MIXED_TRIAL_28_c',
'MTBDCCS_MIXED_TRIAL_29_c',
'MTBDCCS_MIXED_TRIAL_30_c'

]


#Total time
dccst2['dccs_time']=dccst2.loc[:,dccs_items].sum(axis=1)/1000# Use explicit columns to sum instead of index guessing
#Score sum
dccss2['dccs_score']=dccss2.loc[:,dccs_items].sum(axis=1)# Use explicit columns to sum instead of index guessing

dccs_time=dccst2[['id','dccs_time']]
dccs_score=dccss2[['id','dccs_score']]

dccs_acc=dccs_time.merge(dccs_score,on='id')
#Accuraccy
dccs_acc['DCCS_accuracy']=dccs_acc['dccs_score']/dccs_acc['dccs_time']

dccs_acc_scored=dccs_acc.merge(dccs_taskData, left_on='id', right_on='steps')

dccs_acc_score_filter=dccs_acc_scored[['partition_recordid','dccs_time','dccs_score','DCCS_accuracy','taskStatus']]

dccs_acc_score_filter_meta=dccs_acc_score_filter.merge(dfid, on='partition_recordid', how='left')



#Flanker score-------------------------------------------------------------------------------------------------------------------

flkr_taskData=getS3data(my_bucket,"mtb/mtb_construct/parquet/dataset_taskData/taskIdentifier=Flanker Inhibitory Control/" , token)
flkr_stepsData=getS3data(my_bucket,"mtb/mtb_construct/parquet/dataset_taskData_steps/taskIdentifier=Flanker Inhibitory Control/" , token)


flkrt1=flkr_taskData[['id','identifier','responseTime_int']]


flkrt2=flkrt1.pivot_table(values='responseTime_int', 
                   index='id'
                   ,columns='identifier').reset_index('id') 

flkrs1=flkr_taskData[['id','identifier','score']]

flkrs2=flkrs1.pivot_table(values='score', 
                   index='id'
                   ,columns='identifier').reset_index('id') 

flkr_items=[
'A01c',
'A02c',
'A03c',
'A04c',
'A05c',
'A06c',
'A07c',
'A08c',
'A09c',
'A10c',
'A11c',
'A12c',
'A13c',
'A14c',
'A15c',
'A16c',
'A17c',
'A18c',
'A19c',
'A20c',
'A21c',
'A22c',
'A23c',
'A24c',
'A25c',
'A26c',
'A27c',
'A28c',
'A29c',
'A30c',
'A31c',
'A32c',
'A33c',
'A34c',
'A35c',
'A36c',
'A37c',
'A38c',
'A39c',
'A40c',
'A41c',
'A42c',
'A43c',
'A44c',
'A45c',
'A46c',
'A47c',
'A48c',
'A49c',
'A50c'
]


flkrt2['flkr_time']=flkrt2.loc[:,flkr_items].sum(axis=1)/1000# Use explicit columns to sum instead of index guessing

flkrs2['flkr_score']=flkrs2.loc[:,flkr_items].sum(axis=1)# Use explicit columns to sum instead of index guessing

flkr_time=flkrt2[['id','flkr_time']]
flkr_score=flkrs2[['id','flkr_score']]

flkr_acc=flkr_time.merge(flkr_score,on='id')
flkr_acc['FLANKER_accuracy']=flkr_acc['flkr_score']/flkr_acc['flkr_time']

flkr_acc_scored=flkr_acc.merge(flkr_taskData, left_on='id', right_on='steps')
flkr_acc_score_filter=flkr_acc_scored[['partition_recordid','flkr_time','flkr_score','FLANKER_accuracy','taskStatus']]

flkr_acc_score_filter_meta=flkr_acc_score_filter.merge(dfid, on='partition_recordid', how='left')



#Number Match score----------------------------------------------------------------------------------------------------------------

nm_taskData=getS3data(my_bucket,"mtb/mtb_construct/parquet/dataset_taskData/taskIdentifier=Number Match/" , token)
nm_stepsData=getS3data(my_bucket,"mtb/mtb_construct/parquet/dataset_taskData_steps/taskIdentifier=Number Match/" , token)

nm1=nm_stepsData[['id','identifier','score']]

nm2=nm1.pivot_table(values='score', 
                   index='id'
                   ,columns='identifier').reset_index('id') 

nm_items=[
'NM_TEST_01_A',
    'NM_TEST_01_B',
    'NM_TEST_01_C',
    'NM_TEST_01_D',
    'NM_TEST_01_E',
    'NM_TEST_01_F',
    'NM_TEST_01_G',
    'NM_TEST_01_H',
    'NM_TEST_01_I',
    'NM_TEST_02_A',
    'NM_TEST_02_B',
    'NM_TEST_02_C',
    'NM_TEST_02_D',
    'NM_TEST_02_E',
    'NM_TEST_02_F',
    'NM_TEST_02_G',
    'NM_TEST_02_H',
    'NM_TEST_02_I',
    'NM_TEST_03_A',
    'NM_TEST_03_B',
    'NM_TEST_03_C',
    'NM_TEST_03_D',
    'NM_TEST_03_E',
    'NM_TEST_03_F',
    'NM_TEST_03_G',
    'NM_TEST_03_H',
    'NM_TEST_03_I',
    'NM_TEST_04_A',
    'NM_TEST_04_B',
    'NM_TEST_04_C',
    'NM_TEST_04_D',
    'NM_TEST_04_E',
    'NM_TEST_04_F',
    'NM_TEST_04_G',
    'NM_TEST_04_H',
    'NM_TEST_04_I',
    'NM_TEST_05_A',
    'NM_TEST_05_B',
    'NM_TEST_05_C',
    'NM_TEST_05_D',
    'NM_TEST_05_E',
    'NM_TEST_05_F',
    'NM_TEST_05_G',
    'NM_TEST_05_H',
    'NM_TEST_05_I',
    'NM_TEST_06_A',
    'NM_TEST_06_B',
    'NM_TEST_06_C',
    'NM_TEST_06_D',
    'NM_TEST_06_E',
    'NM_TEST_06_F',
    'NM_TEST_06_G',
    'NM_TEST_06_H',
    'NM_TEST_06_I',
    'NM_TEST_07_A',
    'NM_TEST_07_B',
    'NM_TEST_07_C',
    'NM_TEST_07_D',
    'NM_TEST_07_E',
    'NM_TEST_07_F',
    'NM_TEST_07_G',
    'NM_TEST_07_H',
    'NM_TEST_07_I',
    'NM_TEST_08_A',
    'NM_TEST_08_B',
    'NM_TEST_08_C',
    'NM_TEST_08_D',
    'NM_TEST_08_E',
    'NM_TEST_08_F',
    'NM_TEST_08_G',
    'NM_TEST_08_H',
    'NM_TEST_08_I',
    'NM_TEST_09_A',
    'NM_TEST_09_B',
    'NM_TEST_09_C',
    'NM_TEST_09_D',
    'NM_TEST_09_E',
    'NM_TEST_09_F',
    'NM_TEST_09_G',
    'NM_TEST_09_H',
    'NM_TEST_09_I',
    'NM_TEST_10_A',
    'NM_TEST_10_B',
    'NM_TEST_10_C',
    'NM_TEST_10_D',
    'NM_TEST_10_E',
    'NM_TEST_10_F',
    'NM_TEST_10_G',
    'NM_TEST_10_H',
    'NM_TEST_10_I',
    'NM_TEST_11_A',
    'NM_TEST_11_B',
    'NM_TEST_11_C',
    'NM_TEST_11_D',
    'NM_TEST_11_E',
    'NM_TEST_11_F',
    'NM_TEST_11_G',
    'NM_TEST_11_H',
    'NM_TEST_11_I',
    'NM_TEST_12_A',
    'NM_TEST_12_B',
    'NM_TEST_12_C',
    'NM_TEST_12_D',
    'NM_TEST_12_E',
    'NM_TEST_12_F',
    'NM_TEST_12_G',
    'NM_TEST_12_H',
    'NM_TEST_12_I',
    'NM_TEST_13_A',
    'NM_TEST_13_B',
    'NM_TEST_13_C',
    'NM_TEST_13_D',
    'NM_TEST_13_E',
    'NM_TEST_13_F',
    'NM_TEST_13_G',
    'NM_TEST_13_H',
    'NM_TEST_13_I',
    'NM_TEST_14_A',
    'NM_TEST_14_B',
    'NM_TEST_14_C',
    'NM_TEST_14_D',
    'NM_TEST_14_E',
    'NM_TEST_14_F',
    'NM_TEST_14_G',
    'NM_TEST_14_H',
    'NM_TEST_14_I',
    'NM_TEST_15_A',
    'NM_TEST_15_B',
    'NM_TEST_15_C',
    'NM_TEST_15_D',
    'NM_TEST_15_E',
    'NM_TEST_15_F',
    'NM_TEST_15_G',
    'NM_TEST_15_H',
    'NM_TEST_15_I',
    'NM_TEST_16_A',
    'NM_TEST_16_B',
    'NM_TEST_16_C',
    'NM_TEST_16_D',
    'NM_TEST_16_E',
    'NM_TEST_16_F',
    'NM_TEST_16_G',
    'NM_TEST_16_H',
    'NM_TEST_16_I'
]


nm2['numberMatch_raw']=nm2.loc[:,nm_items].sum(axis=1)# Use explicit columns to sum instead of index guessing

#now link status and recordid and externalid    and dates created and ended
nm_scored=nm2.merge(nm_taskData, left_on='id', right_on='steps')

nm_score_filter=nm_scored[['partition_recordid','numberMatch_raw','taskStatus']]

nm_score_filter_meta=nm_score_filter.merge(dfid, on='partition_recordid', how='left')



#FNAME score-----------------------------------------------------------------------------------------------------------------------

fname_taskData=getS3data(my_bucket,"mtb/mtb_construct/parquet/dataset_taskData/taskIdentifier=FNAME Test Form 1/" , token)
fname_stepsData=getS3data(my_bucket,"mtb/mtb_construct/parquet/dataset_taskData_steps/taskIdentifier=FNAME Test Form 1/" , token)
fname_lookup = pd.read_csv(r'R:\MSS\Research\Projects\MTB\Programs\Scoring_Code\fname_raw-to-theta.csv', index_col=False)

fname1=fname_stepsData[['id','identifier','score']]

# score fname:
fname2=fname1.pivot_table(values='score', 
                   index='id'
                   ,columns='identifier').reset_index('id') 

fname_items=[
'FSBT_MTB_01_A',
    'FSBT_MTB_02_A',
    'FSBT_MTB_03_A',
    'FSBT_MTB_04_A',
    'FSBT_MTB_05_A',
    'FSBT_MTB_06_A',
    'FSBT_MTB_07_A',
    'FSBT_MTB_08_A',
    'FSBT_MTB_09_A',
    'FSBT_MTB_10_A',
    'FSBT_MTB_11_A',
    'FSBT_MTB_12_A',
    'FNLT_MTB_01_A',
    'FNLT_MTB_02_A',
    'FNLT_MTB_03_A',
    'FNLT_MTB_04_A',
    'FNLT_MTB_05_A',
    'FNLT_MTB_06_A',
    'FNLT_MTB_07_A',
    'FNLT_MTB_08_A',
    'FNLT_MTB_09_A',
    'FNLT_MTB_10_A',
    'FNLT_MTB_11_A',
    'FNLT_MTB_12_A',
    'FNMT_MTB_01_A',
    'FNMT_MTB_02_A',
    'FNMT_MTB_03_A',
    'FNMT_MTB_04_A',
    'FNMT_MTB_05_A',
    'FNMT_MTB_06_A',
    'FNMT_MTB_07_A',
    'FNMT_MTB_08_A',
    'FNMT_MTB_09_A',
    'FNMT_MTB_10_A',
    'FNMT_MTB_11_A',
    'FNMT_MTB_12_A'

]


fname2['FNAME_sum']=fname2.loc[:,fname_items].sum(axis=1)# Use explicit columns to sum instead of index guessing


#now link status and recordid and externalid    and dates created and ended
fname_scored=fname2.merge(fname_taskData, left_on='id', right_on='steps')

fname_score_filter=fname_scored[['partition_recordid','FNAME_sum','taskStatus']]

fname_score_filter_meta=fname_score_filter.merge(dfid, on='partition_recordid', how='left')

fname_score_filter_meta2=fname_score_filter_meta.merge(fname_lookup,left_on='FNAME_sum', right_on='rawFname', how='left')
fname_score_filter_meta2.rename(columns = {'theta':'FNAME_theta', 'sd':'FNAME_sd'}, inplace = True)


#Spelling------------------------------------------------------------------------------------------------------------------------

spelling_taskData=getS3data(my_bucket,"mtb/mtb_construct/parquet/dataset_taskData/taskIdentifier=MTB Spelling Form 1/" , token)

df_spelling_scrd=spelling_taskData

df_spelling_scrd['spell_THETA'] = df_spelling_scrd['finalTheta_double']
df_spelling_scrd['spell_SE'] = df_spelling_scrd['finalSE_double']
df_spelling_scrd.loc[df_spelling_scrd['spell_THETA'].isna(), 'spell_THETA'] = df_spelling_scrd['finalTheta_int']
df_spelling_scrd.loc[df_spelling_scrd['spell_SE'].isna(), 'spell_SE'] = df_spelling_scrd['finalSE_int']



df_spelling_scrd2=df_spelling_scrd[['partition_recordid','steps','spell_THETA','spell_SE','taskStatus']]

df_spelling_scrd3=df_spelling_scrd2.merge(dfid, on='partition_recordid', how='left')

#Vocab------------------------------------------------------------------------------------------------------------------------

vocab_taskData=getS3data(my_bucket,"mtb/mtb_construct/parquet/dataset_taskData/taskIdentifier=Vocabulary Form 1/" , token)

df_vocab_scrd=vocab_taskData

df_vocab_scrd['vocab_THETA'] = df_vocab_scrd['finalTheta_double']
df_vocab_scrd['vocab_SE'] = df_vocab_scrd['finalSE_double']
df_vocab_scrd.loc[df_vocab_scrd['vocab_THETA'].isna(), 'vocab_THETA'] = df_vocab_scrd['finalTheta_int']
df_vocab_scrd.loc[df_vocab_scrd['vocab_SE'].isna(), 'vocab_SE'] = df_vocab_scrd['finalSE_int']

 
df_vocab_scrd2=df_vocab_scrd[['partition_recordid','steps','vocab_THETA','vocab_SE','taskStatus']]

df_vocab_scrd3=df_vocab_scrd2.merge(dfid, on='partition_recordid', how='left')




#stop---------------------------------------------------------------------------------------------------------------------------




