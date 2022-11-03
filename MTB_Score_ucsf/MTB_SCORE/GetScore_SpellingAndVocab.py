"""


input: task_data - task Data you collected from Synapse
       dfid - dfid data you collected from Sunapsez
       type - Which score function you want to get, in this function, there are onlu three choices: Spelling, Vocab and PSM


output:dataframe includes columns 'healthcode'and score for spelling or vocab.

"""

def GetScore_SpellingAndVocab(task_data,dfid,type):
    if type=='Spelling':
        task_data=task_data[task_data['assessmentid']=='spelling']
        df_spelling_scrd=task_data
        df_spelling_scrd=df_spelling_scrd.reset_index()


        #df_spelling_scrd['spell_THETA'] = df_spelling_scrd['finalTheta_double']
        #df_spelling_scrd['spell_SE'] = df_spelling_scrd['finalSE_double']
        #df_spelling_scrd.loc[df_spelling_scrd['finalTheta_double'].isna(), 'spell_THETA'] = df_spelling_scrd['finalTheta_int']
        #df_spelling_scrd.loc[df_spelling_scrd['finalSE_double'].isna(), 'spell_SE'] = df_spelling_scrd['finalSE_int']
        
        df_spelling_scrd['spell_THETA'] = df_spelling_scrd['scores_finalTheta_double']
        df_spelling_scrd['spell_SE'] = df_spelling_scrd['scores_finalSE_double']


        df_spelling_scrd2=df_spelling_scrd[['recordid','steps','spell_THETA','spell_SE','taskStatus']]

        df_spelling_scrd3=df_spelling_scrd2.merge(dfid, on='recordid', how='left')

        spelling_score=df_spelling_scrd3[['healthcode','spell_THETA','spell_SE', 'recordid','assessmentid', 
                                         'deviceInfo', 'startDate', 'endDate', 'participantversion', 'clientinfo']]
        return spelling_score
    elif type=='Vocab': 
        task_data=task_data[task_data['assessmentid']=='vocabulary'] 
        df_vocab_scrd=task_data
        df_vocab_scrd=df_vocab_scrd.reset_index()


        #df_vocab_scrd['vocab_THETA'] = df_vocab_scrd['finalTheta_double']
        #df_vocab_scrd['vocab_SE'] = df_vocab_scrd['finalSE_double']

        #df_vocab_scrd.loc[df_vocab_scrd['finalTheta_double'].isna(), 'vocab_THETA'] = df_vocab_scrd['finalTheta_int']
        #df_vocab_scrd.loc[df_vocab_scrd['finalSE_double'].isna(), 'vocab_SE'] = df_vocab_scrd['finalSE_int']
        
        df_vocab_scrd['vocab_THETA'] = df_vocab_scrd['scores_finalTheta_double']
        df_vocab_scrd['vocab_SE'] = df_vocab_scrd['scores_finalSE_double']
        

 
        df_vocab_scrd2=df_vocab_scrd[['recordid','steps','vocab_THETA','vocab_SE','taskStatus']]


        df_vocab_scrd3=df_vocab_scrd2.merge(dfid, on='recordid', how='left')


        vocab_score=df_vocab_scrd3[['healthcode','vocab_THETA','vocab_SE', 'recordid','assessmentid',
                                   'deviceInfo', 'startDate', 'endDate', 'participantversion', 'clientinfo']]
        return vocab_score
    elif type=='PSM':
        task_data=task_data[task_data['assessmentid']=='psm'] 
        psm_taskData_scrd=task_data.loc[((~task_data['scores_adjacentPairs1'].isna()) &
                                      (~task_data['scores_adjacentPairs2'].isna())  )]
        
        psm_taskData_scrd['psm.adj.sum'] = psm_taskData_scrd.loc[:,['scores_adjacentPairs1','scores_adjacentPairs2']].sum(axis=1)
        psm_taskData_scrd['psm.exact.sum'] = psm_taskData_scrd.loc[:,['scores_exactMatch1','scores_exactMatch2']].sum(axis=1)

        psm_taskData_scrd2=psm_taskData_scrd[['recordid','steps','psm.adj.sum','psm.exact.sum']]

        psm_taskData_scrd3=psm_taskData_scrd2.merge(dfid, on='recordid', how='left')

        psm_score=psm_taskData_scrd3[['healthcode','psm.adj.sum','psm.exact.sum', 'recordid','assessmentid',
                                     'deviceInfo', 'startDate', 'endDate', 'participantversion', 'clientinfo']]
        return psm_score

    else:
        raise Exception("There is no score function named this, please check the score name ")
        
    return 
