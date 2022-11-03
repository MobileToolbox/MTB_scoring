"""
data - Before using the fuction, supposed we already have the step data you want(in csv format). If you are not able to 
collect the data you want from Synapse, please take sometime to look at the Vignettes.

input: Step_data - stepData you collected from Synapse
       type - Which score function you want to get 


output:A dataframe contains id, identifier and score 
"""

def Get_Stepdata(Step_data,type):
    
    if type=='MFS':
        Step_data=Step_data[Step_data['assessmentid']=='memory-for-sequences']
        Step_data=Step_data[['id','identifier','score']]
        Step_S=Step_data.pivot_table(values='score', 
                   index='id'
                   ,columns='identifier').reset_index('id') 
        return Step_S
    elif type=='Number Match':
        Step_data=Step_data[Step_data['assessmentid']=='number-match']
        Step_data=Step_data[['id','identifier','score']]
        Step_S=Step_data.pivot_table(values='score', 
                   index='id'
                   ,columns='identifier').reset_index('id') 
        return Step_S
    elif type=='FNAME':
        Step_data=Step_data[Step_data['assessmentid']=='fnameb']
        Step_data=Step_data[['id','identifier','score']]
        Step_S=Step_data.pivot_table(values='score', 
                   index='id'
                   ,columns='identifier').reset_index('id') 
        return Step_S
    elif type=='DCCS':                                 
        
        Step_data=Step_data[Step_data['assessmentid']=='dccs']
        dccst1=Step_data[['id','identifier','responseTime']]

        dccst2=dccst1.pivot_table(values='responseTime', 
                   index='id'
                   ,columns='identifier').reset_index('id') 

        dccss1=Step_data[['id','identifier','score']]

        dccss2=dccss1.pivot_table(values='score', 
                   index='id'
                   ,columns='identifier').reset_index('id') 
        return (dccst2, dccss2)
    elif type=='Flanker':                               
        
        Step_data=Step_data[Step_data['assessmentid']=='flanker']
        flkrt1=Step_data[['id','identifier','responseTime']]

        flkrt2=flkrt1.pivot_table(values='responseTime', 
                   index='id'
                   ,columns='identifier').reset_index('id') 

        flkrs1=Step_data[['id','identifier','score']]

        flkrs2=flkrs1.pivot_table(values='score', 
                   index='id'
                   ,columns='identifier').reset_index('id') 
        return (flkrt2, flkrs2)
    elif type=='PSM' or type=='Spelling' or type=='Vocab':
        raise Exception("Please use another function called 'GetScore_SpellingAndVocab' to get the step data for your data ")
    else:
        raise Exception("There is no score function named this , please check the score name ")
        
    return 

