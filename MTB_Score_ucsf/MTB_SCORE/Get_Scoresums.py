"""
This is the function to get scoresum 

input: data - data output from Get_Stepdata function 
       s_items - the list output from Get_Score_items function
       type - Which score function you want to get 
output:
"""
def Get_Scoresum(data,s_items,type):
    if type=='MFS':
        data['mfs_sum']=data.loc[:,s_items].sum(axis=1)
        return data
    if type=='Number Match':
        data['numberMatch_raw']=data.loc[:,s_items].sum(axis=1)
        return data
    if  type=='FNAME':
        data['FNAME_sum']=data.loc[:,s_items].sum(axis=1)
        return data
    elif type=='DCCS':                              
        data[0]['dccs_time']=data[0].loc[:,s_items].sum(axis=1)/1000

        data[1]['dccs_score']=data[1].loc[:,s_items].sum(axis=1)

        dccs_time=data[0][['id','dccs_time']]
        dccs_score=data[1][['id','dccs_score']]

        data=dccs_time.merge(dccs_score,on='id')
        data['DCCS_accuracy']=data['dccs_score']/data['dccs_time']
        return data
    elif  type=='Flanker':                                
        data[0]['flkr_time']=data[0].loc[:,s_items].sum(axis=1)/1000

        data[1]['flkr_score']=data[1].loc[:,s_items].sum(axis=1)

        flkr_time=data[0][['id','flkr_time']]
        flkr_score=data[1][['id','flkr_score']]

        data=flkr_time.merge(flkr_score,on='id')
        data['FLANKER_accuracy']=data['flkr_score']/data['flkr_time']
        return data
    elif type=='PSM' or type=='Spelling' or type=='Vocab':
        raise Exception("Please use another function called 'GetScore_SpellingAndVocab' to get the step data for your data ")
    else:
        raise Exception("There is no score function named this, please check the score name ")
        
    return 