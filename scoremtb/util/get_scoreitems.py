# MTB Scoring

# import the required packages
from scoremtb.util import util as ut

def get_items(type):
    """
    -----------------------------------------------------------------------------------------
    
    This is a function to get the score items for score functions.
    
    Args:
        type: Task/Score
        
    Return:
        Task/Score item list
    
    -----------------------------------------------------------------------------------------
    """
    config = ut.get_config()
    
    if type == config['MFS_type']:
         items = config['mfs_item']
            
    elif type == config['DCCS_type']:
         items= config['dccs_item']

    elif type == config['Flanker_type']:
         items = config['flnk_item']
            
    elif type == config["NM_type"]:
         items= config['nm_item']
            
    elif type == config["fname_type"]:
         items= config["fname_item"]
            
    elif type == config['PSM_type'] or type == config['spell_type'] or type == config['vocab_type']:
        raise Exception("These score functions don't require score_items ")
    else:
        raise Exception("There is no score function named this , please check the score name ")
    
    return items