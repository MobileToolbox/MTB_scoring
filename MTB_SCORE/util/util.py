# MTB Scoring

# import the required packages
import os
import json

def get_config():
    """
    -----------------------------------------------------------------------------------------
    Loading json config
    
    Returns:
        Config object
    -----------------------------------------------------------------------------------------
    """
    
    dir_name = os.path.dirname(os.path.abspath(__file__))
    measure_path = os.path.abspath(os.path.join(dir_name, '../config/config.json'))
    
    file = open(measure_path)
    conf = json.load(file)
    return conf