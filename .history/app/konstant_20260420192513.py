
import json,json5,os, sys

# setup project root
root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(root_dir)

from app.utils import Helper


utils = Helper()




def load_paths(): return utils.load_json(
    os.path.join(root_dir,"paths.json")
)

def get_company_dir():
    """ type: csv"""
    root_dir = load_paths()["root_dir"]
    company_path = os.path.join(root_dir,"docs","COMPANIES.csv")
    return company_path



#code specific config functions

def get_api_config():
    root_dir = load_paths()["root_dir"]
    api_dir = os.path.join(root_dir,"docs","config.json")
    return utils.load_json(api_dir)

    

def get_output_path(): 
    root_dir = load_paths()["root_dir"]
    return utils.create_dir(root_dir,"output")

def get_log_dir():
    out_dir = get_output_path()
    return utils.create_dir(out_dir,"logs")

def get_data_dir():
    out_dir = get_output_path()
    return utils.create_dir(out_dir,"data")

def get_ngt_data_dir():
    out_dir = get_output_path()
    return utils.create_dir(out_dir,"ngt_data")    

#MAILS
def load_mail_data():
    f = load_paths()
    return f["mail_data"]

def load_sch_config():
    f = load_paths()
    return f["schedular_data"]

