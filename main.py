
import os

from app.jobs.job_ibbi import IBBIJob
from app.jobs.job_cestat import CestatJob
# from app.main_ngt import ngt_runner

from app.schedular import scheduler_loop
from app.logger import setup_logger
from app.mailer import Mailer
from app.utils import Helper
from app.konstant import (
    LOG_DIR, CONFIG_DIR, DATA_DIR, 
    load_mail_data, load_schl_data
)


def run_all():

    #CESTAT
    data_dir = utils.create_dir(DATA_DIR,"cestat")
    conf_dir = os.path.join(CONFIG_DIR,"config_cestat.json5")
    config = utils.load_json5(conf_dir)
    cls = CestatJob(data_dir,config,logger,mailer)
    
    cls.run()
    
    #IBBI
    data_dir = utils.create_dir(DATA_DIR,"ibbi")
    conf_dir = os.path.join(CONFIG_DIR,"config_ibbi.json5")
    config = utils.load_json5(conf_dir)
    cls = IBBIJob(data_dir,config,logger,mailer)
    
    cls.run()
    # ngt_runner()


if __name__ == "__main__":

    # --- logger ---
    logger = setup_logger(name="master", base_dir=LOG_DIR, set_global=True)
    utils = Helper()
    
    mail_cfg = load_mail_data()
    mailer = Mailer(mail_cfg)
    
    # --- scheduler config ---
    schl_cfg = load_schl_data()
    sch_days = schl_cfg.get("run_days", ["mon","tue","wed","thu","fri"])
    sch_time = schl_cfg.get("run_times", ["0939"])

    # --- start scheduler ---
    scheduler_loop(
        logger=logger,
        run_fn=run_all,
        sch_days=sch_days,
        sch_time=sch_time
    )
    
    

        # "schedule_time": [
        #     "2355"
        # ],
        # "schedule_days": [
        #     "mon",
        #     "tue",
        #     "wed",
        #     "thu",
        #     "fri",
        #     "sat",
        #     "sun"
        # ]