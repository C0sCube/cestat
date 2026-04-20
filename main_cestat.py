
import  os
from datetime import datetime

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
import pandas as pd

from app.logger import setup_logger, set_global_logger
from app.konstant import(
    get_data_dir, get_log_dir, 
    create_dir, get_company_dir,
    get_api_config, load_sch_config,
    )
from app.mailer import Mailer
from CESTAT.app.prg_cestat import CESTAT
from app.utils import Helper
from app.schedular import scheduler_loop



def save_csv(df, output_dir, prefix, create_dir=True):
    if create_dir:
        os.makedirs(output_dir, exist_ok=True)
    file_name = f"{prefix}.csv"
    file_path = os.path.join(output_dir, file_name)

    df.to_csv(file_path, index=False)
    return file_path


def program_handler():
    
    date = datetime.now()
    timestamp = date.strftime('%d_%H_%M')
    mailer = Mailer()
    
    api_config = get_api_config()
    companies_path = get_company_dir()
    companies = pd.read_csv(companies_path)["Search Query"].to_list()
    output_dir = create_dir(DATA_DIR,date.strftime("%Y%m%d"))
    try:
        logger.info(f"Running Handler") 
        cestat = CESTAT()        
        
        #get data + filter + final
        raw_data = cestat.get_data(api_config)
        logger.info("Raw data extracted.")
        select_data = cestat.filter_data(raw_data, companies, api_config["filter_on"])
        final_data = select_data[api_config["select_cols"]]
        
        excel_path = os.path.join(output_dir, f"CESTAT_ALL_{timestamp}.xlsx")

        with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
            utils.write_df_safe(
                writer,
                raw_data,
                sheet_name="Raw_Data",
                note_if_empty="No raw data fetched from API"
            )

            utils.write_df_safe(
                writer,
                select_data,
                sheet_name="Filtered_Data",
                note_if_empty="No matching companies found"
            )

            utils.write_df_safe(
                writer,
                final_data,
                sheet_name="Final_Data",
                note_if_empty="Final dataset empty after column selection"
            )

        logger.info("All data written to Excel.")
        
        if mailer.SEND_MAIL:
            logger.info("Sending completion email...")
            mailer.end_mail(
                program=f"{PROGRAM_NAME}: {timestamp}",
                attachments=[excel_path],
            )
    
    except Exception as e:
        logger.critical(e, exc_info=True)
        if mailer.SEND_MAIL:
            logger.info("Sending Error email...")
           
            mailer.fatal_error_mail(
                program=f"{PROGRAM_NAME}: {timestamp}",
                error_message=str(e),
                exception_obj=e,
                dev=True
            )


if __name__ == "__main__":
    
    PROGRAM_NAME = "CESTAT DATA - API CALL (DAILY)"
    DATA_DIR = get_data_dir()
    utils = Helper()
    
    #setup logger + globalize
    logger = setup_logger(
        "watcher", 
        base_dir=get_log_dir(), 
        log_level=12, 
        set_global=True
    )
    set_global_logger(logger)
    logger.info("Starting Scheduler...")
    
    #setup schedular
    sch_data =load_sch_config()
    scheduler_loop(
        logger,
        program_handler,
        sch_data["schedule_days"],
        sch_data["schedule_time"]
    )
        
    
    
    