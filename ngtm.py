import os
import urllib3
from datetime import datetime

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
import pandas as pd

from app.logger import setup_logger, set_global_logger
from app.konstant import (
    get_data_dir, get_log_dir,
    create_dir, load_sch_config,
)
from app.mailer import Mailer
from CESTAT.app.prg_ngt import NGT
from app.utils import Helper
from app.schedular import scheduler_loop


def program_handler():

    date = datetime.now()
    timestamp = date.strftime('%d_%H_%M')
    mailer = Mailer()
    
    
    dir = os.path.join(ROOT_DIR,"docs","config_ngt.json")
    api_config = utils.load_json(dir)
    output_dir = create_dir(DATA_DIR, date.strftime("%Y%m%d"))

    try:
        logger.info("Running NGT Handler")

        ngt = NGT()

        # ---- fetch
        raw_data = ngt.get_data(api_config)
        logger.info("NGT raw data extracted.")

        # ---- no filter yet (keep same pattern)
        final_data = raw_data

        excel_path = os.path.join(output_dir, f"NGT_ALL_{timestamp}.xlsx")

        with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:

            utils.write_df_safe(
                writer,
                raw_data,
                sheet_name="Raw_Data",
                note_if_empty="No raw data fetched"
            )

            utils.write_df_safe(
                writer,
                final_data,
                sheet_name="Final_Data",
                note_if_empty="Final dataset empty"
            )

        logger.info("NGT data written to Excel.")

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

    PROGRAM_NAME = "NGT DATA - SCRAPER (DAILY)"
    DATA_DIR = get_data_dir()
    utils = Helper()
    

    logger = setup_logger(
        "watcher_ngt",
        base_dir=get_log_dir(),
        log_level=12,
        set_global=True
    )

    set_global_logger(logger)
    logger.info("Starting NGT Scheduler...")

    sch_data = load_sch_config()

    scheduler_loop(
        logger,
        program_handler,
        sch_data["schedule_days"],
        sch_data["schedule_time"]
    )