import os
from datetime import datetime
import pandas as pd

from app.prg_cestat import CESTAT
from app.utils import Helper
from app.konstant import DATA_DIR, COMPANY_FILE


class CestatJob:
    name = "CESTAT DATA"

    def __init__(self, data_dir,config, logger, mailer):
        self.config = config
        self.logger = logger
        self.mailer = mailer
        self.utils = Helper()
        self.data_dir = data_dir

    def run(self):
        date = datetime.now()
        timestamp = date.strftime('%d_%H_%M')

        try:
            self.logger.info(f"Running {self.name}")
            companies = pd.read_csv(COMPANY_FILE)["Search Query"].to_list()

            # --- output dir ---
            output_dir = self.utils.create_dir(self.data_dir,date.strftime("%Y%m%d"))

            # --- init ---
            cestat = CESTAT(self.config)

            # --- pipeline ---
            raw_data = cestat.get_data()
            self.logger.info("Raw data extracted.")

            filtered_data = cestat.filter_data(
                raw_data,
                companies,
                self.config["filter_on"]
            )

            final_data = filtered_data[self.config["select_cols"]]

            # --- save ---
            excel_path = os.path.join(
                output_dir,
                f"CESTAT_ALL_{timestamp}.xlsx"
            )

            with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
                self.utils.write_df_safe(writer, raw_data, "Raw_Data")
                self.utils.write_df_safe(writer, filtered_data, "Filtered_Data")
                self.utils.write_df_safe(writer, final_data, "Final_Data")

            self.logger.info("All data written to Excel.")

            # --- mail ---
            if self.mailer.send_enabled:
                self.mailer.end(
                    program=f"{self.name}: {timestamp}",
                    dev=False
                )

            return excel_path

        except Exception as e:
            self.logger.critical(e, exc_info=True)

            if self.mailer.send_enabled:
                self.mailer.error(
                    program=f"{self.name}: {timestamp}",
                    err=e,
                    dev=True
                )

            raise