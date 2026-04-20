import os
from datetime import datetime
import pandas as pd

from app.prg_cestat import CESTAT
from app.utils import Helper
from app.konstant import COMPANY_FILE

class CestatJob:
    name = "CESTAT DATA"

    def __init__(self, config, paths, logger, mailer):
        """
        config: dict -> cestat config (from get_api_config()["cestat"])
        paths: dict -> {
            "data_dir": ...,
            "companies": ...
        }
        """
        self.utils = Helper()
        self.config = config
        self.paths = paths
        self.logger = logger
        self.mailer = mailer
        

    def run(self):
        date = datetime.now()
        timestamp = date.strftime('%d_%H_%M')

        try:
            self.logger.info(f"Running {self.name}")

            # --- load companies ---
            companies = pd.read_csv(self.paths["companies"])["Search Query"].to_list()

            # --- output directory ---
            output_dir = self.utils.create_dir(
                self.paths["data_dir"],
                date.strftime("%Y%m%d")
            )

            # --- init program ---
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
                self.utils.write_df_safe(
                    writer,
                    raw_data,
                    sheet_name="Raw_Data",
                    note_if_empty="No raw data fetched from API"
                )

                self.utils.write_df_safe(
                    writer,
                    filtered_data,
                    sheet_name="Filtered_Data",
                    note_if_empty="No matching companies found"
                )

                self.utils.write_df_safe(
                    writer,
                    final_data,
                    sheet_name="Final_Data",
                    note_if_empty="Final dataset empty after column selection"
                )

            self.logger.info("All data written to Excel.")

            # --- mail ---
            if self.mailer.send_enabled:
                self.logger.info("Sending completion email...")
                self.mailer.end(
                    program=f"{self.name}: {timestamp}",
                    dev=False
                )

            return excel_path

        except Exception as e:
            self.logger.critical(e, exc_info=True)

            if self.mailer.send_enabled:
                self.logger.info("Sending error email...")
                self.mailer.error(
                    program=f"{self.name}: {timestamp}",
                    err=e,
                    dev=True
                )

            raise