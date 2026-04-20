import os
from datetime import datetime
import pandas as pd

from app.prg_ngt import NGT
from app.utils import Helper


class NGTJob:
    name = "NGT DATA"

    def __init__(self, data_dir, config, logger, mailer):
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

            ngt = NGT(self.config)

            # --- pipeline ---
            df = ngt.get_data(retries=10)
            self.logger.info(f"Raw data extracted: {len(df)} rows")

            # --- path ---
            output_dir = self.utils.create_dir(self.data_dir, date.strftime("%Y%m%d"))
            excel_path = os.path.join(output_dir, f"NGT_ALL_{timestamp}.xlsx")

            # --- save ---
            with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
                self.utils.write_df_safe(writer, df, "NGT_Data")

            self.logger.info("Data written to Excel.")

            # --- mail ---
            if self.mailer.send_enabled:
                self.mailer.send(
                    subject=f"{self.name}: {timestamp}",
                    body_html=f"<p>{self.name} completed successfully.</p>",
                    attachments=[excel_path],
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