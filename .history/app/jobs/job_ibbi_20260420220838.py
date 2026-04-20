import os
from datetime import datetime
import pandas as pd

from app.prg_ibbi import IBBI
from app.utils import Helper


class IBBIJob:
    name = "IBBI DATA"

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
            self.logger.info(f"Running {name}")
            ibbi = IBBI(self.config)

            # --- pipeline ---
            current_data = ibbi.get_data()
            self.logger.info("Raw data extracted.")

            
            output_dir = self.utils.create_dir(self.data_dir, date.strftime("%Y%m%d"))
            reference_file = os.path.join(self.data_dir, "IBBI_REFERENCE.xlsx")

            # --- filter ---
            new_data, old_data = ibbi.filter_data(
                current_data, 
                reference_file
            )

            # --- merge ---
            final_data = {}
            for name in current_data:
                final_df = pd.concat(
                    [new_data.get(name, pd.DataFrame()),
                     old_data.get(name, pd.DataFrame())],
                    ignore_index=True
                )
                final_data[name] = final_df

            # --- save (same style as CESTAT) ---
            excel_path = os.path.join(output_dir, f"IBBI_ALL_{timestamp}.xlsx")

            with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
                for name, df in final_data.items():
                    self.utils.write_df_safe(writer, df, name[:31])

            self.logger.info("All data written to Excel.")

            # --- mail ---
            if self.mailer.send_enabled:
                self.mailer.send(
                    subject=f"{name}: {timestamp}",
                    body_html=f"<p>{name} completed.</p>",
                    attachments=[excel_path],   # 👈 THIS is the key line
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