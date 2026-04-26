import os
from datetime import datetime
import pandas as pd
import shutil

from app.prg_ibbi import IBBI
from app.utils import Helper


class IBBIJob:
    

    def __init__(self, data_dir, config, logger, mailer):
        self.config = config
        self.logger = logger
        self.mailer = mailer
        self.utils = Helper()
        self.data_dir = data_dir
        self.name = "IBBI DATA"

    def run(self):
        date = datetime.now()
        timestamp = date.strftime('%d_%H_%M')

        try:
            self.logger.info(f"Running {self.name}")
            ibbi = IBBI(self.config)

            # --- fetch ---
            current_data = ibbi.get_data()
            self.logger.info("Raw data extracted.")

            output_dir = self.utils.create_dir(self.data_dir, date.strftime("%Y%m%d"))
            reference_file = os.path.join(self.data_dir, "IBBI_REFERENCE.xlsx")

            # --- compare ---
            new_data, old_data, status_report = ibbi.filter_data(
                current_data,
                reference_file
            )

            # --- merge ---
            final_data = {}
            for name in current_data:
                df = pd.concat(
                    [new_data.get(name, pd.DataFrame()),
                    old_data.get(name, pd.DataFrame())],
                    ignore_index=True
                )

                # NEW on top
                if "is_new" in df.columns:
                    df = df.sort_values(by="is_new", ascending=False)

                final_data[name] = df

            # --- save output ---
            excel_path = os.path.join(output_dir, f"IBBI_ALL_{date.strftime('%Y%m%d')}.xlsx")

            with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
                for name, df in final_data.items():
                    self.utils.write_df_safe(writer, df, name[:31])

            self.logger.info(f"Output saved: {excel_path}")

            # --- archive reference---
            if os.path.exists(reference_file):
                archive_path = os.path.join(
                    self.data_dir,
                    f"IBBI_ARCHIVE_{date.strftime('%Y%m%d')}.xlsx"
                )
                shutil.copy(reference_file, archive_path)
                self.logger.info(f"Reference archived: {archive_path}")

            # --- update reference ---
            with pd.ExcelWriter(reference_file, engine="openpyxl", mode="w") as writer:
                for name, df in final_data.items():
                    df.to_excel(writer, sheet_name=name[:31], index=False)

            self.logger.info("Reference updated.")

            # --- alert logic ---
            issues = [k for k, v in status_report.items() if v in ["FAILED", "STALE", "PARTIAL"]]

            if issues:
                self.logger.warning(f"Issues detected: {issues}")

                if self.mailer.send_enabled:
                    self.mailer.send(
                        subject=f"IBBI ALERT: {timestamp}",
                        body_html=f"<p>Issues detected in: {issues}</p>",
                        dev=False
                    )

            if self.mailer.send_enabled:
                self.mailer.send(
                    subject=f"{self.name}: {timestamp}",
                    body_html=f"<p>{self.name} completed.<br>*AUTOMATED MAIL*</p>",
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