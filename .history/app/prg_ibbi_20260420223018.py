# app/ibbi.py
import os
import re
import time
import hashlib
import requests
import pandas as pd
from bs4 import BeautifulSoup #type: ignore
from urllib.parse import quote_plus

from app.logger import get_global_logger
from app.utils import Helper


class IBBI:

    def __init__(self, config):
        
        # print(config)
        self.config = config
        self.session = requests.Session()
        self.logger = get_global_logger()
        self.utils = Helper()
        
        self.sections = self.config["sections"]
        self.base_site = self.config["base_url"]
        self.selectors = self.config["selectors"]
        self.regex_pdf = re.compile(self.config["regex"]["pdf"])


    def extract_rows(self, soup):
        
        table = self.selectors["table"]
        table = soup.select_one(table)
        if not table:
            return []

        rows = table.find_all("tr")
        if len(rows) <= 1:
            return []

        results = []

        for row in rows[1:]:
            cols = [td.get_text(strip=True) for td in row.find_all("td")]
            link_tag = row.find(self.selectors["link"])
            pdf_link = ""

            if link_tag:
                onclick = link_tag.get("onclick", "")
                match = self.regex_pdf.search(onclick)

                if match:
                    pdf_link = self.base_site + match.group(1)

            if cols:
                cols.append(pdf_link)
                results.append(cols)

        return results

    def fetch_pages(self, name):
        page = 1
        all_rows = []

        self.logger.info(f"Fetching Data For: {name}")
        
        s_config = self.sections[name]
        max_pages = s_config.get("pages")  # e.g. 20 or None

        while True:
            
            if max_pages is not None and page > max_pages: #stop if page limit
                break

            url = f"{self.base_site}{s_config['url']}?page={page}"
            self.logger.info(f"{name} → Page {page}")
            print(f"{name} : Page {page}")

            resp = self.session.get(url, verify=False)
            if resp.status_code != 200:
                break

            soup = BeautifulSoup(resp.text, "html.parser")
            rows = self.extract_rows(soup)

            if not rows:
                break

            for r in rows:
                r.insert(0, name)

            all_rows.extend(rows)
            page += 1
            time.sleep(1)

        return all_rows

    def fetch_court_pages(self):
        all_rows = []
        s_config = self.sections["high_courts"]
        param = s_config["param"]
        courts = s_config["courts"]

        for court in courts:
            page = 1

            while True:
                encoded = quote_plus(court)
                url = f"{self.base_site}{s_config['url']}?{param}={encoded}&page={page}"

                self.logger.info(f"{court} → Page {page}")
                print(f"{court} → Page {page}")
                resp = self.session.get(url, verify=False)
                if resp.status_code != 200:
                    break

                soup = BeautifulSoup(resp.text, "html.parser")
                rows = self.extract_rows(soup)

                if not rows:
                    break

                for r in rows:
                    r.insert(0, court)

                all_rows.extend(rows)

                page += 1
                time.sleep(1)

        return all_rows

    def get_data(self):
        results = {}

        for section_name, section_config in self.config["sections"].items():

            if section_config["type"] == "pagination":
                rows = self.fetch_pages(section_name)
                df = pd.DataFrame(rows)

                results[section_name] = df

            elif section_config["type"] == "court_wise":

                rows = self.fetch_court_pages()
                df = pd.DataFrame(rows)
                results["high_courts"] = df

        return results
    
    
    def _generate_hash(self,row):
        values = []

        for v in row:
            if pd.isna(v):
                v = ""
            v = str(v).strip().lower()
            values.append(v)

        row_str = "|".join(values)
        return hashlib.md5(row_str.encode()).hexdigest()


    def filter_data(self, current_data: dict, file_path: str):

        
        old_sheets = pd.read_excel(file_path, sheet_name=None) if os.path.exists(file_path) else {}

        new_data = {}
        old_data = {}

        for section, df in current_data.items():

            if df is None or df.empty:
                new_data[section] = df
                old_data[section] = df
                continue

            df = df.copy()
            df["hash_id"] = df.apply(self._generate_hash, axis=1)
            prev_df = old_sheets.get(section, pd.DataFrame())

            if not prev_df.empty and "hash_id" in prev_df.columns:
                prev_hashes = set(prev_df["hash_id"])
            else:
                prev_hashes = set()

            # --- 3. split ---
            new_df = df[~df["hash_id"].isin(prev_hashes)]
            old_df = df[df["hash_id"].isin(prev_hashes)]

            new_data[section] = new_df
            old_data[section] = old_df

        return new_data, old_data