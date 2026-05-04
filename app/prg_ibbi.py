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

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
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
        
        self.logger.info(f"Fetching Data For HIGH COURTS")
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
                    r.insert(0,"high_courts")
                
                all_rows.extend(rows)

                page += 1
                time.sleep(1)

        return all_rows
    
    def fetch_type_pages(self, type_pages:str):
        
        self.logger.info(f"Fetching Data For IBBI/NCLT")
        all_rows = []
        s_config = self.sections[type_pages]
        param = s_config["param"]
        types = s_config["types"]
        
        total_pages = int(s_config["pages"])

        for court in types.keys():
            print(f"Fething for : {court}")
            page = 1

            while True:
                encoded = quote_plus(court)
                url = f"{self.base_site}{s_config['url']}?{param}={encoded}&page={page}"

                self.logger.info(f"{types[court]} → Page {page}")
                print(f"{types[court]} → Page {page}")
                resp = self.session.get(url, verify=False)
                if resp.status_code != 200:
                    break

                soup = BeautifulSoup(resp.text, "html.parser")
                rows = self.extract_rows(soup)

                if not rows:
                    break

                for r in rows:
                    category_name = types[court]
                    r.insert(0, category_name)
                    r.insert(0, type_pages)

                all_rows.extend(rows)

                
                #page exit loop condition
                page += 1
                time.sleep(1)
                if page > total_pages:
                    break

        return all_rows

    def get_data(self):
        results = {}

        for section_name, section_config in self.config["sections"].items():

            if section_config["type"] == "pagination":
                rows = self.fetch_pages(section_name)
                df = pd.DataFrame(rows, columns=section_config["columns"])
                results[section_name] = df

            elif section_config["type"] == "court_wise":
                rows = self.fetch_court_pages()
                df = pd.DataFrame(rows, columns=section_config["columns"])
                results["high_courts"] = df

            elif section_config["type"] == "type_page":
                rows = self.fetch_type_pages(section_name)
                df = pd.DataFrame(rows, columns=section_config["columns"])
                results[section_name] = df

        return results
    
    
    # def prepare_df(self, df):
    #     df = df.copy()
    #     df.columns = df.columns.str.strip().str.lower()

    #     # clean directly on real columns
    #     df["case"] = df.get("case", "").astype(str).str.strip().str.lower()
    #     df["remark"] = df.get("remark", "").astype(str).str.strip().str.lower()
    #     df["date"] = pd.to_datetime(df.get("date"), errors="coerce")

    #     # keep only stable columns
    #     # df = df[["date", "case", "remark", "pdf_link"]]

    #     # dedupe
    #     df = df.drop_duplicates(subset=["pdf_link"])

    #     # hash using actual fields
    #     df["hash_id"] = df.apply(
    #         lambda r: hashlib.md5(
    #             f"{r['date']}|{r['case']}|{r['remark']}".encode()
    #         ).hexdigest(),
    #         axis=1
    #     )

    #     return df
    def prepare_df(self, df):
        df = df.copy()
        df.columns = df.columns.str.strip().str.lower()

        # clean directly on real columns
        df["case"] = df.get("case", "").astype(str).str.strip().str.lower()
        df["remark"] = df.get("remark", "").astype(str).str.strip().str.lower()
        df["date"] = pd.to_datetime(df.get("date"), errors="coerce")

        # keep only stable columns
        # df = df[["date", "case", "remark", "pdf_link"]]

        # dedupe
        df = df.drop_duplicates(subset=["pdf_link"])

        # hash using actual fields
        df["hash_id"] = df.apply(
            lambda r: hashlib.md5(
                f"{r['date']}{r['case']}|{r['pdf_link']}".encode() #|{r['case']}|{r['remark']}
            ).hexdigest(),
            axis=1
        )

        return df

    def filter_data(self, data_dict, ref_path):

        df_old = pd.read_excel(ref_path)
        prev_map = df_old.set_index("pdf_link")["hash_id"].to_dict()

        final_results = {}
        compared_results = {}

        for section_name, df in data_dict.items():

            print(f"Processing: {section_name}")

            df_new = self.prepare_df(df)

            def classify(row):
                key = row["pdf_link"]

                if key not in prev_map:
                    return "NEW"
                elif prev_map[key] != row["hash_id"]:
                    return "UPDATED"
                return "UNCHANGED"

            df_new["status"] = df_new.apply(classify, axis=1)

            # sort by date 
            df_new = df_new.sort_values(by="date", ascending=False)

            final_df = df_new[df_new["status"].isin(["NEW", "UPDATED"])]
            compared_df = df_new

            final_results[section_name] = final_df
            compared_results[section_name] = compared_df

        return final_results, compared_results
    
    # def filter_data(self, current_data: dict, file_path: str):

    #     old_sheets = pd.read_excel(file_path, sheet_name=None, engine="openpyxl") if os.path.exists(file_path) else {}
    #     old_sheets = {k.lower(): v for k, v in old_sheets.items()}

    #     new_data = {}
    #     updated_data = {}
    #     prepared_data = {}   # reference

    #     for section, df in current_data.items():

    #         section_key = section.lower()
    #         prev_df = old_sheets.get(section_key, pd.DataFrame())

    #         if df is None or df.empty:
    #             new_data[section] = pd.DataFrame()
    #             updated_data[section] = pd.DataFrame()
    #             prepared_data[section] = pd.DataFrame()
    #             continue

    #         df = df.copy()
    #         df.columns = self.columns

    #         # normalize
    #         df["title"] = df["title"].astype(str).str.strip().str.lower()
    #         df["category"] = df["category"].astype(str).str.strip().str.lower()
    #         df["date"] = pd.to_datetime(df["date"], errors="coerce")
    #         df = df.drop_duplicates(subset=["pdf_link"])


    #         def make_hash(row):
    #             date_val = row["date"].strftime("%Y-%m-%d") if pd.notna(row["date"]) else ""

    #             return hashlib.md5(
    #                 "|".join([
    #                     date_val,
    #                     row["title"],
    #                     row["category"]
    #                 ]).encode()
    #             ).hexdigest()

    #         df["hash_id"] = df.apply(make_hash, axis=1)


    #         if not prev_df.empty and "pdf_link" in prev_df.columns:
    #             prev_map = prev_df.set_index("pdf_link")["hash_id"].to_dict()
    #         else:
    #             prev_map = {}

    #         def classify(row):
    #             key = row["pdf_link"]
    #             if key not in prev_map:
    #                 return "NEW"
    #             elif prev_map[key] != row["hash_id"]:
    #                 return "UPDATED"
    #             return "UNCHANGED"

    #         df["record_status"] = df.apply(classify, axis=1)

    #         new_data[section] = df[df["record_status"] == "NEW"]
    #         updated_data[section] = df[df["record_status"] == "UPDATED"]

    #         prepared_data[section] = df   #refernce

    #     return new_data, updated_data, prepared_data