import re, requests, time
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import pandas as pd

from app.logger import get_global_logger
from app.utils import Helper

class CESTAT:
    
    def __init__(self, config:None):
        self.config = config
        self.logger = get_global_logger()
        self.session = requests.Session()
        self.utils = Helper()
        
        self.base_site = self.config["base_url"]
        self.selectors = self.config["selectors"]
        self.headers = self.config["headers"]

   
    def normalize_name(self,text: str) -> str:
        text = "" if text is None else str(text)
        
        text = re.sub(r"\b(limted|limlited|limited|ltd)\.?\b", "ltd", text, flags=re.IGNORECASE)
        text = re.sub(r"\b(private|pvt)\.?\b", "pvt", text, flags=re.IGNORECASE)
        text = re.sub(r"\bco\.?\b", "co", text, flags=re.IGNORECASE)

        text = text.replace("&amp;", "&").replace("&amp;amp;", "&")
        text = re.sub(r"\band\b", "&", text, flags=re.IGNORECASE)

        # whitespace cleanup
        text = re.sub(r"\s+", " ", text).strip()
        return text.lower()

    #get data
    def get_token(self, base_url):
        self.logger.info("Getting Token...")

        resp = self.session.get(self.base_site, verify=False)
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")
        token_tag = soup.find("input", {"name": "csrf_token"})

        if not token_tag or not token_tag.get("value"):
            raise RuntimeError("CSRF token not found")

        csrf_token = token_tag["value"]
        self.logger.info(f"Token: {csrf_token}")
        return csrf_token

    def get_payload(
        self,
        payload,
        csrf_token, 
        bench,
        order_type,
        date_format = "%d-%m-%Y"
    ):
        
        from_date, to_date = self.utils.generate_dates(date_format)
        payload.update({
            "csrf_token":csrf_token,
            "bench":bench,
            "from":from_date,
            "to":to_date,
            "order_type":order_type,
        })
        return payload

    def get_data(self):

        results = []        
        payload = self.config["api_payload"]
        api_url = self.config["api_url"]
        home_url = self.config["home_url"]
        
        benches = self.config["benches"]
        order_type = self.config["order_type"]
        date_format = self.config["date_format"]
        fieldnames = self.config["field_names"]
        
        try:
            for order_name, order in order_type.items():
        
                csrf_token = self.get_token(self.config["base_url"])  # may raise

                for city, bench in benches.items():
                    time.sleep(5)
                    payload = self.get_payload(payload,csrf_token, bench, order, date_format= date_format)

                    self.logger.info(f"[POST] bench={city}, order_type={order_name} -> {api_url}")
                    resp =  self.session.post(api_url, data=payload, verify=False, timeout=30)
                    resp.raise_for_status()

                    data = resp.json().get("data", [])
                    if isinstance(data, dict) or not data:
                        self.logger.warning(f"No data for bench={bench} (city={city}), order_type={order_name}")
                        continue

                    for row in data:
                        # response structure: [serial, case_no, parties_html, date, link_html]
                        serial, case_no, parties, date = row[0], row[1], row[2].replace("<br>", " "), row[3]
                        
                        link_html = row[4]
                        soup = BeautifulSoup(link_html or "", "html.parser")
                        link_tag = soup.find("a")
                        href = link_tag.get("href") if link_tag else None
                        if href:
                            href_clean = href.replace("./", "")
                            pdf_url = urljoin(home_url.rstrip("/") + "/", href_clean)
                        else:
                            pdf_url = None
                            
                        results.append(
                            dict(zip(
                                fieldnames,
                                [bench, city, order_name, serial, case_no, parties, date, pdf_url]
                            ))
                        )

                    self.logger.info(f"[OK] Extracted {len(results)} rows so far (bench={city})")
                    time.sleep(5)

            df = pd.DataFrame(results, columns=fieldnames)
            self.logger.info(f"Data Fetched, Rows: {len(df)}")
            return df

        except Exception as e:
            self.logger.error(f"Error in get_data: {e}", exc_info=True)
            return pd.DataFrame(columns=fieldnames)

    def filter_data(self, df: pd.DataFrame, companies, filter_on = "") -> pd.DataFrame:
        df = df.copy()
        
        # select only limited|ltd
        ltd_suffix = re.compile(r"\b(?:ltd|limited|limlited|limted)\.?\b", re.IGNORECASE)
        m = df[filter_on].apply(lambda x: bool(ltd_suffix.search(x)))
        df = df[m]
        
        # company name
        search_company = f"\\b({'|'.join([str(c) for c in companies])})\\b"
        df["norm"] = df[filter_on].astype(str).apply(self.normalize_name)
        
        m = df["norm"].str.contains(search_company, case=False, na=False, regex=True)
        matched_df = df[m]
        unmatched_df = df[~m]
        
        return matched_df
    
    
