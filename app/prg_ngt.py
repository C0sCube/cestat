import re, time
import requests
from bs4 import BeautifulSoup
import pandas as pd
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from app.logger import get_global_logger
from app.utils import Helper
from app.prg_captcha import CaptchaSolver

class NGT:

    def __init__(self, config:None):
        self.config = config
        self.session = requests.Session()
        self.logger = get_global_logger()
        self.utils = Helper()
        
        self.base_site = self.config["base_url"]
        self.data_site = self.config["data_url"]
        self.captcha_url = self.config["captcha_url"]
        self.selectors = self.config["selectors"]
        self.headers = self.config["headers"]
        
        
        self.model_path = r"C:\Users\kaustubh.keny\Projects\OFFICE PROJECTS\CESTAT\docs\captcha_model.pth" 
        self.solver = CaptchaSolver(self.model_path)
        
    def normalize_name(self, text: str) -> str:
        text = "" if text is None else str(text)

        text = re.sub(r"\b(limted|limlited|limited|ltd)\.?\b", "ltd", text, flags=re.IGNORECASE)
        text = re.sub(r"\b(private|pvt)\.?\b", "pvt", text, flags=re.IGNORECASE)
        text = re.sub(r"\bco\.?\b", "co", text, flags=re.IGNORECASE)

        text = text.replace("&amp;", "&").replace("&amp;amp;", "&")
        text = re.sub(r"\band\b", "&", text, flags=re.IGNORECASE)

        text = re.sub(r"\s+", " ", text).strip()
        return text.lower()

    def get_payload(self, payload, captcha, zone_type, order_by, date_format):
        from_dt, to_dt = self.utils.generate_dates(date_format, minus_days=1)

        from_dt = "13/04/2026"
        to_dt = "14/04/2026"
        
        payload.update({
            "zone_type": str(zone_type),
            "from_date": from_dt,
            "to_date": to_dt,
            "order_by": str(order_by),
            "captcha_input": captcha
        })

        return payload

    def get_captcha(self):
        
        resp = self.session.get(self.captcha_url, headers= self.headers, verify=False)
        resp.raise_for_status()

        with open("captcha.png", "wb") as f:
            f.write(resp.content)

        time.sleep(1)
        # self.logger.info("Captcha saved as captcha.png")
        # return input("Enter captcha: ")
        png_path = "captcha.png"
        perd = self.solver.predict(png_path)
        print(f"Predict: {perd}")
        # return "888888"
        return perd
    

    def extract_rows(self, soup):
        table = soup.find("table")

        if not table:
            self.logger.warning("No table found")
            return []

        rows = table.find_all("tr")
        if len(rows) < 2:
            return []

        headers = [th.get_text(strip=True) for th in rows[0].find_all(["th", "td"])]

        results = []

        for row in rows[1:]:
            cols = row.find_all("td")
            if not cols:
                continue

            row_data = [c.get_text(strip=True) for c in cols]

            link_tag = row.find("a")
            link = None

            if link_tag and link_tag.get("href"):
                link = link_tag["href"]
                if not link.startswith("http"):
                    link = self.base_site + link

            row_dict = dict(zip(headers, row_data))
            row_dict["link"] = link

            results.append(row_dict)

        return results

    def get_total_pages(self, soup):
        pages = []
        
        pagination = self.selectors.get("pagination","ul a")
        for a in soup.select(pagination):
            href = a.get("href")
            if not href:
                continue

            match = re.search(r"page=(\d+)", href)
            if match:
                pages.append(int(match.group(1)))

        return max(pages) if pages else 1

    def fetch_pages(self, payload):
        all_results = []

        headers = self.config.get("headers",{"User-Agent": "Mozilla/5.0"})

        payload_page = payload.copy()
        payload_page["page"] = 1
        
        print(payload)

        print("Fetching Data Right Now.")
        resp = self.session.post(self.data_site, data=payload_page, verify=False) #headers removed
        resp.raise_for_status()

        if "Invalid Captcha" in resp.text:
            raise RuntimeError("Captcha failed")

        soup = BeautifulSoup(resp.text, "html.parser")

        page_results = self.extract_rows(soup)
        all_results.extend(page_results)

        self.logger.info(f"Page 1 → {len(page_results)} rows")

        total_pages = self.get_total_pages(soup)
        self.logger.info(f"Total pages: {total_pages}")

        for page in range(2, total_pages + 1):
            payload_page = payload.copy()
            payload_page["page"] = page

            self.logger.info(f"Fetching page {page}")

            resp = self.session.post(self.data_site, data=payload_page,verify=False) #headers removed
            resp.raise_for_status()

            soup = BeautifulSoup(resp.text, "html.parser")

            page_results = self.extract_rows(soup)
            all_results.extend(page_results)

            self.logger.info(f"Page {page} → {len(page_results)} rows")

        return all_results

    def get_data(self, retries=10):

        benches = self.config["benches"]
        order_types = self.config["order_type"]
        base_payload = self.config["api_payload"]
        date_format = self.config.get("date_format", "%d/%m/%Y")

        for attempt in range(1, retries + 1):
            try:
                self.logger.info(f"NGT get_data attempt {attempt}/{retries}")

                all_results = []

                captcha = self.get_captcha()

                for order_name, order_val in order_types.items():
                    for bench_name, bench_val in benches.items():

                        payload = self.get_payload(
                            base_payload.copy(),
                            captcha,
                            bench_val,
                            order_val,
                            date_format
                        )

                        self.logger.info(
                            f"[POST] bench={bench_name}, order={order_name}"
                        )

                        results = self.fetch_pages(payload)

                        for r in results:
                            r["Zonal Bench"] = bench_name
                            r["Order Type"] = order_name

                        all_results.extend(results)
                        time.sleep(5)

                df = pd.DataFrame(all_results)
                self.logger.info(f"NGT Data fetched: {len(df)} rows")

                return df 
            
            except Exception as e:
                self.logger.error(
                    f"Attempt {attempt} failed: {e}",
                    exc_info=True
                )

                if attempt == retries:
                    self.logger.error("All retries exhausted. Returning empty DataFrame.")
                    return pd.DataFrame()

                # backoff before retry
                sleep_time = min(5 * attempt, 30)
                time.sleep(sleep_time)
