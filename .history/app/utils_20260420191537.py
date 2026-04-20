import os, re, json, string, shutil, json5, random
import fitz #type:ignore
from datetime import datetime, timedelta
from collections import defaultdict
import pandas as pd #type:ignore
from typing import List
from uuid import uuid4
from app.logger import get_global_logger

class Helper:
    def __init__(self):
        self.logger = get_global_logger()
    



    @staticmethod
    def generate_uid():
        return uuid4().hex

    def generate_dates(self,date_format, minus_days=1):
        today = datetime.today()
        to_str = today.strftime(date_format)
        yesterday = today - timedelta(days=minus_days)
        from_str = yesterday.strftime(date_format)
        return from_str, to_str
        # return "20-01-2026","21-01-2026"
    
    
    #JSON UN/LOAD
    @staticmethod
    def create_dir(base_path, *folders):
        dir_path = os.path.join(base_path, *folders)
        os.makedirs(dir_path, exist_ok=True)
        return dir_path
    
    @staticmethod
    def save_json(data: dict, path: str, indent: int = 2):
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=indent)

    @staticmethod
    def load_json(file_path: str):
        if not os.path.exists(file_path):
            return
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)
        
    @staticmethod
    def save_json5(data: dict, path: str, indent: int = 2):
        with open(path, "w", encoding="utf-8") as f:
            json5.dump(data, f, indent=indent)

    @staticmethod
    def load_json5(file_path: str):
        if not os.path.exists(file_path):
            return
        with open(file_path, "r", encoding="utf-8") as f:
            return json5.load(f)
        
    @staticmethod
    def load_json_as_string(path: str, indent: int = None) -> str:
        with open(path, "r", encoding="utf-8") as f:
            return json.dumps(json.load(f), indent=indent, ensure_ascii=False)

    @staticmethod
    def load_json5_as_string(path: str, indent: int = None) -> str:
        with open(path, "r", encoding="utf-8") as f:
            return json5.dumps(json5.load(f), indent=indent)

    
    #WRITE TEXT
    @staticmethod
    def save_text(data,path:str):
        if not data:
            print("Empty Data")
            return
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, 'a', encoding='utf-8') as f:
            if isinstance(data,dict):
                f.writelines(f"{k}:{v}\n" for k,v in data.items())
            elif isinstance(data,list):
                f.writelines(f"{k}\n" for k in data)
            elif isinstance(data,str):
                f.writelines(data)
            else: print("Invalid type")
           
    def debug_save(pdf_bytes: bytes, filename="debug.pdf"):
        """Save in-memory PDF bytes to disk for debugging purposes."""
        with open(filename, "wb") as f:
            f.write(pdf_bytes)
        print(f"[debug] PDF saved to {filename}")
    
    def _clean_leading_noise(self,text: str) -> str:
        if not isinstance(text,str):
            return text
        return re.sub(r'^[\s\n\r\t\\:;\-–—•|]+', '', text).strip()
    
    def _normalize_key(self,text: str) -> str:
        if not isinstance(text,str):
            return text
        text = re.sub(r"[^\w\s\.]", "", text)
        text = re.sub(r"\s+", "_", text)
        return text.strip().lower()
    
    def _normalize_key_to_alnum_underscore(self, text: str) -> str:
        if not isinstance(text, str):
            return text
        text = text.strip().lower()
        text = re.sub(r"[^\w]", "_", text)
        text = re.sub(r"__+", "_", text)
        return text.strip("_")

    def _remove_duplicates(self,text):
        if not text:
            return text
        seen = []
        text = text.split(" ")
        for word in text:
            word = word.lower().strip()
            if word not in seen:
                seen.append(word)
        return " ".join(seen)

    #match type
    def is_numeric(self,text):
        return bool(re.fullmatch(r'[+-]?(\d+(\.\d*)?|\.\d+)', text))

    def is_alphanumeric(self,text):
        return bool(re.fullmatch(r'[A-Za-z0-9]+', text))

    def is_alpha(self,text):
        return bool(re.fullmatch(r'[A-Za-z]+', text))
        
    def _remove_non_word_space_chars(self,text:str)->str:
        if not isinstance(text,str):
            return text
        text = re.sub("[^\\w\\s]", "", text).strip()
        return text
    
    def _normalize_whitespace(self,text:str)->str:
        if not isinstance(text,str):
            return text
        return re.sub(r"\s+", " ", text).strip()
    
    def _normalize_date(self,text:str)->str:
        if not isinstance(text,str):
            return text
        text = re.sub(r"[^A-Za-z0-9\s\.\/\,\-\\]+"," ",text).strip()
        return self._normalize_whitespace(text)
    
    def _normalize_ascii(self, text: str) -> str:
        if not isinstance(text, str):
            return text
        text = re.sub(r"[^\x20-\x7E]+", " ", text)
        return re.sub(r"\s+", " ", text).strip()

    def _normalize_alphanumeric(self, text: str) -> str:
        if not isinstance(text,str):
            return text
        text = re.sub(r"[^a-zA-Z0-9]+", " ", str(text))
        return re.sub(r"\s+", " ", text).strip().lower()
    
    def _normalize_alpha(self, text: str) -> str:
        if not isinstance(text,str):
            return text
        text = re.sub(r"[^a-zA-Z]+", " ", str(text))
        return re.sub(r"\s+", " ", text).strip().lower()

    def _normalize_numeric(self, text: str) -> str:
        if not isinstance(text,str):
            return text
        text = re.sub(r"[^0-9\.]+", " ", str(text))
        return re.sub(r"\s+", " ", text).strip().lower()
    
    
    def write_df_safe(self, writer, df, sheet_name, note_if_empty=None):
        if isinstance(df, pd.DataFrame) and not df.empty:
            df.to_excel(writer, sheet_name=sheet_name, index=False)
        else:
            placeholder = pd.DataFrame({
                "Info": [note_if_empty or "No data available"]
            })
            placeholder.to_excel(writer, sheet_name=sheet_name, index=False)
    
    #PYMUPDF/FITZ HELPERS
    
    @staticmethod
    def get_pdf_text(path:str):
    
        doc = fitz.open(path)
        text_data = {}
        for pgn in range(doc.page_count):
            page = doc[pgn]
            text = page.get_text("text")
            text = text.encode('utf-8', 'ignore').decode('utf-8')
            data = text.split('\n')
            text_data [pgn] = data
        return text_data
    
    @staticmethod
    def get_clipped_data(input:str, bboxes:list[set]):
    
        document = fitz.open(input)
        final_list = []
        
        for pgn in range(document.page_count):
            page = document[pgn]

            blocks = []
            for bbox in bboxes:
                blocks.extend(page.get_text('dict', clip = bbox)['blocks']) #get all blocks
            
            filtered_blocks = [block for block in blocks if block['type']== 0 and 'lines' in block]
            sorted_blocks = sorted(filtered_blocks, key= lambda x: (x['bbox'][1], x['bbox'][0]))
            
            final_list.append({
            "pgn": pgn,
            "block": sorted_blocks
            })
            
            
        document.close()
        return final_list
    
    @staticmethod
    def get_all_pdf_data(path:str):
    
        doc = fitz.open(path)
        count = doc.page_count
        all_blocks = list()

        for pgn in range(count):
            page = doc[pgn]
            
            blocks = page.get_text('dict')['blocks']
            for line in blocks["lines"]:
                line.update({
                    "uid": Helper.generate_uid()
                })
            images = page.get_images()
            filtered_blocks = [block for block in blocks if block['type']== 0]
            sorted_blocks = sorted(filtered_blocks, key=lambda x: x['bbox'][1])
            all_blocks.append({
                "pgn":pgn,
                "blocks":sorted_blocks,
                "images": images
            })
            
            #draw lines
            
            lines = fitz.Rect()
            
        doc.close()
        
        return all_blocks
    
    @staticmethod
    def draw_lines_on_pdf(pdf_path: str, lines: list, rects:list, pages:list,output_path: str):
        """Open the pdf , draw lines on the mentioned pages
        Args:pdf_path(str) , output_pdf_path (str)
        Returns: nothing, a new pdf created"""
        doc = fitz.open(pdf_path)
        for page_number, page in enumerate(doc, start=1):
            
            height = page.rect.height
            width  = page.rect.width
            if page_number in pages:
                
                # Start drawing on the page
                for line in lines:
                    start, end = line
                    x1, y1 = start
                    x2, y2 = end
                    page.draw_line((x1, y1), (x2, y2))
                    #page.draw_rect((0,20,250,1000))
                
                # Start drawing on the page
                for rec in rects:
                    x0, y0, x1, y1 = rec  
                    rect = fitz.Rect(x0, y0, x1, height) 

                    # Set the rectangle's fill and stroke color
                    shape = page.new_shape()
                    shape.draw_rect(rect)  
                    shape.finish(color=(1,0,0), fill=(1, 0.75, 0.8), width=0.8, fill_opacity = .3)  # Pink fill, no border color
                    shape.commit()
            


        doc.save(output_path)
        print(f"Modified PDF saved to: {output_path}")
        #open the file on screen
        import subprocess
        subprocess.Popen([output_path],shell=True)

    @staticmethod
    def draw_boundaries_on_lines(pdf_path: str):
        doc = fitz.open(pdf_path)

        for page in doc:
            blocks = page.get_text("dict")["blocks"]

            for block in blocks:
                if block.get("lines"):
                    for line in block["lines"]:
                        bbox = line["bbox"]

                        page.draw_rect(
                            bbox,
                            color=(1, 0, 0),  # red
                            width=1.5,
                            overlay=True
                        )

        output_path = pdf_path.replace('.pdf', '_line_hltd.pdf')
        doc.save(output_path)
        doc.close()
        return output_path
    

    @staticmethod
    def fill_boundaries_on_lines(pdf_path: str):

        doc = fitz.open(pdf_path)
        for page in doc:
            shape = page.new_shape() 
            blocks = page.get_text("dict")["blocks"]

            for block in blocks:
                if block.get("lines"):
                    for line in block["lines"]:
                        bbox = fitz.Rect(line["bbox"])
                        shape.draw_rect(bbox)

            shape.finish(
                fill=(0.8, 1, 0.2),      # light red fill
                stroke_opacity=0,        # no border
                fill_opacity=0.7        # transparency
            )

            shape.commit(overlay=True)   # MUST

        output_path = pdf_path.replace(
            '.pdf',
            '_line_filled.pdf'
        )

        doc.save(output_path)
        doc.close()

        return output_path

        
    @staticmethod
    def draw_boundaries_on_pdf(pdf_path: str):
        doc = fitz.open(pdf_path)

        for page in doc:
            blocks = page.get_text("dict")["blocks"]

            for block in blocks:
                bbox = block.get("bbox")

                if bbox:
                    page.draw_rect(
                        bbox,
                        color=(1.0, 0.647, 0.0),
                        width=1.5,
                        overlay=True
                    )

        output_path = pdf_path.replace('.pdf', '_block_hltd.pdf')
        doc.save(output_path)
        doc.close()
        return output_path
        
    @staticmethod
    def draw_span_boundaries(pdf_path: str):
        doc = fitz.open(pdf_path)

        for page in doc:
            blocks = page.get_text("dict")["blocks"]

            for block in blocks:
                for line in block.get("lines", []):
                    for span in line.get("spans", []):

                        bbox = span["bbox"]

                        page.draw_rect(
                            bbox,
                            color=(0, 1, 0),  # green
                            width=1,
                            overlay=True
                        )

        output_path = pdf_path.replace('.pdf', '_span_hltd.pdf')
        doc.save(output_path)
        doc.close()
        return output_path

    @staticmethod
    def draw_bboxes_on_pdf(pdf_path:str, bbox:tuple):
        
        doc = fitz.open(pdf_path)
        for page in doc:
            page.draw_rect(bbox, color = (1.0,0,1.0), width = 1.5, overlay = False)

        output_path = pdf_path.replace('.pdf', '_bbox_hltd.pdf')
        doc.save(output_path)
        doc.close()
        return output_path
    
    @staticmethod
    def draw_pink_lines(pdf_path:str, gap:int=5):

        doc = fitz.open(pdf_path)
        for page in doc:

            page_width  = page.rect.width
            page_height = page.rect.height

            shape = page.new_shape()

            y = 0
            while y <= page_height:

                shape.draw_line(fitz.Point(0, y),fitz.Point(page_width, y))

                y += gap   # EXACT STEP

            shape.finish(
                color=(1, 0, 0.5),    # pink
                width=0.2
            )

            shape.commit(overlay=True)

        
        output_path = pdf_path.replace('.pdf', '_x_axis_line.pdf')
        doc.saveIncr()
        doc.save(output_path)
        doc.close()
        return output_path


    @staticmethod
    def mask_outside_bboxes(input_pdf, bboxes):
        doc = fitz.open(input_pdf)

        for page in doc:
            page_rect = page.rect

            for bbox in bboxes:
                x0, y0, x1, y1 = bbox

                # Top
                if y0 > page_rect.y0:
                    page.add_redact_annot(
                        fitz.Rect(page_rect.x0, page_rect.y0, page_rect.x1, y0),
                        fill=(1, 1, 1)
                    )

                # Bottom
                if y1 < page_rect.y1:
                    page.add_redact_annot(
                        fitz.Rect(page_rect.x0, y1, page_rect.x1, page_rect.y1),
                        fill=(1, 1, 1)
                    )

                # Left
                if x0 > page_rect.x0:
                    page.add_redact_annot(
                        fitz.Rect(page_rect.x0, y0, x0, y1),
                        fill=(1, 1, 1)
                    )

                # Right
                if x1 < page_rect.x1:
                    page.add_redact_annot(
                        fitz.Rect(x1, y0, page_rect.x1, y1),
                        fill=(1, 1, 1)
                    )

            # Apply AFTER all annots are added
            page.apply_redactions()


        output_path = input_pdf.replace('.pdf', '_bbox_mask.pdf')
        doc.save(output_path)
        doc.close()
        return output_path
    
    
    
    import random
