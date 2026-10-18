from io import StringIO,TextIOWrapper
import pandas as pd
import requests
from io import BytesIO
from zipfile import ZipFile
from pathlib import Path



class ScrapePages:
    def __init__(self,pages,headers,start_page=1):
        self.pages = pages
        self.page = start_page
        self.headers = headers
        self.all_names = None
    def __iter__(self):

        return self
    def fetch_names(self,url):
        response = requests.get(
            url,
            headers=self.headers,
            timeout=30,
        )
        response.raise_for_status()
        frames = []
        with ZipFile(BytesIO(response.content)) as names_file:
            for file_name in names_file.namelist():
                if not file_name.upper().endswith(".TXT"):
                    continue
                with names_file.open(file_name) as binary_files:
                    with TextIOWrapper(binary_files,encoding='utf-8') as file:
                        df = pd.read_csv(
                            file,
                            header=None,
                            names=['state','sex','year','first_name','count']
                        )
                        frames.append(df)
        self.all_names =  pd.concat(frames,ignore_index=True)

        output_dir = Path("./data/ingestion/scraping/names/")

        output_dir.mkdir(parents=True,exist_ok=True)

        self.all_names.to_parquet(
            output_dir / 'ssa_first_names.parquet',
            index=False
        )

        return self

    def __next__(self):
        if self.page > self.pages:
            raise StopIteration
        response = requests.get(
            BASE_URL,
            params={"page": self.page},
            headers=self.headers,
            timeout=30,
        )
        response.raise_for_status()

        tables = pd.read_html(StringIO(response.text))
        current_page = tables[0]
        self.page += 1

        return current_page

if __name__ == "__main__":

    iterator = ScrapePages(1500,HEADERS,1)
    last_names = []
    for page in iterator:
        last_names.append(page)

    last_names = pd.concat(last_names,ignore_index=True)

    output_path = Path("./data/ingestion/scraping/names/")
    output_path.mkdir(parents=True,exist_ok=True)
    last_names.to_parquet(
        output_path / 'census_last_names.parquet',
        index=False
    )
    names = iterator.fetch_names(SSA_URL).all_names



