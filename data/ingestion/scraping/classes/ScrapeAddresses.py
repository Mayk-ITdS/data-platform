from pathlib import Path
import pyarrow.parquet as pq
from data.ingestion.scraping.utils.remote_data import CENSUS_URL, HEADERS
import pyogrio
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm
import logging
import time

start = time.perf_counter()
pyogrio.set_gdal_config_options({
        "GDAL_HTTP_USERAGENT": HEADERS["User-Agent"]
    })
logger = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
class ScrapeTigerAddresses:
    TOTAL_ROWS = 37_730_671
    def __init__(self):
        self.tiger_url = CENSUS_URL
        self.source = (
            f"zip+{self.tiger_url}"
            "!tlgpkg_2026_us_addr.gpkg"
        )
        self.output_dir = Path("data/ingestion/scraping/addresses/tiger_data")
    def scrape_addresses(self):
        self.output_dir.mkdir(
            parents=True,
            exist_ok=True
        )
        logger.info("Opening remote TIGER GeoPackage...it may take a while...")
        with pyogrio.open_arrow(
            self.source,
            layer="Address Ranges",
            columns=[
                "ZIP",
                "FULLNAME",
                "FROMHN",
                "TOHN",
                "PARITYL",
                "PARITYR",
                "SIDE",
                "ARID",
            ],
            batch_size=100_000,
            use_pyarrow=True,
        ) as (meta, reader):
            logger.info(
                "TIGER Arrow stream opened in %.2f seconds",
                time.perf_counter() - start
            )
            logger.info(
                "Starting TIGER ingestion: %s expected rows",
                f"{self.TOTAL_ROWS:,}"
            )
            writer = None
            rows_in_file = 0
            file_no = 1

            try:
                with logging_redirect_tqdm():
                    with tqdm(
                        total=self.TOTAL_ROWS,
                        desc="Tiger ingestion",
                        unit="rows",
                        unit_scale=True,

                    ) as progress_bar:
                            for batch in reader:
                                if writer is None:
                                    output_path = (
                                            self.output_dir /
                                            f"tiger_addresses_{file_no:04d}.parquet"
                                    )
                                    writer = pq.ParquetWriter(
                                        output_path,
                                        batch.schema,
                                        compression="snappy"
                                    )
                                writer.write_batch(batch)
                                rows_in_file += batch.num_rows
                                progress_bar.update(batch.num_rows)
                                if rows_in_file >= 1_000_000:
                                    writer.close()
                                    logger.info(
                                        "Written %s with %s rows",
                                        output_path.name,
                                        f"{rows_in_file:,}"
                                    )
                                    writer = None
                                    rows_in_file = 0
                                    file_no += 1
            finally:
                if writer is not None:
                    writer.close()
                    logger.info(
                        "Written final file %s with %s rows",
                        output_path.name,
                        f"{rows_in_file:,}"
                    )
            logger.info("TIGER ingestion completed successfully")

if __name__ == "__main__":
    scraper = ScrapeTigerAddresses()
    scraper.scrape_addresses()

