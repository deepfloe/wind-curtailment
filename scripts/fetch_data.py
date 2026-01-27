import logging

import pandas as pd
from sqlalchemy import create_engine
from pathlib import Path
import sqlite3


import sys
from sys import path

path.append(r'C:\wind-curtailment')

from lib.constants import DATA_DIR
from lib.data.fetch_boa_data import run_boa
from lib.db_utils import drop_and_initialize_tables

engine = sqlite3.connect("phys_data.db")
df_bm_units = pd.read_csv(DATA_DIR / "BMU.csv", header=0)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


if __name__ == "__main__":
    logger.info("Get BOA data")
    start = pd.Timestamp("2022-01-01")
    end = pd.Timestamp("2022-01-02")
    path_to_db = f"data/phys_data_{start.isoformat()}_{end.isoformat()}.db".replace(":",'-')
    drop_and_initialize_tables(path_to_db)
    wind_units = df_bm_units[df_bm_units["FUEL TYPE"] == "WIND"]["SETT_BMU_ID"].unique()

    run_boa(start_date=start, end_date=end, units=wind_units, chunk_size_in_days=1)
