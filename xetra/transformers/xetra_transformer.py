"""Xetra ETL Component"""

from typing import NamedTuple
from xetra.common.s3 import S3BucketConnector
import logging
class XetraSourceConfig(NamedTuple):
    """
    Class for source config

    src_first_extract_date: date for extracting the source
    src_columns: source columns names
    src_col_date: column name for date in source
    src_col_isin: column name for isin in source
    src_col_time: column name for time in source
    src_col_start_price: column name for start time in source
    src_col_min_price: column name for min price in source
    src_col_max_price: column name for max price in source
    src_col_traded_vol: column name for traded volume in source

    """
    src_first_extract_date: str
    src_columns: list
    src_col_date: str
    src_col_isin: str
    src_col_time: str
    src_col_start_price: str
    src_col_min_price: str
    src_col_max_price:str

class XetraTargetConfig(NamedTuple):
    """
    Class for target config
    """
    trg_col_isin:str
    trg_col_date: str
    trg_col_op_price: str
    trg_col_close_price:str
    trg_col_min_price: str
    trg_col_max_price: str
    trg_col_daily_trade_vol: str
    trg_col_ch_prev_close: str
    trg_key: str
    trg_key_date_format: str
    trg_format: str

class XetraETL():
    """
    Reads the Xetra data, transform and write to target
    """
    def __init__(self,s3_bucket_src:S3BucketConnector,s3_bucket_trg:S3BucketConnector,meta_key:str,src_args:XetraSourceConfig,trg_args:XetraTargetConfig):
        """
        contructor for XetraTransformer
        :param s3_bucket_src:
        :param s3_bucket_trg:
        :param meta_key:
        :param src_args:
        :param trg_args:
        """
        self._logger = logging.getLogger(__name__)
        self.s3_bucket_src=s3_bucket_src
        self.se_bucket_trg=s3_bucket_trg
        self.meta_key=meta_key
        self.src_args=src_args
        self.trg_args=trg_args
        self.extract_date
        self.extract_date_list
        self.meta_update_list
    def extract(self):
        pass

    def transform_report1(self):
        pass

    def load(self):
        pass

    def etl_report1(self):
        pass

