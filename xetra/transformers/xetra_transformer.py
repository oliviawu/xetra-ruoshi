"""Xetra ETL Component"""
from datetime import datetime
from typing import NamedTuple
from xetra.common.s3 import S3BucketConnector
import logging
import pandas as pd
from xetra.common.meta_process import MetaProcess
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
    src_col_traded_vol:str

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
        self.s3_bucket_trg=s3_bucket_trg
        self.meta_key=meta_key
        self.src_args=src_args
        self.trg_args=trg_args
        self.extract_date,self.extract_date_list=MetaProcess.return_date_list(self.src_args.src_first_extract_date,self.meta_key,self.s3_bucket_trg)
        self.meta_update_list=None
    def extract(self):
        """
        Read the source data and concatenates them to one Pandas DateFrame
        :return:
        data_frame: Pandas DataFrame with the extracted data
        """
        self._logger.info("Extracting Xetra source files started...")
        files=[key for date in self.extract_date_list\
                   for key in self.s3_bucket_src.list_files_in_prefix(date)]
        if not files:
            data_frame=pd.DataFrame()

        else:
            data_frame=pd.concat([self.s3_bucket_src.read_csv_to_df(file)\
                                  for file in files],ignore_index=True)
        self._logger.info('Extracting Xetra source files finished.')
        return data_frame


    def transform_report1(self,data_frame:pd.DataFrame):
        """
        Applies the necessary transformation to create report 1
        :param data_frame: Pandas DateFrame as Input
        :return:
        data_frame: Transformed Pandas DateFrame as Output
        """
        if data_frame.empty:
            self._logger.info('The dataframe is empty; No transformation will be preceeded')
            return data_frame
        self._logger.info('Applying transformations to Xetra source dta for report 1')
        # Filtering necessary source columns
        data_frame=data_frame.loc[:,self.src_args.src_columns]
        # Removing rows with missing values
        data_frame.dropna(inplace=True)

        # Calculating opening price per ISIN and day
        data_frame=[self.trg_args.trg_col_op_price]=data_frame\
        .sort_values(by=[self.src_args.src_col_time])\
        .groupby([
            self.src_args.src_col_isin,
            self.src_args.src_col_date
        ])[self.src_args.src_col_start_price].transform('last')

        # Renaming columns
        data_frame.rename(
            columns={
                self.src_args.src_col_min_price:self.trg_args.trg_col_min_price,
                self.src_args.src_col_max_price:self.trg_args.trg_col_max_price,
                self.src_args.src_col_traded_vol:self.trg_args.trg_col_daily_trade_vol

            },inplace=True
        )

        # Aggregating per ISIN and day -> opening price, closing price,
        # minimum price, maximum price, traded volume
        data_frame=data_frame.groupby([
            self.src_args.src_col_isin,
            self.src_args.src_col_date
        ],as_index=False)\
        .agg({
            self.trg_args.trg_col_op_price:'min',
            self.trg_args.trg_col_close_price:'min',
            self.trg_args.trg_col_min_price:'min',
            self.trg_args.trg_col_daily_trade_vol:'sum'
        })
        # Change of current days closing price compared to the previous trading days
        data_frame[self.trg_args.trg_col_ch_prev_close]=data_frame\
        .sort_values(by=[self.src_args.src_col_date])\
        .groupby([self.src_args.src_col_isin])[self.trg_args.trg_col_op_price]\
        .shift(1)

        data_frame[self.trg_args.trg_col_ch_prev_close]=(
            data_frame[self.trg_args.trg_col_op_price]\
            -data_frame[self.trg_args.trg_col_ch_prev_close]
        )/data_frame[self.trg_args.trg_col_ch_prev_close]*100

        # rounding to 2 dicimal
        data_frame=data_frame.round(decimal=2)

        # Removing the day befoer extract_date
        data_frame=data_frame[data_frame.Date>=self.extract_date].reset_index(drop=True)
        self._logger.info('Applying transformation to Xetra source data finished...')
        return data_frame



        
    def load(self,data_frame:pd.DataFrame):
        """
        Saves a Pandas DataFrame to the target
        :param data_frame: Pandas DataFrame as Input
        :return:
        """
        # Creating target key
        target_key=(
            f'{self.trg_args.trg_key}'
            f'{datetime.today().strftime(self.trg_args.trg_key_date_format)}.'
            f'{self.trg_args.trg_format}'
        )

        # Writing to target
        self.s3_bucket_src.write_df_to_s3(data_frame,target_key,self.trg_args.trg_format)
        self._logger.info("Xetra target data successfully written.")

        # Updating meta file
        MetaProcess.update_meta_file(self.meta_update_list,self.meta_key,self.s3_bucket_trg)
        self._logger.info('Xetra meta file successfully updated.')
        return True



    def etl_report1(self):
        """
        Extract, transform and load to create report 1
        :return:
        """

        # Extraction
        data_frame=self.extract()
        # Transform
        data_frame=self.transform_report1(data_frame)
        # Load
        self.load(data_frame)
        return True

