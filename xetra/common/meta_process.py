"""
Methods for processing the meta file
"""
import collections

from xetra.common.s3 import S3BucketConnector
from xetra.common.constants import MetaProcessFormat
import pandas as pd
from datetime import datetime,timedelta
from xetra.common.custom_exceptions import WrongMetaFileException

class MetaProcess():
    """
    Class for working with the meta file
    """

    @staticmethod
    def update_meta_file(extract_data_list: list,meta_key: str,s3_bucket_meta: S3BucketConnector):
        """
        Updating the meta file with the processed Xetra dates and todays date as processed date
        :param extract_data_list:
        :param meta_key:
        :param s3_bucket_meta:
        :return:
        """
        # Creating an empty DataFrame using the meta file columns names
        df_new=pd.DataFrame(columns=[MetaProcessFormat.META_SOURCE_DATE_COL.value,
                                     MetaProcessFormat.META_PROCESS_COL.value])

        # Filling the date column with extract_date_list
        df_new[MetaProcessFormat.META_SOURCE_DATE_COL.value]=extract_data_list

        # Filling the processed column
        df_new[MetaProcessFormat.META_PROCESS_COL.value]=\
           datetime.today().strftime(MetaProcessFormat.META_PROCESS_DATE_FORMAT.value)
        try:
            # If meta file exists ->union DataFrame of old and new meta is created
            df_old=s3_bucket_meta.read_csv_to_df(meta_key)
            if collections.Counter(df_old.columns)!=collections.Counter(df_new.columns):
                raise WrongMetaFileException
            df_all=pd.concat([df_old,df_new])
        except s3_bucket_meta.session.client('s3').exception.NoSuchKey:
             # No meta file exsists -> only the new data is used
             df_all=df_new

        # Write to s3
        s3_bucket_meta.write_df_to_s3(df_all,meta_key,MetaProcessFormat.META_FILE_FORMAT.value)
        return True



    @staticmethod
    def return_date_list(first_date:str,meta_key:str,s3_bucket_meta:S3BucketConnector):
        """
        Creating a list of dates based on yhe input first_date and the already processed dates in the meta file
        :param first_date: the earliest date Xetra data should be processed
        :param meta_key: key of the meta file on the S3 bucket
        :param s3_bucket_meta: S3BucketConnector for he bucket with the meta file
        :return:
        min_date: first date that should be processed
        return_date_list: list of all dates form Min_date till today
        """

        start=datetime.strptime(first_date,MetaProcessFormat.META_DATE_FORMAT.value)\
                                .date() - timedelta(days=1)

        today=datetime.today().date()

        try:
            # If meta file exists create return_date_list_ using the content of the meta file
            # Reading meta file
            df_meta=s3_bucket_meta.read_csv_to_df(meta_key)
            # Creating a list of dates from first_date until today
            dates=[start+ timedelta(days=x) for x in range(0,(today - start).days+1)]
            # create set of all dates in meta file
            src_dates=set(pd.to_datetime(
                df_meta[MetaProcessFormat.META_PROCESS_DATE_FORMAT.value]
            ).dt.date)
            dates_missing=set(dates[1:])-src_dates
            if dates_missing:
                # Determining the earliest date that should be extracted
                min_date=min(set(dates[1:])-src_dates)-timedelta(days=1)
                #creating a list of dates from min_dates until today
                return_min_date=(min_date +timedelta(days=1))\
                .strftime(MetaProcessFormat.META_DATE_FORMAT.value)
                return_dates=[
                    date.strftime(MetaProcessFormat.META_DATE_FORMAT.value)\
                         for date in dates if date>min_date
                ]
            else:
                # Setting values for the earliest date and the list of dates
                return_dates=[]
                return_min_date=datetime(2200,1,1).date()\
                .strftime(MetaProcessFormat.META_DATE_FORMAT.value)
        except s3_bucket_meta.session.client('s3').exceptions.NoSuchKey:
            # No meta file found - > creating a date list form first_date -1 day until today
            return_min_date=first_date
            return_dates=[
                start+timedelta(days=x).strftime(MetaProcessFormat.META_DATE_FORMAT.value)\
                for x in range(0,(today -start).days+1)
            ]

        return return_min_date,return_dates




    

