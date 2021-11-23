"""Connector and methods accessing S3"""

import os
import boto3
import logging
from io import StringIO,BytesIO
import pandas as pd
from xetra.common.constants import S3FileTypes
from xetra.common.custom_exceptions import WrongFormatException
class S3BucketConnector():
    """
    Class for interacting with S3 Buckets
    """

    def __init__(self,access_key:str, secret_key:str,endpoint:str,bucket:str):
        """
        Constructor for S3BucketConnector
        :param access_key: Access key for accessing S3
        :param secret_key: Secret key for accessing S3
        :param endpoint: endpoint url for S3
        :param bucket: S3 bucket name
        :return:
        """
        self._logger=logging.getLogger(__name__)
        self.endpoint=endpoint
        self.session=boto3.Session(aws_access_key_id=os.environ[access_key],
                                   aws_secret_access_key=os.environ[secret_key])

        self._s3=self.session.resource(service_name='s3',endpoint_url=endpoint)
        self._bucket=self._s3.Bucket(bucket)

    def list_files_in_prefix(self,prefix: str):
        """
        Listing all files with a prefix on the S3 bucket
        :param prefix: prefix on the S3 bucket that should be  filter with
        :return: files: list of all the files names containing the prefix in the ley
        """
        files = [obj.key for obj in self._bucket.objects.filter(Prefix=prefix)]
        return files

    def read_csv_to_df(self,key:str,encoding:str ='utf-8',sep: str=','):
        """
        reading a csv file from the S3 bucket and returning a dataframe
        :param key: key of a file that should be read
        :param encoding: encoding of the data inside the csv file
        :param sep: separator of the csv file
        :return: Pandas dataframe fo the file
        """
        self._logger.info('Reading file %s/%s/%s', self.endpoint,self._bucket.name,key)
        csv_obj = self._bucket.Object(key=key).get().get('Body').read().decode(encoding)
        data = StringIO(csv_obj)
        df = pd.read_csv(data, sep)
        return df


    def write_df_to_s3(self,df:pd.DataFrame, key: str,file_format:str):
        """
        Write a Pandas DataFrame to S3
        :param df:
        :param key:
        :param file_format:
        :return:
        """
        if df.empty:
            self._logger.info('The dataframe is empty.')
            return None
        if file_format== S3FileTypes.CSV.value:
            out_buffer = StringIO()
            df.to_csv(out_buffer, index=False)
            return self.__put_object(out_buffer, key)
        if file_format== S3FileTypes.PARQUET.value:
            out_buffer = BytesIO()
            df.to_partquet(out_buffer, index=False)
            return self.__put_object(out_buffer, key)
        self._logger.info('The file format %s is not supported to be written to s3.',file_format)
        raise WrongFormatException

    def __put_object(self,out_buffer: StringIO or BytesIO, key: str):
        """
        Helper function for self.write_df_to_s3()
        :param out_buffer: StringIO or BytesIO that should be written
        :param key: target key of the saved file
        :return:
        """
        self._logger.info('Writing file to %s/%s/%s', self.endpoint, self._bucket.name,key)
        self._bucket.put_object(Body=out_buffer.getvalue(),Key=key)
        return True


    

