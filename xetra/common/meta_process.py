"""
Methods for processiong the meta file
"""
from xetra.common.s3 import S3BucketConnector
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

    @staticmethod
    def return_date_list():
        pass

    

