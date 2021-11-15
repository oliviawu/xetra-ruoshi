
"""
TestS3BucketconnectorMethods
"""
import os
import unittest
import boto3
from moto import mock_s3
from xetra.common.s3 import S3BucketConnector

class TestS3BucketConnectorMethods(unittest.TestCase):
    """
    Testing the S3BucketConnector Class
    """
    def setUp(self):
        """
        Setting up the Environment
        :return:
        """
        # mocking s3 start
        self.mock_s3=mock_s3()
        self.mock_s3.start()
        #Defining the class argument
        self.s3_access_key='AWS_ACCESS_KEY_ID'
        self.s3_secret_key='AWS_SECRET_ACCESS_KEY'
        self.s3_endpoint_url='https://s3.eu-central-1.amazonaws.com'
        


    def tearDown(self):
        """
        Executing after unit tests
        :return:
        """
        #mocking s3 connection stops
        self.mock_s3.stop()
    def test_list_files_in_prefix_ok(self):
        """
        Tests the list files in prefix methods for getting 2 files keys
        :return:
        """
        pass
    def test_list_files_in_prefix_wrong_prefix(self):
        """
        test the list file prefix method in case wrong prefix
        :return:
        """
        pass
if __name__=='__main__':
    unittest.main()