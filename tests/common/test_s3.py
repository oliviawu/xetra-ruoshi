
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
        self.s3_bucket_name='test_bucket'
        # Creating s3 access keys as environment variable
        os.environ[self.s3_access_key]='KEY1'
        os.environ[self.s3_secret_key]='KEY2'
        # creating a bucket on the mocked s3
        self.s3=boto3.resource(service_name='s3',endpoint_url=self.s3_endpoint_url)
        self.s3.create_bucket(Bucket=self.s3_bucket_name,
                             CreateBucketConfiguration={
                                 'LocationConstraint': 'eu-central-1'

                             })
        self.s3_bucket=self.s3.Bucket(self.s3_bucket_name)
        # Creating testing instance
        self.s3_bucket_conn=S3BucketConnector(self.s3_access_key,
                                              self.s3_secret_key,
                                              self.s3_endpoint_url,
                                              self.s3_bucket_name)

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
        # Expected results
        prefix_exp='prefix/'
        key1_exp=f'{prefix_exp}test1.csv'
        key2_exp=f'{prefix_exp}test2.csv'
        # Test init
        csv_content="""col1,col2
        valA,valB
        """
        self.s3_bucket.put_object(Body=csv_content,Key=key1_exp)
        self.s3_bucket.put_object(Body=csv_content, Key=key2_exp)
        # Method Exec
        list_result=self.s3_bucket_conn.list_files_in_prefix(prefix_exp)
        # Tests after method exec

        self.assertEqual(len(list_result),2)
        self.assertIn(key1_exp,list_result)
        self.assertIn(key2_exp, list_result)
        # cleanup after tests
        self.s3_bucket.delete_objects(
            Delete={
                'Objects':[
                    {
                        'Key':key1_exp
                    },
                    {
                        'Key': key2_exp
                    }
                ]
            }
        )

    def test_list_files_in_prefix_wrong_prefix(self):
        """
        test the list file prefix method in case wrong prefix
        :return:
        """
        # Expected results
        prefix_exp = 'no-prefix/'
        # Method Exec
        list_result = self.s3_bucket_conn.list_files_in_prefix(prefix_exp)
        # Tests after method exec
        self.assertTrue(not list_result)

if __name__=='__main__':
    unittest.main()
    # test=TestS3BucketConnectorMethods()
    # test.setUp()
    # test.test_list_files_in_prefix_ok()
    # test.tearDown()