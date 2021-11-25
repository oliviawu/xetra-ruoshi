
"""
TestS3BucketconnectorMethods
"""
import os
import unittest
import boto3
from moto import mock_s3
from xetra.common.s3 import S3BucketConnector
import pandas as pd
from io import StringIO,BytesIO
from xetra.common.custom_exceptions import WrongFormatException

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

    def test_read_csv_to_df_ok(self):
        """
        Tests the read_csv_to_df method for reading 1 csv file from the mocked s3 bucket
        :return:
        """
        #Expected values
        key_exp='text.csv'
        col1_exp='col1'
        col2_exp='col2'
        val1_exp='val1'
        val2_exp='val2'
        log_exp=f'Reading file {self.s3_endpoint_url}/{self.s3_bucket_name}/{key_exp}'

        #Test init
        csv_content=f'{col1_exp},{col2_exp}\n{val1_exp},{val2_exp}'
        self.s3_bucket.put_object(Body=csv_content,Key=key_exp)

        #Methid execution
        with self.assertLogs() as logm:
            df_result=self.s3_bucket_conn.read_csv_to_df(key_exp)
            # Log test after method execution
            self.assertIn(log_exp,logm.output[0])

        # Test after method execution
        self.assertEqual(df_result.shape[0],1)
        self.assertEqual(df_result.shape[1], 2)
        self.assertEqual(val1_exp,df_result[col1_exp][0])
        self.assertEqual(val2_exp, df_result[col2_exp][0])

        # Clean up
        self.s3_bucket.delete_objects(
            Delete={
                'Object':[
                    {'Key': key_exp}
                ]
            }
        )

    def test_write_df_to_s3_empty(self):
        """
        Test the write_df_to_s3 method with as empty DataFrame as input
        :return:
        """

        # Exception results
        return_exp=None
        log_exp='The dataframe is empty.'

        # Test Init
        df_empty=pd.DataFrame()
        key='key.csv'
        file_format='csv'
        # Method exce
        with self.assertLogs() as logm:
            results=self.s3_bucket_conn.write_df_to_s3(df_empty,key,file_format)
            # log test after exec
            self.assertIn(log_exp,logm.output[0])

        # Test after mehod exec
        self.assertEqual(return_exp,results)

    def test_write_df_to_s3_csv(self):
        """
        Test the qrite_df_to_s3 method
        :return:
        """
        # Expected results
        return_exp=True
        df_exp=pd.DataFrame([['A','B'],['C','D']], columns =['col1','col2'])
        key_exp='test.csv'
        log_exp=f'Writing file to {self.s3_endpoint_url}/{self.s3_bucket_name}/{key_exp}'

        # Test init
        file_format='csv'

        # Method exec
        with self.assertLogs() as logm:
            result=self.s3_bucket_conn.write_df_to_s3(df_exp,key_exp,file_format)
            # Log test after the exec
            self.assertIn(log_exp,logm.output[0])

        # Test after method exec
        data=self.s3_bucket.Object(key=key_exp).get().get('Body').read().decode('utf-8')
        out_buffer=StringIO(data)
        df_result=pd.read_csv(out_buffer)
        self.assertEqual(return_exp,result)
        self.assertTrue(df_exp.equals(df_result))

        # clean up
        self.s3_bucket.delete_objects(
            Delete={
                'Object':[
                    {
                        'Key': key_exp
                    }
                ]
            }
        )
    def test_write_df_to_s3_parquet(self):
        """
        Test the qrite_df_to_s3 method
        :return:
        """
        # Expected results
        return_exp=True
        df_exp=pd.DataFrame([['A','B'],['C','D']], columns =['col1','col2'])
        key_exp='test.parquet'
        log_exp=f'Writing file to {self.s3_endpoint_url}/{self.s3_bucket_name}/{key_exp}'

        # Test init
        file_format='parquet'

        # Method exec
        with self.assertLogs() as logm:
            result=self.s3_bucket_conn.write_df_to_s3(df_exp,key_exp,file_format)
            # Log test after the exec
            self.assertIn(log_exp,logm.output[0])

        # Test after method exec
        data=self.s3_bucket.Object(key=key_exp).get().get('Body').read().decode('utf-8')
        out_buffer=StringIO(data)
        df_result=pd.read_csv(out_buffer)
        self.assertEqual(return_exp,result)
        self.assertTrue(df_exp.equals(df_result))

        # clean up
        self.s3_bucket.delete_objects(
            Delete={
                'Object':[
                    {
                        'Key': key_exp
                    }
                ]
            }
        )

    def test_write_df_to_s3_wrong_format(self):
        """
        Tests the write_df_to_s3 method
        if a not support format is given as argument
        :return:
        """

        # Expected results
        df_exp=pd.DataFrame([['A','B'],['C','D']],columns=['col1','col2'])
        key_exp='test.parquet'
        format_exp='wrong_format'
        log_exp=f'The file format {format_exp} is not supported to be written to s3.'
        exception_exp=WrongFormatException

        # method exec
        with self.assertLogs() as logm:
            with self.assertRaises(exception_exp):
                self.s3_bucket_conn.write_df_to_s3(df_exp,key_exp,format_exp)

            # log test after exec
            self.assertIn(log_exp,logm.output[0])


if __name__=='__main__':
    unittest.main()
    # test=TestS3BucketConnectorMethods()
    # test.setUp()
    # test.test_list_files_in_prefix_ok()
    # test.tearDown()