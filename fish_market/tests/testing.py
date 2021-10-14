import boto3
import pandas as pd
import datatest as dt
import pytest

s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')
bucket_name = 'data-eng-resources'

data_frame_one = s3_client.get_object(Bucket=bucket_name, Key="python/fish-market.csv")['Body']
data_frame_two = s3_client.get_object(Bucket=bucket_name, Key="python/fish-market-mon.csv")['Body']
data_frame_three = s3_client.get_object(Bucket=bucket_name, Key="python/fish-market-tues.csv")['Body']


def test_df1():
    return pd.read_csv(data_frame_one)


def test_df2():
    return pd.read_csv(data_frame_two)


def test_df3():
    return pd.read_csv(data_frame_three)

