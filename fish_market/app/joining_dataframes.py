import boto3
import pandas as pd
import io

from fish_market.app.fish_market import fish_market_averages
from fish_market.app.fish_market_mon import fish_market_mon_averages
from fish_market.app.fish_market_tues import fish_market_tues_averages

s3_client = boto3.client('s3')
bucket_name = 'data-eng-resources'

"""
The following function (join_dataframes_and_upload_csv()) takes the three functions, fish_market_mon_averages(),
fish_market_tues_averages() and fish_market_averages(), and puts them into a list,
it then concatenates the list making one dataframe instead of three and stores it in a variable,
it then groups the new concatenated dataframe by species,
finally it uploads it to the Amazon S3 'data-eng-resources' bucket
"""


def join_dataframes_and_upload_csv():
    frames = [                              # variable for storing the three dataframes
        fish_market_mon_averages(),         # first dataframe
        fish_market_tues_averages(),        # second dataframe
        fish_market_averages()              # third dataframe
    ]

    result = pd.concat(frames)                                  # concatenating and storing the dataframes
    average_per_species = result.groupby('Species').mean()      # grouping the new single dataframe by species

    string_buffer = io.StringIO()                                               # in-memory text streams
    average_per_species.to_csv(string_buffer)                                   # converting to csv file
    s3_client.put_object(Body=string_buffer.getvalue(), Bucket=bucket_name,     # uploading to the bucket
                         Key='Data24/Fish/Tia_Fish_Market_Averages.csv')        # assigning new key value
