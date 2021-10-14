import boto3
import pandas as pd

s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')

bucket_name = 'data-eng-resources'

s3_object = s3_client.get_object(Bucket=bucket_name, Key='python/fish-market.csv')

"""
The following function (fish_market_averages_length()) takes the data in the csv file and groups it by species, 
it then uses the three 'Length' columns to calculate the average length per species,
finally it prints the final result to the console
"""


def fish_market_averages_length():
    df = pd.read_csv(s3_object['Body'])                                         # reading the file and storing
    mean_df = df.groupby(['Species']).mean()                                    # grouping by species
    general_average = mean_df[['Length1', 'Length2', 'Length3']].mean(axis=1)   # getting the average per column
    print(general_average)                                                      # checking the result


"""
The following function (fish_market_averages()) takes the data in the csv file and groups it by species, 
it calculates the average then returns the final result to the console
"""


def fish_market_averages():
    df = pd.read_csv(s3_object['Body'])             # reading the file and storing in a variable
    mean_df = df.groupby(['Species']).mean()        # grouping by species and calculating the average
    return mean_df                                  # final result wanted
    # print(mean_df)                                # checking the result
