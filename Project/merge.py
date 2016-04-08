#!/usr/bin/env python
import requests
import pandas as pd
import os
import pickle
import time


'''
This script uses foursquare EXPLORE api to fetch the foursquare
ratings for the restaurants in the inspection data.
'''


# read csv file and drop the irrelevant column
final = pd.read_csv('Merge_target.csv')
final.drop('Unnamed: 0', axis=1, inplace=True)
# test the api with one restaurant record
'''
url = 'https://api.foursquare.com/v2/venues/explore'  # create url
params = {'ll': '40.84826, -73.85595',  # create params
          'query': 'MORRIS PARK BAKE SHOP'.lower(),
          'limit': 2,
          'client_id': os.getenv('FOUR_SQUARE_CLIENT_ID'),
          'client_secret': os.getenv('FOUR_SQUARE_CLIENT_SECRET'),
          'v': 20151231}
data = requests.get(url=url, params=params)  # grab data
print data.json()['response']
'''
# scrape data from foursquare and save it in a pickle file

# create a dictionary with the restaurant index as keys and
# the query parameters as values
params_all = {}
for i in range(len(final)):
    params_all[i] = {'ll': final.loc[i, 'latlon'],
                     'query': final.loc[i, 'DBA'],
                     'limit': 2,
                     'client_id': os.getenv('FOUR_SQUARE_CLIENT_ID'),
                     'client_secret': os.getenv('FOUR_SQUARE_CLIENT_SECRET'),
                     'v': 20151231}
# create a dictionary restau to save the query result
restau = {}
# carry out the scraping task (query limitation is 5000 per hour)
url = 'https://api.foursquare.com/v2/venues/explore'
n = 0
for i in range(len(final)):
    restau[i] = requests.get(url=url, params=params_all[i]).json()
    n += 1
    if n % 3000.0 == 0:
        print 'parsed {0} records'.format(n)
        with open('ratings.pkl', 'w') as fh:
            pickle.dump(restau, fh)
            print 'the work has been saved'
        print 'resting'
        time.sleep(3600)
print 'task finished.'
