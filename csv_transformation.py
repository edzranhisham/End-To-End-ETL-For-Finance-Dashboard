#csv transformation

import numpy as np
import pandas as pd
from os import listdir


# link to source folder directory
folderPath = "/Users/mac/Desktop/VSCode/Projects/Development-Personal-Finance/src_transactions/"


# GET FILENAMES #

# get list of filenames in directory
def find_csv_filenames(path_to_dir, suffix='.csv'):
    filenames = listdir(path_to_dir)
    return[filename for filename in filenames if filename.endswith(suffix)]

filenames = find_csv_filenames(folderPath, suffix=".csv")

for csv in filenames:
    print(csv)


# GET LATEST FILE NAME #

# extract the dates from filenames and convert string to integer
dates = [int(csv.split('_')[1][:6]) for csv in filenames]

# get the index of the latest date
maxDate = str(max(dates))

# get the latest filename
latestFile = 'transactions_' + maxDate + '.csv'



# READING CSV #

# read latest file starting from row 7
df = pd.read_csv(folderPath + latestFile, skiprows=6)

# drop missing rows with missing value in all columns
df = df.dropna(how='all')

# preview final dataframe
df.head()