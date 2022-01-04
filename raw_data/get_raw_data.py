## -- Load libs -- ##
import os
os.chdir(os.getcwd().replace('\\raw_data', ''))

import pandas as pd
from datetime import datetime
from utils.extract_data import wrapper_extract_realtime_price 
from joblib import Parallel, delayed
import itertools


## -- Data -- ##
possible_autonomous_regions = [
    None, 8742, 8743, 8744, 4, 5, 6, 7, 8, 9, 10, 11, 13, 14, 15, 16, 17,
    20, 21
    ]

# Read last date
with open('raw_data/last_date.txt', 'r') as f:
    initial_date = f.read()

end_date = datetime.now().strftime('%Y-%m-%dT%H:%M')

# I chunckify the dates every 500h to avoid errors
dates= pd.date_range(initial_date, end_date, freq = '500h').strftime('%Y-%m-%dT%H:%M').tolist()
dates.append(end_date)

# Create periods
date_periods = [(dates[i-1], dates[i]) for i, _ in enumerate(dates) if i >0]

# Find all possible combinations of date periods and autonomous regions
possible_options =  list(itertools.product(*[possible_autonomous_regions, date_periods]))

# Convert each in a tuple
possible_options = [(comb[1][0], comb[1][1], comb[0])   for comb in possible_options]

# Now, I parallelize the extractions to do so, I create a wrapper functiion
data = Parallel(n_jobs=-1)(delayed(wrapper_extract_realtime_price)(opt) for opt in possible_options)

# Combine the dat into a single dataframe
data = pd.concat(data)

# Save the data
data.to_csv('raw_data/raw_data.csv')

# Save last date as txt
with open('raw_data/last_date.txt', 'w') as f:
    f.write(end_date)