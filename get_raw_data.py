## -- Load libs -- ##
import os

import pandas as pd
from datetime import datetime
from utils.extract_data import wrapper_extract_data
from joblib import Parallel, delayed
import itertools
from datetime import datetime

## -- Data -- ##
possible_autonomous_regions = [
    None, 8742, 8743, 8744, 4, 5, 6, 7, 8, 9, 10, 11, 13, 14, 15, 16, 17,
    20, 21
    ]

widgets = ['demanda-tiempo-real', 'estructura-generacion', 'precios-mercados-tiempo-real']

if 'last_date.txt' in os.listdir('data'):
    with open('data/last_date.txt', 'r') as f:
        initial_date = f.read()
else:
    initial_date = '2015-01-01T00:00'

end_date = datetime.now().strftime('%Y-%m-%dT%H:%M')

# Auxiliar data
widget_category = {
    'demanda-tiempo-real': 'demanda',
    'estructura-generacion': 'generacion',
    'precios-mercados-tiempo-real': 'mercados'
}

data_granularity = {
    'demanda-tiempo-real': 'hour',
    'estructura-generacion': 'day',
    'precios-mercados-tiempo-real': 'hour'
}

# I chunckify the dates every 480h
dates= pd.date_range(initial_date, end_date, freq = '480h').strftime('%Y-%m-%dT%H:%M').tolist()
dates.append(end_date)

# Create periods
date_periods = [(dates[i-1], dates[i]) for i, _ in enumerate(dates) if i >0]

# Find all possible combinations of date periods and autonomous regions
possible_options =  list(itertools.product(*[widgets, possible_autonomous_regions, date_periods]))

# Convert each in a tuple
possible_options = [
    (widget_category.get(comb[0]), comb[0], comb[2][0], comb[2][1], data_granularity.get(comb[0]), comb[1])  
    for comb in possible_options
    ]

# Now, I parallelize the extractions to do so, I create a wrapper functiion
data = Parallel(n_jobs=-1)(delayed(wrapper_extract_data)(opt) for opt in possible_options)

# Combine the dat into a single dataframe
data = pd.concat(data)

# Turn the datetime to datetime 
data['datetime'] = pd.to_datetime(
    data['datetime'],
    format = '%Y-%m-%d %H:%M:%S',
    utc=True
    )

# Save the data
for group, group_data in data.groupby('api'):

    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    
    # Save the data as pickle
    group_data.to_pickle(f'data/{group}/{timestamp}.pickle')

# Save last date as txt
with open('data/last_date.txt', 'w') as f:
    f.write(end_date)