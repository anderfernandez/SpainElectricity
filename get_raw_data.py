# Load libs

import os

import pandas as pd
from datetime import datetime
import importlib
import utils

importlib.reload(utils)
from utils.extract_data import red_electrica_data

from joblib import Parallel, delayed
import itertools

os.getcwd()

possible_autonomous_regions = [
    None, 8742, 8743, 8744, 4, 5, 6, 7, 8, 9, 10, 11, 13, 14, 15, 16, 17,
    20, 21
    ]

widgets = ['demanda-tiempo-real', 'estructura-generacion', 'precios-mercados-tiempo-real']

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


# For each widget, I get the widget
extract_categories = [widget_category.get(widget) for widget in widgets] 

# Initial dates
initial_dates = []
    
for i, widget in enumerate(widgets):
    
    intial_date_path = f'data/{extract_categories[i]}_{widget}'
    if 'last_date.txt' in os.listdir(intial_date_path):
    
        with open(f'{intial_date_path}/last_date.txt', 'r') as f:
            tmp = f.read()
        
    else:
        tmp = '2015-01-01T00:00'

    initial_dates.append(tmp)

# End date
end_date = datetime.now().strftime('%Y-%m-%dT%H:%M')

# I combine everything
input_data = [
    (
        widget,
        extract_categories[i],
        data_granularity.get(widget),
        possible_autonomous_regions,
        initial_dates[i]
    ) 
    for i, widget in enumerate(widgets)] 

def wrapper_red_electrica_data(x):
    
    return red_electrica_data(x[0], x[1], x[2], x[3], x[4])

for input in input_data:
    
    try:
        wrapper_red_electrica_data(input)
    
    except:
        # If error, pass and it will be retried next run
        pass