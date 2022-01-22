import requests
import pandas as pd
import time
from datetime import datetime
import itertools

def extract_data(category, widget, start_datetime, end_datetime, data_granularity, geo_id = None):
    """ Given a timeperiod, extract realtime electricity price data from the spanish electricity market.

    Args:
        start_datetime (str): Initial datetime with format %Y-%m-%dT%H:%M. Example: 2020-01-01T00:00 
        end_datetime (str): Initial datetime with format %Y-%m-%dT%H:%M. Example: 2020-01-01T00:00
        geo_id (str)[Optional]: Geoid defining the autonomous region you want to extract data from. You can finde more information about geo ids in [this link](https://www.ree.es/es/apidatos). If None, no geo_id is used.

    Returns:
        DataFrame: dataframe containing the hourly price information for the desired timeperiod. 
    """

    # Data
    dict_autonomous_regions = {
        8742: 'Canarias',
        8743 : 'Baleares',
        8744: 'Ceuta',
        8745: 'Melilla',
        4: 'Andalucía',
        5: 'Aragón',
        6: 'Cantabria',
        7: 'Castilla la Mancha',
        8: 'Castilla y León',
        9: 'Cataluña',
        10: 'País Vasco',
        11: 'Principado de Asturias',
        13: 'Comunidad de Madrid',
        14: 'Comunidad de Navarra',
        15: 'Comunidad Valenciana',
        16: 'Extremadura',
        17: 'Galicia',
        20: 'La Rioja',
        21: 'Región de Murcia'
    }

    # Request data
    if geo_id is None:
        url = f'https://apidatos.ree.es/es/datos/{category}/{widget}?start_date={start_datetime}&end_date={end_datetime}&time_trunc={data_granularity}'
    else:
        url = f'https://apidatos.ree.es/es/datos/{category}/{widget}?start_date={start_datetime}&end_date={end_datetime}&time_trunc={data_granularity}&geo_id={geo_id}'
    
    resp = requests.get(url)
    # Convert response to DataFrame
    data = pd.DataFrame(
    resp.json()['included'][0]['attributes']['values']
    )

    data['api'] = category + '_' + widget
    data['data_granularity'] = data_granularity

    

    # Add autonomous region
    if geo_id is None:
        data['autonomous_region'] = 'Global'
    else:
        data['autonomous_region'] = dict_autonomous_regions.get(geo_id)
    
    # Sleep for slow fetching
    time.sleep(0.2)
    
    return data



def red_electrica_data(
    widget, widget_category, data_granularity,
    possible_autonomous_regions, initial_date
    ):
    """Extracts and saves the data from the Red Electrica Española API.

    Args:
        widget (str): Name of the widget.
        widget_category (str): [description]
        data_granularity (str): [description]
        possible_autonomous_regions (list): [description]
        initial_date (str): Date from which to begin extracting the data. Date format must follow %Y-%m-%dT%H:%D

    Returns:
        str: Ok if function runs ok.
    """

    # Set end date as now
    end_date = datetime.now().strftime('%Y-%m-%dT%H:%M')
    
    # I chunkify the dates every 480h
    dates= pd.date_range(pd.Timestamp(initial_date), pd.Timestamp(end_date), freq = '480h').strftime('%Y-%m-%dT%H:%M').tolist()
    dates.append(end_date)

    # Create periods
    date_periods = [(dates[i-1], dates[i]) 
    for i, _ in enumerate(dates) if i >0]

    # Find all possible combinations of date periods and autonomous regions
    possible_options =  list(itertools.product(*[widget, possible_autonomous_regions, date_periods]))

    # Convert each combination in a tuple
    possible_options = [
        (widget_category, widget, comb[2][0], comb[2][1], data_granularity, comb[1])  
        for comb in possible_options
        ]
    
    # Now, I parallelize the extractions to do so, I create a wrapper functiion
    data = []
    for i, opt in enumerate(possible_options):
        try:
            
            tmp = wrapper_extract_data(opt)
            data.append(tmp)
        
        # If error, then stop execution
        except:
            break
    
    if len(data)> 1:
        # Combine the dat into a single dataframe
        data = pd.concat(data)
    else:
        data = data[0] 
            
    # Turn the datetime to datetime 
    data['datetime'] = pd.to_datetime(
        data['datetime'],
        format = '%Y-%m-%d %H:%M:%S',
        utc=True
        )
    print(initial_date)
    # Filter if I receive info previous to the initial date
    initial_date_dt = pd.to_datetime(initial_date,  format='%Y-%m-%dT%H:%M:%S', utc = True)
    
    data = data.loc[
        data['datetime'] > initial_date_dt,
        :
    ] 
    print( data['datetime'].min())

    # Create the files in the data folder
    group = f'{widget_category}_{widget}'
    
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    
    # Get max data (not necesarilly end_date)
    max_data = data['datetime'].max().strftime('%Y-%m-%dT%H:%M')

    print(f'Guardo los datos como data/{group}/{timestamp}.pickle')
    # Save the data as pickle
    data.to_pickle(f'data/{group}/{timestamp}.pickle')
    
    print(f'Guardo last date en la ruta: data/{group}/last_date.txt')
    # Save last date as txt
    with open(f'data/{group}/last_date.txt', 'w') as f:
        f.write(max_data)
    
    return 'Ok'


def wrapper_red_electrica_data(x):
    
    return red_electrica_data(x[0], x[1], x[2], x[3], x[4])