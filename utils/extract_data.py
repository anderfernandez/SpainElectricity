import requests
import pandas as pd
import time

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

def wrapper_extract_data(x):
    """Wrapper of the extract data function. It takes one tupple and runs the function.

    Args:
        x (tuple): Tuple containing the information of endpoint category & widget, start date, end date and autonomous region id.

    Returns:
        Dataframe: dataframe containing the required information.
    """
    return extract_data(x[0], x[1], x[2], x[3], x[4], x[5])
