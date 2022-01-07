import streamlit as st
import pandas as pd
import plotly.express as px
import os

def app():
    # Read data
    data_path = 'data/generacion_estructura-generacion'

    # I read the files
    files = os.listdir(data_path)
    data_files = [pd.read_pickle(f'{data_path}/{file}') for file in files]
    data = pd.concat(data_files)

    col1, col2 = st.columns(2)
    granularity = col1.selectbox(
        'With which granularity would you like to visualize the data?',
        (
            'Daily', 'Monthly'
            )
        )

    autonomous_region = col2.selectbox(
        'What autonomous region would you like to visualize?',
        (
            'Global',
            'Canarias',
            'Baleares',
            'Ceuta',
            'Melilla',
            'Andalucía',
            'Aragón',
            'Cantabria',
            'Castilla la Mancha',
            'Castilla y León',
            'Cataluña',
            'País Vasco',
            'Principado de Asturias',
            'Comunidad de Madrid',
            'Comunidad de Navarra',
            'Comunidad Valenciana',
            'Extremadura',
            'Galicia',
            'La Rioja',
            'Región de Murcia'
        )
        )

    # Filter the data
    data_filtered = data.loc[data['autonomous_region'] == autonomous_region, :]

    if granularity == 'Daily':
        data_filtered['date'] = data_filtered['datetime'].dt.strftime('%Y-%m-%d')
        data_filtered = data_filtered.groupby('date').sum().reset_index()

    if granularity == 'Monthly':
        data_filtered['date'] = data_filtered['datetime'].dt.strftime('%Y-%m')
        data_filtered = data_filtered.groupby('date').sum().reset_index()

    if granularity == 'Hourly':
        data_filtered['date'] = data_filtered['datetime'].dt.strftime('%Y-%m-%d %H-00-00')

    title_text = 'Spain' if autonomous_region == 'Global'  else autonomous_region

    fig1 = px.line(
        data_frame=data_filtered,
        x = 'date', 
        y = 'value', 
        title = f'Electricity generation evolution in {title_text}'
    )

    st.plotly_chart(fig1, use_container_width=True)