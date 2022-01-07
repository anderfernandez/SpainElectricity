# Thank to https://github.com/dataprofessor/multi-page-app
import streamlit as st
from multiapp import MultiApp
from apps import demand_real_time, electricity_generation, market_price_real_time 

app = MultiApp()

st.markdown("""
# Spain Electricity Dashboard
Visualize the evolution of Electricity Demand, Generation and Price in Spain.
""")

# Add all your application here
app.add_app("Real Time Demand", demand_real_time.app)
app.add_app("Electricity Generation", electricity_generation.app)
app.add_app("Real Time Market Price", market_price_real_time.app)

# The main app
app.run()