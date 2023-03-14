import streamlit as st
import pandas as pd
from helpers import connect_to_snowflake


st.title('A Cut Above All - Gas Metrics')

with connect_to_snowflake() as conn:
    df = pd.read_sql('SELECT * FROM ACUTABOVEALL.PUBLIC.GAS_METRICS', conn)
    st.dataframe(df)
