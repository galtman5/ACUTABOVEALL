import streamlit as st
import pandas as pd
from helpers import connect_to_snowflake
import altair as alt


st.title('A Cut Above All - Gas Metrics')

with connect_to_snowflake() as conn:
    df = pd.read_sql('SELECT * FROM ACUTABOVEALL.PUBLIC.GAS_METRICS', conn)

st.markdown('<span style="color:#7fc97f">Amount Due</span>', unsafe_allow_html=True)
st.markdown('<span style="color:#fdc086">Gas Quantity</span>', unsafe_allow_html=True)

# Create a line chart for the "Amount Due" line
line1 = alt.Chart(df).mark_line(color='#7fc97f').encode(
    x='INVOICE_DATE',
    y='AMOUNT_DUE',
    color=alt.Color(scale=alt.Scale(range=['#7fc97f']))
)

# Create a line chart for the "Gas Quantity" line
line2 = alt.Chart(df).mark_line(color='#fdc086').encode(
    x='INVOICE_DATE',
    y='GAS_QUANTITY',
    color=alt.Color(scale=alt.Scale(range=['#fdc086']))
)

# Combine the two line charts into a single chart
chart = line1 + line2

# Remove axis titles
chart = chart.configure_axis(
    title=None
)
# Display the chart in the Streamlit app
st.altair_chart(chart, use_container_width=True)






# # slice only the relavent columns for viz
# amount_df = df[['INVOICE_DATE', 'INVOICE_AMOUNT_DUE']]
# amount_df.columns = ['Invoice Date', 'Invoice Amount Due']
# st.line_chart(data=amount_df, x='Invoice Date', y='Invoice Amount Due')