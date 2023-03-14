import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from prefect.blocks.system import Secret
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd


def connect_to_snowflake():
    SNOWFLAKE_PASSWORD = Secret.load("snowflake-pw").get()
    SNOWFLAKE_ACCOUNT = Secret.load("snowflake-account-identifier").get()
    SNOWFLAKE_USER = 'GALTMAN5'

    return snowflake.connector.connect(
                account=SNOWFLAKE_ACCOUNT,
                user=SNOWFLAKE_USER,
                password=SNOWFLAKE_PASSWORD
            )


def write_to_snowflake(json_payload):
    df = pd.DataFrame.from_dict(vars(json_payload))

    with connect_to_snowflake() as conn:
        res,t,x,c = write_pandas(conn, 
                             df=df, 
                             table_name='GAS_METRICS', 
                             database='ACUTABOVEALL',
                             schema='PUBLIC')

    return res