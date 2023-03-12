from prefect import flow, Task, get_run_logger
from subflows import get_most_recent_inv_date_from_snowflake, get_most_recent_inv_from_email

@flow
def main_flow():
    logger = get_run_logger()

    recent_snowflake_invoice = get_most_recent_inv_date_from_snowflake()
    recent_email_invoice = get_most_recent_inv_from_email()

    logger.warning(f'snowflake: {recent_snowflake_invoice}')
    logger.warning(f'gmail: {recent_email_invoice.invoice_datetime}')

    if recent_email_invoice.invoice_datetime > recent_snowflake_invoice:
        logger.info("New email has arrived!")
        logger.info("write this to snowflake")
    else:
        logger.info("No new emails have arrived.")


main_flow()