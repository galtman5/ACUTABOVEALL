from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from io import BytesIO
import base64
import pprint as pp
import base64
import os.path
import PyPDF2
import re
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas


# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']

def get_invoice_pdf_list():
    """Shows basic usage of the Gmail API.
    Lists the user's emails sent from "djoy@portconsolidated.com" and downloads the attachments.
    """
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.json', 'w') as token:
            token.write(creds.to_json())

    try:
        # Call the Gmail API
        service = build('gmail', 'v1', credentials=creds)

        # Query emails sent from djoy@portconsolidated.com
        query = "from:djoy@portconsolidated.com"
        messages = service.users().messages().list(userId='me', q=query).execute().get('messages', [])

        if not messages:
            print('No messages found.')
            return

        print(f"Number of messages sent from djoy@portconsolidated.com: {len(messages)}")

        for ctr, message in enumerate(messages):
            # Get the message by ID
            message = service.users().messages().get(userId='me', id=message['id']).execute()

            # Get the payload of the message (the headers and body)
            payload = message['payload']

            # Iterate over the parts of the payload (which could include email body and/or attachments)
            for part in message['payload']['parts']:                
                if part['filename'] and part['filename'].endswith('.pdf'):
                    data = part['body'].get('data')
                    if not data:
                        att_id = part['body'].get('attachmentId')
                        att = service.users().messages().attachments().get(userId='me', messageId=message['id'], id=att_id).execute()
                        data = att['data']
                    pdf_bytes = base64.urlsafe_b64decode(data.encode('UTF-8'))

                    # read pdf file
                    pdf_file = BytesIO(pdf_bytes)
                    pdf_reader = PyPDF2.PdfReader(pdf_file)

                    # invoice is only on the first page of the pdf email attachment
                    page_text = pdf_reader.pages[0].extract_text()
                    invoice_data_payload = extract_data_from_pdf(page_text)
                    write_to_snowflake(invoice_data_payload)

    except HttpError as error:
        print(f'An error occurred: {error}')

def write_to_snowflake(json_payload):

    df = pd.DataFrame(json_payload)

    # Define connection parameters
    SNOWFLAKE_ACCOUNT = 'MAB87887.us-east-1'
    SNOWFLAKE_USER = 'GALTMAN5'
    SNOWFLAKE_PASSWORD = 'G091198a'

    conn = snowflake.connector.connect(
                account=SNOWFLAKE_ACCOUNT,
                user=SNOWFLAKE_USER,
                password=SNOWFLAKE_PASSWORD
            )
    
    res,t,x,c = write_pandas(conn, df, 'GAS_METRICS', 'ACUTABOVEALL', 'PUBLIC')

    print(res)


def extract_invoice_id(invoice):
    pattern = r"(?i)(?<=Invoice Number[:;]\s)[A-Z0-9]+"
    try:
        invoice_id = re.search(pattern, invoice).group(0)
        return invoice_id
    except AttributeError:
        return None

def extract_invoice_datetime(invoice):
    pattern = r"(?i)(?<=Invoice Date[:;]\s)\d{2}/\d{2}/\d{2}"
    try:
        invoice_datetime = re.search(pattern, invoice).group(0)
        return invoice_datetime
    except AttributeError:
        return None

def extract_invoice_amount_due(invoice):
    pattern = r"(?i)(?:Ampunt|Amount)\s*Due[;:]\s*\$\s*(\d{1,3}(?:,\d{3})*(?:\.\d{2})?)"
    try:
        invoice_amnt_due = re.search(pattern, invoice).group(1)
        invoice_amnt_due = invoice_amnt_due.replace(',', '')
        return float(invoice_amnt_due)
    except AttributeError:
        return None

def extract_invoice_due_date(invoice):
    pattern = r"(?<=Due Date[:;] )\d{2}\/\d{2}\/\d{2}"
    try:
        invoice_amnt_due = re.search(pattern, invoice).group(0)
        return invoice_amnt_due
    except AttributeError:
        return None

def extract_gas_quantity(invoice):
    pattern = r"(?<!\.)\b\d+\.\d{4}\b(?!\.)"
    try:
        invoice_amnt_due = re.search(pattern, invoice).group(0)
        return invoice_amnt_due
    except AttributeError:
        return None

def extract_driver(invoice):
    pattern = r"Driver[:;]? (\w+\s+\w+)"
    try:
        invoice_amnt_due = re.search(pattern, invoice).group(0)
        return invoice_amnt_due
    except AttributeError:
        return None

# experimental
def extract_unit_price(invoice):
    pattern = r"(?<!\.)\b\d+\.\d{4}\b\s*\$(\d{1,3}(,\d{3})*(\.\d+)?)\b"
    try:
        invoice_amnt_due = re.search(pattern, invoice).group(1)
        return invoice_amnt_due
    except AttributeError:
        return invoice

def extract_data_from_pdf(pdf):
    invoice_id = extract_invoice_id(pdf)
    invoice_datetime = extract_invoice_datetime(pdf)
    invoice_amnt_due = extract_invoice_amount_due(pdf)
    invoice_due_date = extract_invoice_due_date(pdf)
    gas_quantity = extract_gas_quantity(pdf)
    driver = extract_driver(pdf)
    #unit_price = extract_unit_price(pdf)

    return {
        'INVOICE_ID': [invoice_id],
        'INVOICE_DATE': [invoice_datetime],
        'INVOICE_GAS_QUANTITY': [gas_quantity],
        'INVOICE_AMOUNT_DUE': [invoice_amnt_due],
        'INVOICE_DUE_DATE': [invoice_due_date],
        'DRIVER': [driver]
        }


if __name__ == '__main__':
    get_invoice_pdf_list()
