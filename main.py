from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from io import BytesIO
from prefect.blocks.system import Secret
from snowflake.connector.pandas_tools import write_pandas
from invoice_pdf_class import InvoicePdf
import base64
import pprint as pp
import base64
import os.path
import PyPDF2
import pandas as pd
import snowflake.connector



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
                    invoice_pdf = InvoicePdf(page_text)
                    print(invoice_pdf)

                    # backfill has already happened. Need to implement the checker flow now.
                    #write_to_snowflake(invoice_data_payload)

    except HttpError as error:
        print(f'An error occurred: {error}')

def write_to_snowflake(json_payload):

    df = pd.DataFrame(json_payload)

    SNOWFLAKE_PASSWORD = Secret.load("snowflake-pw").get()
    SNOWFLAKE_ACCOUNT = Secret.load("snowflake-account-identifier")
    SNOWFLAKE_USER = 'GALTMAN5'

    with snowflake.connector.connect(
                account=SNOWFLAKE_ACCOUNT,
                user=SNOWFLAKE_USER,
                password=SNOWFLAKE_PASSWORD
            ) as conn:
    
        res,t,x,c = write_pandas(conn, 
                             df=df, 
                             table='GAS_METRICS', 
                             database='ACUTABOVEALL',
                             schema='PUBLIC')

    print(res)





if __name__ == '__main__':
    get_invoice_pdf_list()
