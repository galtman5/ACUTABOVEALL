from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from io import BytesIO
from prefect import task
from invoice_pdf_class import InvoicePdf
from helpers import connect_to_snowflake
from rich.traceback import install
install()

import base64
import os.path
import PyPDF2
import pandas as pd

@task
def get_most_recent_inv_date_from_snowflake():
    query = ''' SELECT INVOICE_DATE
                FROM ACUTABOVEALL.PUBLIC.GAS_METRICS
                ORDER BY INVOICE_DATE DESC
                LIMIT 1;'''

    with connect_to_snowflake() as conn:
        cur = conn.cursor()
        cur.execute(query)

        most_recent_snowflake_invoice, = cur.fetchone()        

    return most_recent_snowflake_invoice   

@task
def get_most_recent_inv_from_email():

    """Shows basic usage of the Gmail API.
    Lists the user's emails sent from "djoy@portconsolidated.com" and downloads the attachments.
    """

    # If modifying these scopes, delete the file token.json.
    SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']

    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('creds/token.json'):
        creds = Credentials.from_authorized_user_file('creds/token.json', SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'creds/credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('creds/token.json', 'w') as token:
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
        
        # Get the first message in the list (most recent invoice)
        message = service.users().messages().get(userId='me', id=messages[0]['id']).execute()

        # Get the payload of the message (the headers and body)
        payload = message['payload']

        # Iterate over the parts of the payload (which include email body and/or attachments)
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

                return InvoicePdf(page_text, message['id'])
    except HttpError as error:
        print(f'An error occurred: {error}')