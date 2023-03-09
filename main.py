from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from prefect.filesystems import GCS
from io import BytesIO
import base64
import pprint as pp
import pytz
import datetime
import base64
import os.path
import PyPDF2
import re


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
            
            #pp.pprint(est_datetime)
            #print(est_datetime.strftime("%Y-%m-%d %H:%M:%S"))

            # pp.pprint(message)
            # pp.pprint(dir(message))

            # Get the payload of the message (including the headers and body)
            payload = message['payload']
            # Iterate over the parts of the payload (which could include email body and/or attachments)
            for part in message['payload']['parts']:
                #pp.pprint(part)
                
                if part['filename'] and part['filename'].endswith('.pdf'):
                    data = part['body'].get('data')
                    if not data:
                        att_id = part['body'].get('attachmentId')
                        att = service.users().messages().attachments().get(userId='me', messageId=message['id'], id=att_id).execute()
                        data = att['data']
                    pdf_bytes = base64.urlsafe_b64decode(data.encode('UTF-8'))
                    pdf_file = BytesIO(pdf_bytes)
                    pdf_reader = PyPDF2.PdfReader(pdf_file)
                    text = ''
                    for page in range(1):
                        text += pdf_reader.pages[page].extract_text()
                    #new = text.split('\n')
                    # print(text)
                    # print()
                    extract_data_from_pdf(text)
                    
            #         if ctr == 6:
            #             break

            # if ctr == 6:
            #     break

    except HttpError as error:
        print(f'An error occurred: {error}')

def extract_invoice_id(invoice):
    pattern = r"(?i)(?<=Invoice Number[:;]\s)[A-Z0-9]+"
    try:
        invoice_id = re.search(pattern, invoice).group(0)
        return invoice_id
    except AttributeError:
        print(invoice)

def extract_invoice_datetime(invoice):
    pattern = r"(?i)(?<=Invoice Date[:;]\s)\d{2}/\d{2}/\d{2}"
    try:
        invoice_datetime = re.search(pattern, invoice).group(0)
        return invoice_datetime
    except AttributeError:
        print(invoice)

def extract_invoice_amount_due(invoice):
    pattern = r"(?i)(?:Ampunt|Amount)\s*Due[;:]\s*\$\s*(\d{1,3}(?:,\d{3})*(?:\.\d{2})?)"
    try:
        invoice_amnt_due = re.search(pattern, invoice).group(1)
        invoice_amnt_due = invoice_amnt_due.replace(',', '')
        return float(invoice_amnt_due)
    except AttributeError:
        print(invoice)

def extract_invoice_due_date(invoice):
    pattern = r"(?<=Due Date[:;] )\d{2}\/\d{2}\/\d{2}"
    try:
        invoice_amnt_due = re.search(pattern, invoice).group(0)
        return invoice_amnt_due
    except AttributeError:
        print(invoice)

def extract_gas_quantity(invoice):
    pattern = r"(?<!\.)\b\d+\.\d{4}\b(?!\.)"
    try:
        invoice_amnt_due = re.search(pattern, invoice).group(0)
        return invoice_amnt_due
    except AttributeError:
        print('no gas quantity found, check pdf. Possible that gas is not included in invoice.')


def extract_data_from_pdf(pdf):
    invoice_id = extract_invoice_id(pdf)
    invoice_datetime = extract_invoice_datetime(pdf)
    invoice_amnt_due = extract_invoice_amount_due(pdf)
    invoice_due_date = extract_invoice_due_date(pdf)
    gas_quantity = extract_gas_quantity(pdf)
    print(gas_quantity, invoice_datetime)


if __name__ == '__main__':
    get_invoice_pdf_list()


# import json
# import fsspec

# # create a GCSFileSystem object with authentication using the `project` parameter
# gcs = fsspec.filesystem('gcs', project='My First Project', token='browser')

# # specify the path to the JSON file in GCS
# path = 'gs://creds/token.json'

# # open the file using GCSFileSystem
# with gcs.open(path, 'r') as f:
#     # read the JSON content
#     json_content = json.load(f)
#     pp.pprint(json_content)

