import os.path
from google.auth.transport.requests import Request
import base64
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
import pprint as pp
import pytz
import datetime
import base64
from io import BytesIO
import PyPDF2

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

        for message in messages:
            # Get the message by ID
            message = service.users().messages().get(userId='me', id=message['id']).execute()
            timestamp = int(message['internalDate']) / 1000.0
            email_datetime_recieved = datetime.datetime.fromtimestamp(timestamp)
            
            # Set EST timezone
            est_tz = pytz.timezone('US/Eastern')

            # Convert datetime object to EST timezone
            est_datetime = email_datetime_recieved.astimezone(est_tz)
            #pp.pprint(est_datetime)
            #print(est_datetime.strftime("%Y-%m-%d %H:%M:%S"))

            # pp.pprint(message)
            # pp.pprint(dir(message))

            # Get the payload of the message (including the headers and body)
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
                    pdf_file = BytesIO(pdf_bytes)
                    pdf_reader = PyPDF2.PdfReader(pdf_file)
                    text = ''
                    for page in range(1):
                        text += pdf_reader.pages[page].extract_text()
                    new = text.split('\n')
                    print(new[8])
                    #break
            #break in place so that loop only executes once
            #break
                        
    except HttpError as error:
        print(f'An error occurred: {error}')

if __name__ == '__main__':
    get_invoice_pdf_list()
