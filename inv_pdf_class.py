import openai
import json
import pprint as pp
from pprint import pformat
from datetime import datetime
from prefect.blocks.system import Secret


class InvoicePdf:
    def __init__(self, pdf_text, email_id):
        email_acc = Secret.load("email-user").get()
        self.email_url = f'https://mail.google.com/mail/{email_acc}/#inbox/{email_id}'
        self.extract_data(pdf_text)
    
    def __repr__(self) -> str:
        return pformat(self.__dict__)
    
    def extract_data(self, pdf_text):
        openai.api_key = Secret.load("openai-key").get()

        response = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        messages=[
                {"role": "system", "content": '''You are a text parser. You need to extract the invoice_id, invoice_date, gas_quantity,
                                                total_amount_due, invoice_due_date, and the driver_name from whatever is inputted. Return it
                                                as a python dictionary. Return the key's as as I gave them to you. If you cannot find any value please return None. 
                                                Leave the total_amount_due as a float. Use double quotes for all keys and values. If there is more info than just the gas quantity, ignore it. 
                                                All I want is the more expensive gas amount and unit price. You can ignore the cheaper line items. You must adhere to a
                                                strict py dictionary schema. {"driver_name":string, "gas_quantity":float, "invoice_date":datetime object, "invoice_due_date":datetime object,
                                                "invoice_id":string, "total_amount_due":float}'''},
                {"role": "user", "content": pdf_text}
            ]
        )
        gpt_output = (response['choices'][0]['message']['content'].replace('\\', ''))
        data_dict = json.loads(gpt_output)
        pp.pprint(data_dict)
        print(type(data_dict))