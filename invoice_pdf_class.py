import re
from pprint import pformat
from datetime import datetime
from prefect.blocks.system import Secret

class InvoicePdf:
    def __init__(self, pdf, email_id):
        email_acc = Secret.load("email-user").get()
        self.email_url = f'https://mail.google.com/mail/{email_acc}/#inbox/{email_id}'
        self.extract_invoice_id(pdf)
        self.extract_invoice_datetime(pdf)
        self.extract_invoice_amount_due(pdf)
        self.extract_invoice_due_date(pdf)
        self.extract_gas_quantity(pdf)
        self.extract_driver(pdf)
    
    def __repr__(self) -> str:
        return pformat(self.__dict__)

    def extract_invoice_id(self, invoice_text):
        """
        Extracts the invoice id from the given invoice string.

        Args:
            invoice_text (str): A string containing invoice information.

        Returns:
            None

        Example:
            >>> invoice_text = "...Invoice Number: ABC123..."
            >>> extract_invoice_id(invoice_text)
            Sets self.invoice_id = 'ABC123'
        """


        pattern = r"(?i)(?<=Invoice Number[:;]\s)[A-Z0-9]+"
        try:
            temp = re.search(pattern, invoice_text).group(0)
            self.invoice_id = [temp]
        except AttributeError:
            self.invoice_id = [None]

    def extract_invoice_datetime(self, invoice_text):
        """
        Extracts the datetime that the invoice was sent out from the given invoice string.

        Args:
            invoice_text (str): A string containing invoice information.

        Returns:
            None

        Example:
            >>> invoice_text = "...Invoice Date: 03/01/22..."
            >>> extract_invoice_datetime(invoice_text)
            Sets self.invoice_datetime = '03/01/22'
        """


        pattern = r"(?i)(?<=Invoice Date[:;]\s)\d{2}/\d{2}/\d{2}"
        try:
            date_string = re.search(pattern, invoice_text).group(0)
            self.invoice_date = [datetime.strptime(date_string, '%m/%d/%y').date()]
        except AttributeError:
            print(invoice_text)
            self.invoice_date = [None]

    def extract_invoice_amount_due(self, invoice_text):
        """
        Extracts the invoice amount due from the given invoice string.

        Args:
            invoice_text (str): A string containing invoice information.

        Returns:
            None

        Example:
            >>> invoice_text = "...Amount Due: $ 1,234.56..."
            >>> extract_invoice_amount_due(invoice_text)
            Sets self.invoice_amnt_due = 1234.56
        """


        pattern = r"(?i)(?:Ampunt|Amount)\s*Due[;:]\s*\$\s*(\d{1,3}(?:,\d{3})*(?:\.\d{2})?)"
        try:
            invoice_amnt_due = re.search(pattern, invoice_text).group(1)
            invoice_amnt_due = invoice_amnt_due.replace(',', '')
            self.invoice_amount_due = [float(invoice_amnt_due)]
        except AttributeError:
            self.invoice_amount_due = [None]

    def extract_invoice_due_date(self, invoice_text):

        """
        Extracts the date that the invoice payment is due from the given invoice string.

        Args:
            invoice_text (str): A string containing invoice information.

        Returns:
            None

        Example:
            >>> invoice_text = "...Due Date: 03/31/22..."
            >>> extract_invoice_due_date(invoice_text)
            Sets self.invoice_due_date = '03/31/22'
        """

        pattern = r"(?<=Due Date[:;] )\d{2}\/\d{2}\/\d{2}"
        try:
            date_string = re.search(pattern, invoice_text).group(0)
            self.invoice_due_date = [datetime.strptime(date_string, "%m/%d/%y").date()]
        except AttributeError:
            self.invoice_due_date = [None]

    def extract_gas_quantity(self, invoice_text):
        """
        Extracts the gas quantity from the given invoice string.

        Args:
            invoice_text (str): A string containing invoice information.

        Returns:
            None

        Example:
            >>> invoice_text = "...Gas Quantity: 1234.5678..."
            >>> extract_gas_quantity(invoice_text)
            Sets self.gas_quantity = '1234.5678'
        """


        pattern = r"(?<!\.)\b\d+\.\d{4}\b(?!\.)"
        try:
            temp = re.search(pattern, invoice_text).group(0)
            self.invoice_gas_quantity = [float(temp)]
        except AttributeError:
            self.invoice_gas_quantity = [None]

    def extract_driver(self, invoice_text):
        """
        Extracts the driver that delievered the gas from the given invoice string.

        Args:
            invoice_text (str): A string containing invoice information.

        Returns:
            None

        Example:
            >>> invoice_text = "...Driver: John Doe..."
            >>> extract_driver(invoice_text)
            Sets self.driver = 'John Doe'
        """


        pattern = r"Driver[:;]? (\w+\s+\w+)"
        try:
            temp = re.search(pattern, invoice_text).group(1)
            self.driver = [temp]
        except AttributeError:
            self.driver = [None]

    # experimental
    def extract_unit_price(self, invoice_text):
        pattern = r"(?<!\.)\b\d+\.\d{4}\b\s*\$(\d{1,3}(,\d{3})*(\.\d+)?)\b"
        try:
            self.unit_pp = re.search(pattern, invoice_text).group(1)
        except AttributeError:
            self.unit_pp = invoice_text