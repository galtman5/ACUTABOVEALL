import re
from pprint import pformat

class InvoicePdf:
    def __init__(self, pdf_text):
        self.extract_data_from_pdf(pdf_text)
    
    def __repr__(self) -> str:
        return pformat(self.__dict__)

    def extract_data_from_pdf(self, pdf):
        self.extract_invoice_id(pdf)
        self.extract_invoice_datetime(pdf)
        self.extract_invoice_amount_due(pdf)
        self.extract_invoice_due_date(pdf)
        self.extract_gas_quantity(pdf)
        self.extract_driver(pdf)
        #self.extract_unit_price(pdf)

        return {
            'INVOICE_ID': [self.invoice_id],
            'INVOICE_DATE': [self.invoice_datetime],
            'INVOICE_GAS_QUANTITY': [self.gas_quantity],
            'INVOICE_AMOUNT_DUE': [self.invoice_amnt_due],
            'INVOICE_DUE_DATE': [self.invoice_due_date],
            'DRIVER': [self.driver]
            }

    def extract_invoice_id(self, invoice):
        pattern = r"(?i)(?<=Invoice Number[:;]\s)[A-Z0-9]+"
        try:
            self.invoice_id = re.search(pattern, invoice).group(0)
        except AttributeError:
            self.invoice_id = None

    def extract_invoice_datetime(self, invoice):
        pattern = r"(?i)(?<=Invoice Date[:;]\s)\d{2}/\d{2}/\d{2}"
        try:
            self.invoice_datetime = re.search(pattern, invoice).group(0)
        except AttributeError:
            self.invoice_datetime = None

    def extract_invoice_amount_due(self, invoice):
        pattern = r"(?i)(?:Ampunt|Amount)\s*Due[;:]\s*\$\s*(\d{1,3}(?:,\d{3})*(?:\.\d{2})?)"
        try:
            invoice_amnt_due = re.search(pattern, invoice).group(1)
            invoice_amnt_due = invoice_amnt_due.replace(',', '')
            self.invoice_amnt_due = float(invoice_amnt_due)
        except AttributeError:
            self.invoice_amnt_due = None

    def extract_invoice_due_date(self, invoice):
        pattern = r"(?<=Due Date[:;] )\d{2}\/\d{2}\/\d{2}"
        try:
            self.invoice_due_date = re.search(pattern, invoice).group(0)
        except AttributeError:
            self.invoice_due_date = None

    def extract_gas_quantity(self, invoice):
        pattern = r"(?<!\.)\b\d+\.\d{4}\b(?!\.)"
        try:
            self.gas_quantity = re.search(pattern, invoice).group(0)
        except AttributeError:
            self.gas_quantity = None

    def extract_driver(self, invoice):
        pattern = r"Driver[:;]? (\w+\s+\w+)"
        try:
            self.driver = re.search(pattern, invoice).group(0)
        except AttributeError:
            self.driver = None

    # experimental
    def extract_unit_price(self, invoice):
        pattern = r"(?<!\.)\b\d+\.\d{4}\b\s*\$(\d{1,3}(,\d{3})*(\.\d+)?)\b"
        try:
            self.invoice_amnt_due = re.search(pattern, invoice).group(1)
        except AttributeError:
            self.invoice_amnt_due = invoice