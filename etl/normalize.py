"""Source normalization and auditable preflight rules; business MR remains W04."""
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation, localcontext, ROUND_HALF_UP
import hashlib
import re
from orchestration.quality import REJECTIONS

HEADERS = ('InvoiceNo','StockCode','Description','Quantity','InvoiceDate','UnitPrice','CustomerID','Country')
MAX_I64 = 2**63-1
MIN_I64 = -2**63
ASCII_UPPER = str.maketrans('abcdefghijklmnopqrstuvwxyz','ABCDEFGHIJKLMNOPQRSTUVWXYZ')
MERCHANDISE = re.compile(r'[0-9]{5,6}[A-Z]{0,2}\Z', re.ASCII)


@dataclass
class Normalized:
    fields: list
    record_class: str
    primary_reason: str | None
    warnings: list[str]
    amount: int | None
    is_merchandise: bool
    field_errors: list[str]


def decimal(value):
    if isinstance(value, bool) or value is None or not isinstance(value,(int,float,str,Decimal)):
        raise ValueError('Invalid numeric type')
    try:
        result = Decimal(str(value).strip())
    except InvalidOperation:
        raise ValueError('Invalid numeric value') from None
    if not result.is_finite():
        raise ValueError('Nonfinite numeric value')
    return result


def integer(value):
    number = decimal(value)
    if number != number.to_integral_value() or not MIN_I64 <= number <= MAX_I64:
        raise ValueError('Invalid int64')
    return int(number)


def identifier(value):
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError('Invalid identifier')
    if isinstance(value, (float,Decimal)):
        value = str(integer(value))
    result = str(value).strip().translate(ASCII_UPPER)
    if any(ord(c)<32 for c in result):
        raise ValueError('Invalid identifier control character')
    return result or None


def normalize(values, source_sha256, row_number, date_format=None, cell_error=False):
    if len(values) != 8:
        raise ValueError('Expected eight source fields')
    invoice, stock, description, quantity, invoice_at, price, customer, country = values
    errors = []
    warnings = []
    parse_error = 'PARSE_ERROR' if cell_error else None
    def code(value, missing, length, invalid_length):
        nonlocal parse_error
        try: result = identifier(value)
        except ValueError:
            parse_error = 'PARSE_ERROR'; result = None
        if result is None: errors.append(missing)
        elif len(result)>length: errors.append(invalid_length)
        return result
    invoice = code(invoice,'MISSING_INVOICE',32,'INVALID_INVOICE_LENGTH')
    stock = code(stock,'MISSING_STOCK_CODE',32,'INVALID_STOCK_LENGTH')
    if description is None or not str(description).strip():
        description = None
        warnings.append('MISSING_DESCRIPTION')
    else:
        description = str(description).replace('\x00','').replace('\r',' ').replace('\n',' ').replace('\t',' ')
        if len(description)>512:
            description = description[:512]; warnings.append('DESCRIPTION_TRUNCATED')
    try: quantity = integer(quantity)
    except ValueError:
        quantity = None; errors.append('INVALID_QUANTITY')
    if quantity == 0: errors.append('ZERO_QUANTITY')
    try:
        if isinstance(invoice_at,str):
            if not date_format: raise ValueError('Text date requires explicit source format')
            invoice_at = datetime.strptime(invoice_at,date_format)
        elif isinstance(invoice_at,date) and not isinstance(invoice_at,datetime):
            invoice_at = datetime.combine(invoice_at,datetime.min.time())
        if not isinstance(invoice_at,datetime) or invoice_at.tzinfo is not None or invoice_at.microsecond:
            raise ValueError('Invalid source-local datetime')
        invoice_at = invoice_at.isoformat(timespec='seconds')
    except (ValueError,OverflowError):
        invoice_at = None; errors.append('INVALID_DATETIME')
    try:
        amount = decimal(price)
        with localcontext() as context:
            context.prec = 50
            rounded = amount.quantize(Decimal('0.01'),rounding=ROUND_HALF_UP)
            pence = rounded*100
        if not MIN_I64<=pence<=MAX_I64: raise ValueError('Price overflow')
        price = int(pence)
        if amount != rounded: warnings.append('PRICE_ROUNDED')
        if price<=0: errors.append('NONPOSITIVE_PRICE')
    except (ValueError,InvalidOperation):
        price = None; errors.append('INVALID_PRICE')
    if customer is None or (isinstance(customer,str) and not customer.strip()):
        customer = None; warnings.append('MISSING_CUSTOMER_ID')
    else:
        try:
            if isinstance(customer,str):
                if not re.fullmatch('[0-9]{1,32}',customer.strip(),flags=re.ASCII):
                    raise ValueError('Invalid customer ID')
                customer = customer.strip()
            else:
                customer = str(integer(customer))
                if not re.fullmatch('[0-9]{1,32}',customer,flags=re.ASCII): raise ValueError('Invalid customer ID')
        except ValueError:
            customer = None; warnings.append('INVALID_CUSTOMER_ID')
    country = ' '.join(str(country).split()) if country is not None else 'UNKNOWN'
    country = country or 'UNKNOWN'
    if '\x00' in country:
        parse_error = 'PARSE_ERROR'
        country = country.replace('\x00','') or 'UNKNOWN'
    if len(country)>64: errors.append('INVALID_COUNTRY_LENGTH')
    record_class = 'RETURN' if (invoice and invoice.startswith('C')) or (quantity is not None and quantity<0) else 'SALE'
    signed = None
    if quantity is not None and price is not None:
        signed = (-abs(quantity) if record_class=='RETURN' else quantity)*price
        if not MIN_I64<=signed<=MAX_I64 or abs(quantity)>MAX_I64:
            errors.append('AMOUNT_OVERFLOW'); signed = None
    if parse_error:
        errors.append('PARSE_ERROR')
    reason = next((name for name in REJECTIONS if name in errors),None)
    if reason:
        record_class = 'REJECTED'; signed = None
    else:
        if invoice.startswith('C') and quantity>0: warnings.append('CANCEL_POSITIVE_QUANTITY')
        if quantity<0 and not invoice.startswith('C'): warnings.append('NEGATIVE_WITHOUT_CANCEL')
    row_id = hashlib.sha256(f'{source_sha256}:{row_number}'.encode()).hexdigest()
    return Normalized([row_id,row_number,invoice,stock,description,quantity,invoice_at,price,customer,country,parse_error],
                      record_class,reason,warnings,signed,bool(stock and MERCHANDISE.fullmatch(stock)),
                      [name for name in REJECTIONS if name in errors])
