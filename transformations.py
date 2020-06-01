import time
import json
import logging
import base64

logger = logging.getLogger(__file__)

def transform_password(valueRaw):
    """
    Transform a password hash into an object that specifies the type of hashing
    algorithm used. This allows password hashes from legacy systems to be
    loaded on a per-record basis.
    """
    if not valueRaw:
        return None

    value = base64.b64decode(valueRaw)

    SHA1_LENGTH = 20

    b64_ssha = value[6:]
    ssha = base64.b64decode(b64_ssha)
    sha, salt = ssha[0:SHA1_LENGTH], ssha[SHA1_LENGTH:]

    return {
        'type': 'password-saltedsha-1-right-base64',
        'value': base64.b64encode(sha).decode('utf-8'),
        'salt': base64.b64encode(salt).decode('utf-8'),
    }


def transform_date(value):
    """
    Date formats are required in the format of "%Y-%m-%d %H:%M:%S" (UTC). This
    function transforms dates from the legacy system(s) into this format.
    """
    if not value:
        return None

    input_formats = (
        "%m/%d/%Y",
    )

    for try_format in input_formats:
        try:
            parsed_time = time.strptime(value.upper(), try_format)
            return time.strftime("%Y-%m-%d %H:%M:%S", parsed_time)
        except ValueError:
            pass

    raise ValueError("Could not parse date [{}]".format(value))


def transform_plural(value):
    """
    Transform the plural data represented in the CSV as a JSON string into a
    Python object.
    """
    if not value:
        return []
    return json.loads(value)


def transform_boolean(value):
    """
    Transform boolean values that are blank into NULL so that they are not
    imported as empty strings.
    """
    if value.lower() in ("true", "t", "1"):
        return True
    elif value.lower() in ("false", "f", "0"):
        return False
    else:
        return None

def transform_gender(value):
    """
    Transform any gender value into a normalized value.
    """
    if value.lower() in ("male", "m"):
        return "male"
    elif value.lower() in ("female", "f"):
        return "female"
    elif value.lower() in ("other", "o"):
        return "other"
    elif value.lower() in ("not specified", "ns", "na", "n/a"):
        return "not specified"
    elif value.strip():
        return "not specified"
    else:
        return None

def transform_number(value):
    """
    Convert a number from the CSV file, into a real float value.
    """
    if not value:
        return None
    return float(value)
