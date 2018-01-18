import time
import json
import logging

logger = logging.getLogger(__file__)

def transform_password(value):
    """
    Transform a password hash into an object that specifies the type of hashing
    algorithm used. This allows password hashes from legacy systems to be
    loaded on a per-record basis.
    """
    return {
        'type': "password-crypt-sha256",
        'value': value.strip()
    }


def transform_date(value):
    """
    Janrain requires dates in the format of "%Y-%m-%d %H:%M:%S" (UTC). This
    function transforms dates from the legacy system(s) into this format.
    """
    if not value:
        return

    input_formats = (
        "%Y-%m-%d %H:%M:%S",
        "%a, %b %w %H:%M:%S"
    )

    for try_format in input_formats:
        try:
            parsed_time = time.strptime(value.upper(), try_format)
            return time.strftime("%Y-%m-%d %H:%M:%S", parsed_time)
        except ValueError:
            pass

    raise ValueError("Could not parse date: {}".format(value))


def transform_plural(value):
    """
    Transform the plural data represented in the CSV as a JSON string into a
    Python object.
    """
    return json.loads(value)


def transform_clients_plural(value):
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    return [{
        'clientId': value,
        'firstLogin': timestamp,
        'lastLogin': timestamp,
        'name': "Data Migration"
    }]


def transform_boolean(value):
    """
    Transform boolean values that are blank into NULL so that they are not
    imported as empty strings.
    """
    if value.lower in ("true", "t", "1"):
        return True
    elif value.lower in ("false", "f", "0"):
        return False
    else:
        return None