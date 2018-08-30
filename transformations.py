import time
import datetime
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
        'type': "password-phpass-md5",
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
        "%m/%d/%Y",
    )

    for try_format in input_formats:
        try:
            parsed_time = time.strptime(value.upper(), try_format)
            return time.strftime("%Y-%m-%d %H:%M:%S", parsed_time)
        except ValueError:
            pass

    raise ValueError("Could not parse date: {}".format(value))

def transform_emailVerified(value):
    """
    ##### takes any value and returns current datetime.  Useful for migrations that require this to be computed at runtime
    """
    currentDT = datetime.datetime.now()
    ##print (str(currentDT))

    return (str(currentDT))

def transform_plural(value):
    """
    Transform the plural data represented in the CSV as a JSON string into a
    Python object.
    """
    if value:
        return json.loads(value)
    else:
        return json.loads('[]')


def transform_boolean(value):
    """
    Transform boolean values that are blank into NULL so that they are not
    imported as empty strings.
    """
    ##print(value)
    ##print(value.lower())

    if value.lower() in ("true", "t") or  (value == "1"): 
        return True
    elif value.lower() in ("false", "f") or (value == "0"):
        return False
    else:
        return None


def transform_facebook(value):       

### custom transformation for a profiles column during a migration where the only social accounts will be facebook.  Computes the profiles plural value based on user facebook id. 
    
    if value:

        Url = "http://www.facebook.com/profile.php?id="+value

        plural = '[{"identifier":"'+Url+'","domain": "facebook.com"}]'

        return json.loads(plural)
    else:
        return json.loads('[]')
