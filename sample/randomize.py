"""
A collection of functions to generate random values for each field,
using rules pre-defined on sample config file.

The random function must follow a stardard name: generate_random_FIELDTYPE.

Eg.: type string -> generate_random_string
     type number -> generate_random_number
"""
import json
import random
import datetime
import string


def generate_random_boolean(field, **kwargs):
    """
    Generate a random boolean value formatted as a string.
    If field is not required, return an empty result.

    Args:
        field: The field object.
    Keyword Arguments:
        only_required: The argument to generate only required fields.
        fulldate: A boolen to decide if must be a timestamp or time.
        index: The index that indicate the record line on CSV.

    Returns:
        A random value based on the pre-defined value list.
    """
    if not field['required'] and kwargs.get("only_required"):
        return ''
    defined_values = ['True', 'False', 'T', 'F',
                      'true', 'false', '1', '0', '']
    return random.choice(defined_values)


def generate_random_gender(field, **kwargs):
    """
    A random gender based on a predefined list. The record has 75%
    of chance to use a value of the list and 25% of chance to use
    a random generated value.
    If field is not required, return an empty result.

    Args:
        field: The field object.
    Keyword Arguments:
        only_required: The argument to generate only required fields.
        fulldate: A boolen to decide if must be a timestamp or time.
        index: The index that indicate the record line on CSV.

    Returns:
        A random value based on the pre-defined value list or a
        random value generated.
    """
    if not field['required'] and kwargs.get("only_required"):
        return ''

    if random.randint(1, 100) > 75:
        return generate_random_string(field, **kwargs)
    defined_values = ['Male', 'male', 'MALE', 'M', 'Female',
                      'female', 'FEMALE', 'F', 'O', 'Other',
                      'OTHER', 'other', 'Not Specified', 'NS',
                      'NOT SPECIFIED', 'not specified', 'N/A',
                      'n/a', 'N/a', 'n/A', '']
    return random.choice(defined_values)


def generate_random_datetime(field, **kwargs):
    """
    Generate a random full date, using current datetime as final.
    If field is not required, return an empty result.

    Args:
        field: The field object.
    Keyword Arguments:
        only_required: The argument to generate only required fields.
        fulldate: A boolen to decide if must be a timestamp or time.
        index: The index that indicate the record line on CSV.

    Returns:
        A random datetime value.
    """
    return generate_random_date(field,
                                only_required=kwargs.get("only_required"),
                                fulldate=True)


def generate_random_date(field, **kwargs):
    """
    Generate a random full date, using current datetime as final.
    If field is not required, return an empty result.

    Args:
        field: The field object.
    Keyword Arguments:
        only_required: The argument to generate only required fields.
        fulldate: A boolen to decide if must be a timestamp or time.
        index: The index that indicate the record line on CSV.

    Returns:
        A random date value.
    """
    if not field['required'] and kwargs.get("only_required"):
        return ''
    start_days = random.randrange(1, 43435)
    end_date = datetime.datetime.now()
    start_date = end_date - datetime.timedelta(days=start_days)
    random_date = start_date + (start_date - end_date) * random.random()
    if not kwargs.get("fulldate"):
        return random_date.strftime("%m/%d/%Y")
    return random_date.strftime("%Y-%m-%d %H:%M:%S")


def generate_random_string(field, **kwargs):
    """
    Generate a random string. If there is a default value, use it.
    If field is not required, return an empty result.

    Args:
        field: The field object.
    Keyword Arguments:
        only_required: The argument to generate only required fields.
        fulldate: A boolen to decide if must be a timestamp or time.
        index: The index that indicate the record line on CSV.

    Returns:
        A random string value using a min and max length.
    """
    if not field['required'] and kwargs.get("only_required"):
        return ''
    if 'format' in field:
        return str(field['format']).format(index=kwargs.get("index"))
    random_values = string.ascii_lowercase
    string_size = random.randint(field['min_length'], field['max_length'])
    random_str = ''.join(random.choices(random_values, k=string_size))
    return str(random_str.title())


def generate_random_client(field, **kwargs):
    """
    Generate clients from 0 to 3 entries.
    If field is not required, return an empty result.

    Args:
        field: The field object.
    Keyword Arguments:
        only_required: The argument to generate only required fields.
        fulldate: A boolen to decide if must be a timestamp or time.
        index: The index that indicate the record line on CSV.

    Returns:
        A random json list with random client values.
    """
    if not field['required'] and kwargs.get("only_required"):
        return ''

    # Generate a number between 0 and 3 to define the number of clients.
    clients_number = random.randint(0, 3)
    clients = []

    # If no clients, check if it will return an empty list or empty value.
    if clients_number == 0:
        if bool(random.getrandbits(1)):
            return ''
        return json.dumps(clients)

    for i in range(clients_number):
        json_loaded = json.loads(field['format'])
        # Generate the client id and name.
        json_loaded['clientId'] = str(random.randint(999999, 99999999))
        json_loaded['name'] = 'Client Name {}'.format(i)
        clients.append(json_loaded)
    return json.dumps(clients)


def generate_random_password(field, **kwargs):
    """
    Generate a random string password or a random encryption method.

    Args:
        field: The field object.
    Keyword Arguments:
        only_required: The argument to generate only required fields.
        fulldate: A boolen to decide if must be a timestamp or time.
        index: The index that indicate the record line on CSV.

    Returns:
        A random string value using a min and max length.
        Or a encryption method defined to the field in sample_config.json.
    """
    if "predefined_password_list" not in field:
        return generate_random_string(field, **kwargs)

    count_encryption = len(field['predefined_password_list'])

    index_pass = random.randint(0, count_encryption)

    # Randomly Pick one of the predefined hashing algorithms or
    # random plain text password.
    if index_pass == count_encryption:
        return generate_random_string(field, **kwargs)

    return json.dumps(field['predefined_password_list'][index_pass])
