# Data Loader

The Dataload script is itended for assisting with the data migration process. It demonstrates how to use the
[/entity.bulkCreate](https://educationcenter.janrain.com/home/entitybulkcreate)
API to bulk load user profiles into the platform.

Table of Contents:
------------------
* [Requirements](#requirements)
    * [Dependences](#dependencies)
    * [Data Format](#data-format)
* [Data Load](#data-load)
    * [Dataload Command Line](#dataload-command-line)
    * [Delta Migration](#delta-migration)
    * [Live Run](#live-run)
    * [Result Logs](#result-logs)
    * [Data Transformations](#data-transformations)
    * [Logging](#logging)
* [Tips and Best Practices](#tips-and-best-practices)
* [Sample Generator](#sample-generator)
    * [Sample Config file](#sample-config-file)
    * [Sample Generator Command Line](#sample-generator-command-line)
    * [Sample Generator Random Values](#sample-generator-random-values)
* [Rollback](#rollback)
    * [Rollback Command Line](#rollback-command-line)

## Requirements

* [Python](https://www.python.org/) >= 3.0
* [janrain-python-api](https://pypi.python.org/pypi/janrain-python-api) >= 0.3.0
* CSV file with all data to be imported following Data Format guidelines below
* API client with direct_access or owner feature

### Dependencies

The dependencies are specified in the `requirements.txt` and can be installed
using `pip`:

    pip install -r requirements.txt

### Data Format

The script consumes CSV files formatted with the following rules:

* UTF-8 encoded
* Comma delimited
* Unix-style line endings
* First row (column headers) must match the schema attribute names
  * Object attributes expressed in "dot notation" such as `primaryAddress.city`
* Plural attributes represented as JSON strings (don't forget to use double-quotes)
* Password attributes should be represented as JSON strings containing the encryption hash or ideally just for development purposes as plain text which will be encrypted using bcrypt. See all [supported hashing](https://educationcenter.janrain.com/home/managing-your-schema).


***Password Examples***

```json
{
    "type": "password-md5",
    "value": "2f23fa3579f3f75175793649115c1b25"
}
```

```json
{
    "type": "password-md5-salted-right-base64",
    "value": "EA781758F006D63D8B138D9D01E00E55",
    "salt": "S41t"
}
```

Note: For a valid CSV syntax, JSON strings will have additional double-quotes like in the example below:

```json
"{""type"":""password-md5"",""value"": ""2f23fa3579f3f75175793649115c1b25""}"
```

See `sample_data.csv` for an example. You can also use the [sample generator](#sample-generator) to generate more examples.

## Data Load

The `dataload.py` script will import all records in specified CSV file into target Entity Type.

### Dataload Command Line

***Command-line Arguments***

The utility is invoked from the command-line as a Python 3 script. The `--help`
flag is used to obtain usage information:

    python3 dataload.py --help
    usage: dataload.py [-h] [-u APID_URI] [-i CLIENT_ID] [-s CLIENT_SECRET]
                    [-k CONFIG_KEY] [-d] [-t TYPE_NAME] [-b BATCH_SIZE]
                    [-a START_AT] [-w WORKERS] [-o TIMEOUT] [-r RATE_LIMIT]
                    [-x] [-m] [-p PRIMARY_KEY]
                    DATA_FILE

    positional arguments:
    DATA_FILE             full path to the data file being loaded

    optional arguments:
    -h, --help            show this help message and exit
    -u APID_URI, --apid_uri APID_URI
                            Full URI to the Capture API domain
    -i CLIENT_ID, --client-id CLIENT_ID
                            authenticate with a specific client_id. (Ideally a direct_access)
    -s CLIENT_SECRET, --client-secret CLIENT_SECRET
                            authenticate with a specific client_secret
    -k CONFIG_KEY, --config-key CONFIG_KEY
                            authenticate using the credentials defined at a
                            specific path in the configuration file (eg.
                            clients.demo)
    -d, --default-client  authenticate using the default client defined in the
                            configuration file
    -t TYPE_NAME, --type-name TYPE_NAME
                            entity type name (default: user)
    -b BATCH_SIZE, --batch-size BATCH_SIZE
                            number of records per batch (default: 100)
    -a START_AT, --start-at START_AT
                            record number to start at (default: 1)
    -w WORKERS, --workers WORKERS
                            number of worker threads (default: 10)
    -o TIMEOUT, --timeout TIMEOUT
                            timeout in seconds for API calls (default: 10)
    -r RATE_LIMIT, --rate-limit RATE_LIMIT
                            max API calls per second (default: 4)
    -x, --dry-run         process data without making any API calls

    Delta Migration Arguments:
    -m, --delta-migration
                            import the new entity and update any existing entity
    -p PRIMARY_KEY, --primary-key PRIMARY_KEY
                            use an existing attribute in the target Entity Type to
                            identify a duplicate record that will be updated. The
                            attribute must be unique on schema (default: email)

A progress bar indicating the number of successfull imports, fails, average imported records per minute and import progress are displayed to the user. The total number of records and the estimated time to completion are also available.

At the end, a summary with success, fail and retry count are displayed on the screen.

Additionally, an [/entity.count](https://educationcenter.janrain.com/home/entitycount) is performed in the entire Entity Type to indicate the final result after import.

    Starting the dataload import

        Loading data from my_data.csv into the 'user' entity type

        Validating UTF-8 encoding and checking for Byte Order Mark

    Labels: Total Success(S) | Total Fails(F) | Success Rate(SR) | Average Records per Minute(AVG)

    S:700 F:300 SR:70.0% AVG:5117rec/m : 100%|█████████████████████████████████████████████████████████████| 1000/1000 [00:11<00:00, 128.01rec/s]

    DATALOAD RESULTS
        [1000] Total processed users

        [1000] Import success. Number of new records inserted in database
        [0] Import failures

        [1000] Total number of records in Entity Type after execution user_dataload_test

    Please check detailed results in the files below:
        success_May_20_2019_14_54.csv
        fail_May_20_2019_14_54.csv

If BOM (Byte Order Mark) is detected the script will skip it and start at the 4th character. This usually happens when the CSV is generated or saved through the MS Excel.

    INFO utils.reader: Byte Order Mark detected.

UTF-8 validation will occur prior to processing the records. If any encoding issues are found the script will terminate and indicate which line needs to be corrected.

    Starting the dataload import

        Loading data from my_data.csv into the 'user' entity type

        Validating UTF-8 encoding and checking for Byte Order Mark
    ERROR utils.reader: Line 23116: 'utf-8' codec can't decode byte 0xbf in position 7: invalid start byte

### Delta Migration

When the delta migration flag is enabled, the `dataload.py` will perform a standard import, inserting all records of the given CSV. If any record returns a duplicate error, the script will not mark the record as fail and instead add it to an update list that will be triggered as soon as the import finishes. Records will be updated based on the email attribute by default.

Two new log files will be generated in this case: `update_success_*.csv` and `update_fail_*.csv`. The first one will store the records that failed import but were successfully updated and the second one will store the records that failed both.

    Starting the dataload import

        Loading data from my_data.csv into the 'user' entity type

        Validating UTF-8 encoding and checking for Byte Order Mark

    Labels: Total Success(S) | Total Fails(F) | Success Rate(SR) | Average Records per Minute(AVG)

    S:0 F:100 SR:0.0% AVG:2142rec/m : 100%|█████████████████████████████████████████████████████████████| 100/100 [00:02<00:00, 35.72rec/s]


    Starting the update progress for the duplicated records

        Checking if there are any duplicate records to update

        100 duplicate records were found and will be updated

        Validating UTF-8 encoding and checking for Byte Order Mark

    Success:100 Fail:0: 100%|█████████████████████████████████████████████████████████████| 100/100 [00:09<00:00, 10.23rec/s]

    DATALOAD RESULTS
        [100] Total processed users

        [0] Import success. Number of new records inserted in database
        [0] Import failures

        [100] Update success. Existing users that were updated
        [0] Update failures

        [100] Total number of records in Entity Type after execution user_dataload_test

    Please check detailed results in the files below:
        success_May_20_2019_15_13.csv
        fail_May_20_2019_15_13.csv
        update_success_May_20_2019_15_13.csv
        update_fail_May_20_2019_15_13.csv

The argument `--primary-key` will be used to match the CSV record with the entity record. It defaults to `email` but can be overridden by any other attribute that is unique in the schema.

The `update_success_*.csv` contains the original batch id (from the import), the original csv line number and the primary key. The `update_fail_*.csv` contains the same columns of the `fail_*.csv` with batch id, line number and error message.

#### Important Notes

* The `--delta-migration` agument works together with the `--primary-key` argument. If delta migration is present but no primary key has been defined, dataload will assume the `email` as primary key for a update purpose.
* For plurals attributes, the entire structure will be replaced with the new one, using the data available on the data file used. This will work **ONLY** for plurals attributes attached with the `transform_plural` def.

_Note: If the delta migration argument is not present in the argument list, the log files for update job will not be created._

### Live Run

_Note: Always coordinate a production data migration through support portal to ensure application rate limits and monitoring have been configured appropriately._

The live run will be making API calls and thus must be limited to ensure that it runs well below API rate limits. The default values for number of workers, rate-limit and batch-size should already be set to a good throughput below API limits. These parameters can additionally be used to fine-tune the rate at which records are loaded.

    python3 dataload.py --apid_uri=https://my_application.dev.janraincapture.com --client_id=REDACTED --client_secret=REDACTED --rate-limit=4 --workers=10 --batch-size=100 my_data.csv

_Depending on the amount of data being imported in each records, you may experience API timeouts. Decreasing `--batch-size` is recommended in order to solve this, but increasing `--timeout` is also a possibility._

### Result Logs

While the data is being loaded, 2 separate CSV log files are used to store the
results *per record*. You can watch each of these logs in a separate terminal
to keep an eye on errors as they are returned from the API.

The failure log stores the batch number, line number, and error message:

    batch,line,error
    1,2,Attempted to update a duplicate value

The success log stores the batch number, line number, and UUID of the newly
created user:

    batch,line,uuid
    1,2,4f2db274-8d4c-4738-843f-6036fa21c802

### Data Transformations

Some data within the CSV source will need to be transformed before it can be
consumed by the API. A transformation function can be defined in
`transformations.py` and associated with a column in the import CSV file by inluding a line in `dataload_import.py`.

#### Available Transformations

* **transform_password** - Check if the received value is a valid json to be imported or will return a plain-text value to be converted using the bcrypt.
* **transform_date** - Transforms dates from known formats (eg.: `m/d/Y`) into the UTC date format.
* **transform_plural** - Transforms a JSON list string representation of a plural into JSON object representation. Empty list or empty string will be imported as NULL.
* **transform_boolean** - Transforms boolean insensitive strings (eg.: `1`, `0`, `TRUE`, `False`, `T`, `F`) into true Boolean types. Blank or unexpected values will be imported as NULL.
* **transform_gender** - Transforms gender strings (eg.: `M`, `F`, `MALE`, `FEMALE`, `Other`, `O`, `N/A`) into a specific gender string (`male`, `female`, `other` and `not specified`). Any string that does not match the transformation rule, will fallback to `not specified`. Blank values will be kept.

#### Custom Transformations

To create a custom transformation, define a new function in
`transformations.py`. For example, if you need to convert apples to
oranges:

```python
def transform_apples(value):
    if value == "apples":
        return "oranges"
    else:
        return value
```

You can then add that transformation to a CSV column using the
`add_transformation` method of the CSV reader object and inclduing the following in `dataload_import.py`:

```python
reader.add_transformation("favoriteFruit", transform_apples)
```

### Logging

The utility uses the standard Python
[logging](https://docs.python.org/3.3/library/logging.html) module for both the
application log and the result logs.

* `stdout` - Displays progress and summary results outout.
* `fail_*.csv` - Result log for records which failed to be imported. The name will be generated based on the timestamp.
* `success_*.csv` - Result log for records which successfully got imported. The name will be generated based on the timestamp.
* `retry_*.csv` - CSV file with the subset of user records that failed due to excessive or unexpected API issues. This file should be used after the initial import to ensure all records were processed.
* `dataload.log` - Application log at the DEBUG log level.
* `dataload_info.log` - Application log at the INFO and above log levels.

The formatting, filenames, log level, and other parameters can be configured for
the various loggers using the configuration file `logging_config.json`. See the
[Python Logging HowTo](https://docs.python.org/3/howto/logging.html) for details.

## Tips and Best Practices

* Use automation to generate the test data files to ensure that the exact same processes can generate data files for a production run.
* Adjust date/time values into UTC.
* Fine tune batch sizes with the `--batch-size` argument rather than increasing the `--timeout` argument. API calls should not take longer than 10 seconds.
* Keep the rate of the data load *well under* the application rate limit so as not to impact API usage from other sources (especially on production environments).
* Use the `tail -f` command on the result logs to keep an eye on API calls which are failing. If you see the 510 (rate limit) error code you need to reduce the `--rate-limit` and/or `--workers` arguments.
* Running the utility on a very large set of data may take a while. Try running the script from [screen](http://www.gnu.org/software/screen/manual/screen.html) session on a server.
* You may use the sample generator script to create test data for running the dataload.

## Sample Generator

The `sample_generator.py` script generates a sample CSV file, using a predefined list of fields with [random values](#random-values).

The list of predefined fields can be found on the `sample/sample_config.json` file. The field configuration has 3 attributes as required: `name`, `type` and `required`.

### Sample Config file

`name` must be the same of the schema attribute. `type` can be anything since it's used to define a function that will handle the data. And `required` (possible values are 1 or 0) defines if the field must be filled on the sample.

Also, it's possible to add `min_length`, `max_length` and `format` to the field configuration (as an optional configuration). For `min_length` and `max_length`, if not present on the field definition, the generator will use the global configuration (found above the fields definition).
The `format` configuration is used the generate a sequencial value like email or legacy ID: `email+0@example.com, email+1@example.com`.

Field structure (including optional attributes - `min_length` and `max_length` can't be used next to the `format`.):

***Using min_length and max_length attributes:***
```json
{
    "name": "field_name",
    "type": "field_type",
    "required": 1,
    "min_length": 10,
    "max_length": 30
}
```
***Using format attribute:***
```json
{
    "name": "field_name",
    "type": "field_type",
    "required": 1,
    "format": "email+{index}@example.com"
}
```
For password field types a specific key `"predefined_password_list"` can be used to specify a different hash algorithm.
Below an example:
```json
            {
                "name": "password",
                "type": "password",
                "required": 1,
                "min_length": 6,
                "max_length": 10,
                "predefined_password_list": [
                    {
                        "type": "password-md5",
                        "value": "2f23fa3579f3f75175793649115c1b25"
                    },
                    {
                        "type": "password-sha",
                        "value": "54e8d2e15d3caa89aa3f82c8c0428ad5742f056c"
                    },
                    {
                        "type": "password-base64",
                        "value": "UGFzczEyMw=="
                    }
                ]
            },
```
During the sample file generator one of the items from the `"predefined_password_list"` or a random plain text password will be chosen using `min_length` and `max_length`.

**Note:** The hash values you specify in the config file (`value` key on `predefined_password_list`) needs to be previously generated. For the above examples, the hash corresponds to **Pass123** plain text using the respective algorithm declared on `type` key.

The sample file generated follows the same [data format](#data-format) rules required for the [data load](#data-load).

### Sample Generator Command Line

***Command-line Arguments***

The utility is invoked from the command-line as a Python 3 script. The `--help`
flag is used to obtain usage information:

    python3 sample_generator.py --help
    usage: sample_generator.py [-h] [-n SAMPLE_SIZE] [-r]

    optional arguments:
    -h, --help            show this help message and exit
    -n SAMPLE_SIZE, --sample-size SAMPLE_SIZE
                            number of records on sample file (default: 1)
    -r, --only-required   only required attributes will be generated

A successfull run will generate the sample file with the record number defined on the arguments.

The predefined files are the following:
`email`, `givenName`, `familyName`, `displayName`, `password`, `mobileNumber`, `gender`, `birthday`, `primaryAddress.phone`, `primaryAddress.address1`, `primaryAddress.address2`, `primaryAddress.city`, `primaryAddress.zip`, `primaryAddress.stateAbbreviation`, `primaryAddress.country`, `created`, `clients`, `optIn.status`.

### Sample Generator Random Values

Most of fields will generate a completely random value, following the field rules. However, some fields has it own list of possible values, with the intention to test a specific [transformation](#transformation) like gender, boolean or password (See [password examples](#password-examples)).

All random generators can be found on `sample/randomize.py`. You may create custom field generator by following the pattern below.

For example, if your field is defined as `animal` type on [sample config file](#sample-config-file), you must create a generate function.

```python
def generate_random_animal(field, **kwargs):
    animal_list = ['dog','cat', 'bird']
    return random.choice(animal_list)
```
or something like:
```python
def generate_random_animal(field, **kwargs):
    return str('Dog')
```

**The def function MUST HAVE the same field type name.**

The parameters `field` and `**kwargs` are **required** for all random functions. The `field` parameter contains the field definition (loaded from sample cofig). The second contains the generator row count (to be used if needed) and the required only argument (passed when the generator was called) and can be used to define if a field must be generated.

A sample file already generated with 1 record, can be found named as `sample_data.csv`.

_Note: Any error during the sample generator execution, will be displayed on the screen._

## Rollback
The `rollback.py` script will delete all records that were successfully imported in a previous dataload execution. You should provide the success_*.csv file from the previous execution as an argument.

### Rollback Command Line
***Command-line Arguments***

The utility is invoked from the command-line as a Python 3 script. The `--help`
flag is used to obtain usage information:

    python3 rollback.py --help

    usage: rollback.py [-h] [-u APID_URI] [-i CLIENT_ID] [-s CLIENT_SECRET]
                   [-k CONFIG_KEY] [-d] [-t TYPE_NAME] [-w WORKERS]
                   [-o TIMEOUT] [-r RATE_LIMIT] [-x]
                   DATA_FILE

    positional arguments:
    DATA_FILE             full path to the data file being loaded

    optional arguments:
    -h, --help            show this help message and exit
    -u APID_URI, --apid_uri APID_URI
                            Full URI to the Capture API domain
    -i CLIENT_ID, --client-id CLIENT_ID
                            authenticate with a specific client_id
    -s CLIENT_SECRET, --client-secret CLIENT_SECRET
                            authenticate with a specific client_secret
    -k CONFIG_KEY, --config-key CONFIG_KEY
                            authenticate using the credentials defined at a
                            specific path in the configuration file (eg.
                            clients.demo)
    -d, --default-client  authenticate using the default client defined in the
                            configuration file
    -t TYPE_NAME, --type-name TYPE_NAME
                            entity type name (default: user)
    -w WORKERS, --workers WORKERS
                            number of worker threads (default: 10)
    -o TIMEOUT, --timeout TIMEOUT
                            timeout in seconds for API calls (default: 10)
    -r RATE_LIMIT, --rate-limit RATE_LIMIT
                            max API calls per second (default: 4)
    -x, --dry-run         process data without making any API calls


A progress bar indicating the number of successfull rollbacks, rollback fails, average rollback records per minute and rollback progress are displayed to the user. The total number of records and the estimated time to completion are also displayed.

At the end, a summary with success rollback count, rollback fail count and total entities found are displayed on the screen.

The count is performed in the entire Entity Type, so it indicates what existed previously in addition to the new import.

    Starting the rollback process.

    Validating UTF-8 encoding and checking for Byte Order Mark

    Success:100 Fail:0: 100%|█████████████████████████████████████████████████████████████| 100/100 [00:05<00:00,  3.39rec/s]

    DATALOAD ROLLBACK RESULTS
            [100] Total processed users

            [100] Delete success. Number of records deleted in database
            [0] Delete failures
            [0] Total number of records in Entity Type [user_data_load] after execution

    Please check detailed results in the files below:
            rollback_success_May_30_2019_10_23_36.csv
            rollback_fail_May_30_2019_10_23_36.csv
