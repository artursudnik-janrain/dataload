Janrain Data Loader
===================

Example code demonstrating how to use the Janrain
[/entity.bulkCreate](http://developers.janrain.com/rest-api/methods/user-data/entity/bulkcreate/)
API to bulk load user profiles into the Janrain platform.


Requires
--------

* [Python](https://www.python.org/) >= 3.0
* [janrain-python-api](https://pypi.python.org/pypi/janrain-python-api) >= 0.3.0


Data Format
-----------

The script consumes CSV files formatted with the following rules:

* UTF-8 encoded
* Comma delimited
* Unix-style line endings
* First row (column headers) must match Janrain schema attribute names
  * Object attributes expressed in "dot notation" such as `primaryAddress.city`
* Plural attributes represented as JSON strings (don't forget to use double-quotes)

See `sample_data.csv` for an example.


Quick Start
-----------

***Install Dependencies***

The dependencies are specified in the `requirements.txt` and can be installed
using `pip`:

    pip install -r requirements.txt


***Dry Run***

A "dry run" will validate the file encoding (UTF-8) and ensure that all of the
records can be processed without making any API calls. Set the rate limit to 0
since the API is not actually being called.

    python3 janrain-dataload.py --apid_uri=https://my_application.dev.janraincapture.com --client_id=REDACTED --client_secret=REDACTED --rate-limit=0 --dry-run my_data.csv

A successfull run will log a message to the screen as each batch is processed by
a worker thread:

    INFO janrain-dataload.py: Loading data from /home/rsmith/my_data.csv into the 'user' entity type.
    INFO dataload.reader: Validating UTF-8 encoding
    INFO janrain-dataload.py: Batch #1 (lines 2-11)
    ...
    INFO janrain-dataload.py: Batch #63108 (lines 631072-631074)
    INFO janrain-dataload.py: Checking results
    INFO janrain-dataload.py: Done!

UTF-8 validation will occur prior to processing the records. If any data is
encountered which is not UTF-8 encoded the script will terminate and indicate
which line needs to be corrected.

    INFO janrain-dataload.py: Loading data from /home/rsmith/my_data.csv into the 'user' entity type.
    INFO dataload.reader: Validating UTF-8 encoding
    ERROR dataload.reader: Line 23116: 'utf-8' codec can't decode byte 0xbf in position 7: invalid start byte

***Live Run***

_Note: Always coordinate a production data migration with Janrain to ensure
application rate limits and monitoring have been configured appropriately._

The live run will be making API calls to Janrain and thus must be limited to
ensure that it runs well below API rate limits. The `--workers` and
`--rate-limit` arguments can be used to fine-tune the rate at which records are
loaded.

    python3 janrain-dataload.py --apid_uri=https://my_application.dev.janraincapture.com --client_id=REDACTED --client_secret=REDACTED --rate-limit=1 --workers=4 my_data.csv


***Result Logs***

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

_Note: The result logs are overwritten with each run._


Command-line Arguments
----------------------

The utility is invoked from the command-line as a Python 3 script. The `--help`
flag is used to obtain usage information:

    python3 janrain-dataload.py --help

    usage: janrain-dataload.py [-h] [-u APID_URI] [-i CLIENT_ID] [-s CLIENT_SECRET]
                        [-k CONFIG_KEY] [-d] [-t TYPE_NAME] [-b BATCH_SIZE]
                        [-a START_AT] [-w WORKERS] [-q QUEUE_SIZE] [-o TIMEOUT]
                        [-r RATE_LIMIT] [-x]
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
      -b BATCH_SIZE, --batch-size BATCH_SIZE
                            number of records per batch (default: 100)
      -a START_AT, --start-at START_AT
                            record number to start at (default: 1)
      -w WORKERS, --workers WORKERS
                            number of worker threads (default: 4)
      -q QUEUE_SIZE, --queue-size QUEUE_SIZE
                            max number of batches queued (default: 1000)
      -o TIMEOUT, --timeout TIMEOUT
                            timeout in seconds for API calls (default: 10)
      -r RATE_LIMIT, --rate-limit RATE_LIMIT
                            max API calls per second (default: 1)
      -x, --dry-run         process data without making any API calls



Data Transformations
--------------------

Some data within the CSV source will need to be transformed before it can be
consumed by the Janrain API. A transformation function can be defined in
`dataload/transformations.py` and associated with a column in the CSV file in
`janrain-dataload.py`.

***Available Transformations***

* **transform_password** - Transforms hashed password from a legacy system into
  JSON objects which specify the hashing algorithm to use.
* **transform_date** - Transforms dates from known formats into the Janrain's
  UTC date format.
* **transform_plural** - Transforms a JSON string representation of a plural
  into JSON object representation.
* ***transform_booleans*** - Transforms boolean strings into true Boolean types
  (blank values become NULL)

***Custom Transformations***

To create a custom transformation, define a new function in
`dataload/transformations.py`. For example, if you need to convert apples to
oranges:

```python
def transform_apples(value):
    if value == "apples":
        return "oranges"
    else:
        return value
```

You can then add that transformation to a CSV column using the
`add_transformation` method of the CSV reader object in `janrain-dataload.py`:

```python
reader.add_transformation("favoriteFruit", transform_apples)
```


Logging
-------

The utility uses the standard Python
[logging](https://docs.python.org/3.3/library/logging.html) module for both the
application log and the result logs.

* `stdout` - Application log at the INFO log level.
* `log.txt` - Application log (same as stdout) at the DEBUG log level.
* `fail.csv` - Result log for records which failed to get imported.
* `janrain-dataload.log` - Result log for records which were successfully imported.

The formatting, filenames, log level, and other parameters can be configured for
the various loggers using the configuration file `logging_config.json`. See the
[Python Logging HowTo](https://docs.python.org/3/howto/logging.html) for details.


Tips and Best Practices
-----------------------

* Use automation to generate the test data files to ensure that the exact same
  processes can generate data files for a production run.
* Adjust date/time values into UTC.
* Fine tune batch sizes with the `--batch-size` argument rather than
  increasing the `--timeout` argument. API calls should not take longer than 10
  seconds.
* Keep the rate of the data load *well under* the application rate limit so as
  not to impact API usage from other sources (especially on production
  environments).
* Use the `tail -f` command on the result logs to keep an eye on API calls which
  are failing. If you see the 510 (rate limit) error code you need to reduce the
  `--rate-limit` and/or `--workers` arguments.
* Running the utility on a very large set of data may take a while. Try running
  the script from [screen](http://www.gnu.org/software/screen/manual/screen.html)
  session on a server.
