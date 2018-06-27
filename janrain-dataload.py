#!/usr/bin/env python3
"""
Command-line tool to load user profile data from a CSV data source into Janrain.
"""
import os
import sys
import logging
import logging.config
import json
import time
import requests
from dataload.reader import CsvBatchReader
from dataload.cli import DataLoadArgumentParser
from transformations import *
from janrain.capture import ApiResponseError
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger(__file__)

if sys.version_info[0] < 3:
    logger.error("Error: janrain-dataload requires Python 3.")
    sys.exit(1)


# --- Results Logging ----------------------------------------------------------

# The following functions are used to process a result from an entity.bulkCreate
# API call or an error message and log the result for each record in a batch to
# the appropriate CSV log file.
#
# Successfully loaded records are logged to 'logs/success.csv' and records which
# fail are logged to 'logs/fail.csv'.

success_logger = logging.getLogger("success_logger")
fail_logger = logging.getLogger("fail_logger")
uuid_logger = logging.getLogger("uuid_logger")

def log_error(batch, error_message):
    """
    Log a row to the failure CSV log file.

    Args:
        batch          - A dataload.reader.CsvBatch instance
        error_message  - Error message describing why the row was not imported
    """
    try:
        for i in range(len(batch.records)):
            fail_logger.info("{},{},{},{}".format(
                    batch.id,
                    batch.start_line + i,
                    batch.records[i]['email'],
                    error_message
                )
            )
    except Exception as error:
        logger.error(str(error))

def log_result(api, batch, result):
    """
    Log a row for each record in a batch result to the success or failure CSV
    log. A single batch in a result may contain both records that succeeded and
    record that did not succeed.

    Args:
        batch   - A dataload.reader.CsvBatch instance
        result  - A dictionary representing the JSON result from the Janrain API
    """
    if 'stat' in result and result['stat'] == "ok":
        for i, uuid_result in enumerate(result['uuid_results']):
            if isinstance(uuid_result, dict) and uuid_result['stat'] == "error":
                fail_logger.info("{},{},{},{}".format(
                        batch.id,
                        batch.start_line + i,
                        batch.records[i]['email'],
                        uuid_result['error_description']
                    )
                )
                if uuid_result['error_description'] == "Attempted to update a duplicate value":
                    result = api.call("entity", type_name="user", attribute_name="uuid", key_attribute="email", key_value='"{}"'.format(batch.records[i]['email']))
                    uuid_logger.info("{},{}".format(
                        batch.records[i]['email'],
                        result['result']
                    )
                )
            else:
                success_logger.info("{},{},{},{}".format(
                        batch.id,
                        batch.start_line + i,
                        uuid_result,
                        batch.records[i]['email']
                    )
                )
    else:
        logger.error("Unexpected API response")


# --- MAIN ---------------------------------------------------------------------

# The rest of the script is the main functionality:
#   1. Parse command-line arguments for variables needed to run the data load
#   2. Iterate over the CSV file(s) being loaded
#   3. Dispatch batches of records to worker threads to be loaded into Janrain

def load_batch(api, batch, type_name, timeout, min_time, dry_run):
    """
    Call the entity.bulkCreate API endpoint to create a batch of user records.

    Args:
        api        - A janrain.capture.Api instance
        batch      - A dataload.reader.CsvBatch instance
        type_name  - Janrain entity type name (eg. "user")
        timeout    - Number of seconds for the HTTP timeout (10 recommended)
        min_time   - Minimum number of seconds to wait before returning
        dry_run    - Set to True to skip making API calls
    """
    last_time = time.time()
    logger.info("Batch #{} (lines {}-{})" \
        .format(batch.id, batch.start_line, batch.end_line))

    if dry_run:
        log_error(batch, "Dry run. Record was skipped.")
    else:
        #api.sign_requests = False
        try:
            result = api.call('entity.bulkCreate', type_name=type_name,
                              timeout=timeout, all_attributes=batch.records)
            log_result(api, batch, result)
        except ApiResponseError as error:
            error_message = "API Error {}: {}".format(error.code, str(error))
            logger.warn(error_message)
            log_error(batch, error_message)
        except requests.HTTPError as error:
            logger.warn(str(error))
            log_error(batch, str(error))

    # As a very crude rate limiting mechanism, sleep if processing the batch
    # did not use all of the minimum time.
    if (time.time() - last_time) < min_time:
        time.sleep(min_time - (time.time() - last_time))

def main():
    """ Main entry point for script being executed from the command line. """
    parser = DataLoadArgumentParser()
    args = parser.parse_args()

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        logger.info("Loading data from {} into the '{}' entity type." \
            .format(args.data_file, args.type_name))

        # Create a CSV "batch" reader which will read the CSV file in batches
        # of records converted to the JSON structure expected by the Janrain
        # API.
        reader = CsvBatchReader(args.data_file, args.batch_size, args.start_at)

        # Any column in the CSV file can have a "transformation" function
        # defined to transform that data into the format needed for the Janrain
        # API to consume that data. See the example transformations in the
        # file: transformations.py
        reader.add_transformation("password", transform_password)
        reader.add_transformation("birthday", transform_date)
        reader.add_transformation("profiles", transform_plural)

        # The CSV file is processed faster than API calls can be made. When
        # loading large amounts of records this can result in a work queue that
        # uses up a very large amount of memory. The 'queue_size' argument
        # limits the amount of memory consumed at the cost of pausing script
        # execution.
        executor._work_queue.maxsize = args.queue_size

        # Calculate minimum time per worker thread
        if args.rate_limit > 0:
            min_time = round(args.workers / args.rate_limit, 2)
        else:
            min_time = 0
        logger.debug("Minimum processing time per worker: {}".format(min_time))

        # Iterate over batches of rows in the CSV and dispatch load_batch()
        # calls to the worker threads.
        futures = []
        for batch in reader:
            logger.debug(batch.records)
            kwargs = {
                'api': parser.init_api(),
                'batch': batch,
                'timeout': args.timeout,
                'type_name': args.type_name,
                'min_time': min_time,
                'dry_run': args.dry_run
            }
            futures.append(executor.submit(load_batch, **kwargs))

            # Log a warning if the work queue reaches capacity
            queue_size = executor._work_queue.qsize()
            if queue_size >= args.queue_size:
                logger.warn("Maximum queue size reached: {}".format(queue_size))
                time.sleep(60)

        # Iterate over the future results to raise any uncaught exceptions. Note
        # that this means uncaught exceptions will not be raised until AFTER all
        # workers are dispatched.
        logger.info("Checking results")
        for future in futures:
            future.result()

        logger.info("Done!")


if __name__ == "__main__":
    # Setup logging based on the configuration in 'logging_config.json'.
    # See: https://docs.python.org/3/howto/logging.html
    with open("logging_config.json", 'r') as f:
        config = json.loads(f.read())
        config["handlers"]["success_handler"]["filename"] = "success_{}.csv".format(time.strftime("%a-%b-%d-%H.%M.%S"))
        config["handlers"]["fail_handler"]["filename"] = "fail_{}.csv".format(time.strftime("%a-%b-%d-%H.%M.%S"))
        config["handlers"]["uuid_handler"]["filename"] = "uuid_{}.csv".format(time.strftime("%a-%b-%d-%H.%M.%S"))
    logging.config.dictConfig(config)

    # Add header row the the success and failure CSV logs
    success_logger.info("batch,line,uuid,email")
    fail_logger.info("batch,line,email,error")
    uuid_logger.info("email,uuid")
    main()
