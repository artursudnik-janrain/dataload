"""
File to handle the records update in case script was executed with delta flag
"""
import ast
import json
import logging
import logging.config
import time
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Lock

import requests
from janrain.capture import ApiResponseError
from tqdm import tqdm

from utils.reader import CsvReader
from utils.utils import count_lines_in_file, delete_file, rate_limiter

logger = logging.getLogger(__file__)


update_success_logger = logging.getLogger("update_success_logger")
update_fail_logger = logging.getLogger("update_fail_logger")

lock = Lock()
success_count = 0
fail_count = 0


def dataload_update(args, api, configs):
    """
    Creates threads to update records if any of them were marked
    as duplicates during import

    Args:
        args: arguments captured from CLI
        api: object to perform the API calls
        configs: shared configuration variables used across the script
    """
    print("\n\nStarting the update process for the duplicated records\n")
    if args.dry_run:
        logger.debug("Dry run. Dataload update was skipped.")
        print("\tDry run mode detected. Skipping dataload update.")
        return

    if not args.delta_migration:
        return

    logger.info("Checking if there are any duplicate records to update")
    print("\tChecking if there are any duplicate records to update\n")
    data_file = configs['csv_tmp_writer'].get_filename()
    record_update_count = count_lines_in_file(data_file)
    plurals = configs['plurals']

    # Check if there is any record to be updated. If none, delete the temporary
    # file and proceed to finalize
    if record_update_count < 1:
        print("\tNo records found to be updated\n")
        logger.info("No records found to be updated")
        delete_file(data_file, logger)
        return
    print("\t{} duplicate records were found and will be updated\n"
          .format(record_update_count))

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        logger.info("Loading data from TEMP file into the '{}' entity type."
                    .format(args.type_name))

        # Calculate minimum time per worker thread
        min_time = 0
        if args.rate_limit > 0:
            min_time = round(args.workers / args.rate_limit, 2)

        logger.debug("Minimum processing time per worker: {}".format(min_time))

        print("\tValidating UTF-8 encoding and checking for Byte Order Mark\n")
        # Create a CSV reader which will read the CSV TEMP file and return
        # a entire record.
        reader = CsvReader(data_file)

        # TQDM Progress Bar.
        pbar = tqdm(total=record_update_count, unit="rec")
        pbar.set_description("Updating Records.")

        # Iterate over records of rows in the CSV and dispatch update_record()
        # calls to the worker threads.
        futures = []
        for _, row in enumerate(reader):
            logger.debug(row)
            record_info = {
                'record': row[2],
                'batch_id': row[0],
                'line': row[1]
            }

            kwargs = {
                'api': api,
                'args': args,
                'record_info': record_info,
                'min_time': min_time,
                'pbar': pbar,
                'plurals': plurals
            }
            futures.append(executor.submit(update_record, **kwargs))

        # Iterate over the future results to raise any uncaught exceptions.
        # Note that this means uncaught exceptions will not be raised until
        # AFTER all workers are dispatched.
        logger.info("Waiting for workers to finish")
        for future in futures:
            future.result()

        pbar.close()
        logger.info("Update finished!")

        # Delete the temporary file.
        delete_file(data_file, logger)


def update_record(api, args, record_info, min_time, pbar, plurals):
    """
    Call the entity.update API endpoint to update user record.

    Args:
        api              - A janrain.capture.Api instance
        args             - An dictionary with arguments
            type_name       - Entity type name (eg. "user")
            timeout         - Seconds for the HTTP timeout (10 recommended)
            dry_run         - Set to True to skip making API calls
            delta_migration - Set to True to update duplicate records
        record_info      - A dict with original record info.
        min_time         - Minimum number of seconds to wait before returning
        pbar             - Progress bar object
        plurals          - A list with plural fields that must be updated
    """

    start_thread_time = time.time()

    global success_count
    global fail_count

    # Convert the record into a dict so we can use just one argument.
    json_data = json.dumps(ast.literal_eval(record_info['record']))
    json_data_loaded = json.loads(json_data)
    row = {
        'id': record_info['batch_id'],
        'start_line': record_info['line'],
        'primary_key': args.primary_key,
        'record': [json_data],
        'email': json_data_loaded['email']
    }
    try:
        # We must prepare the data before try to update the record.
        primary_key = json_data_loaded[args.primary_key]
        primary_key_value = json.dumps(primary_key)
        json_data = prepare_update_record(json_data)
        results = []

        result_update = api.call('entity.update', type_name=args.type_name,
                                 key_value=primary_key_value,
                                 key_attribute=args.primary_key,
                                 timeout=args.timeout, value=json_data)
        results.append(result_update)

        log_result(row, results)
    except ApiResponseError as error:
        error_message = "API Error {}: {} on Line #{}".format(
            error.code, str(error), record_info['line'])
        logger.warning(error_message)
        log_error(row, error_message)
    except requests.HTTPError as error:
        error_message = "{} on Line #{}".format(str(error),
                                                record_info['line'])
        logger.warning(error_message)
        log_error(row, str(error))
    pbar.update(1)
    pbar.set_description("Success:{} Fail:{}".format(
        success_count,
        fail_count
    ))

    # As a very crude rate limiting mechanism, sleep if processing the batch
    # did not use all of the minimum time.
    rate_limiter(start_thread_time, min_time)


def log_error(row, error_message):
    """
    Log a row to the failure CSV log file.

    Args:
        row            - A dictionary with original row info
        error_message  - Error message describing why the row was not imported
    """
    global fail_count

    try:
        update_fail_logger.info("{},{},{},{}".format(
            row['id'],
            row['start_line'],
            row['email'],
            error_message
        ))
        with lock:
            fail_count += 1
    except Exception as error:
        logger.error(str(error))


def prepare_update_record(record):
    """
    Remove unecessary/forbidden attributes from record so it's possible to
    reuse on the entity.update API call.
    """
    json_loaded = json.loads(record)
    return json.dumps(json_loaded)


def result_has_error(results):
    """
    Check the results list for any possible error and return a tuple which
    contains the status and error message. If the record contains a Plural
    attribute, multiple API calls may be performed for a single record.

    Args:
        results: List of API responses

    Returns:
        error_stat: Boolean to check if the expected 'stat' index exists in
            response
        error_result: Boolean for the status of the API call
        error_msg: Error message String
    """
    for result in results:
        if 'stat' not in result:
            return True, False, "Unexpected API response"
        elif result['stat'] == "error":
            return False, True, result['error_description']
    return False, False, ""


def log_result(row, results):
    """
    Log a row for each record result to the success or failure CSV log.

    Args:
        row       - A dictionary with original row info
        results   - A list of result dictionary from the API calls
    """
    # Must use global variables so each thread can increment it. Using Lock()
    # so it's thread-safe
    global success_count
    global fail_count

    error_stat, error_result, error_msg = result_has_error(results)

    if error_stat:
        logger.error(error_msg)
    elif error_result:
        update_fail_logger.info("{},{},{},{}".format(
            row['id'],
            row['start_line'],
            row['email'],
            error_msg
        ))
        with lock:
            fail_count += 1
    else:
        record = json.loads(row['record'][0])
        update_success_logger.info("{},{},{}".format(
            row['id'],
            row['start_line'],
            record[row['primary_key']]
        ))
        with lock:
            success_count += 1
