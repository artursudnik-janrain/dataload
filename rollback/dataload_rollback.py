"""
File to handle the rollback process in order to delete users added.
"""

import logging
import logging.config
import time
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Lock

import requests
from janrain.capture import ApiResponseError
from tqdm import tqdm

from utils.reader import CsvReader
from utils.utils import count_lines_in_file, rate_limiter

logger = logging.getLogger(__file__)

success_logger = logging.getLogger("success_rollback_logger")
fail_logger = logging.getLogger("fail_rollback_logger")

lock = Lock()
success_count = 0
fail_count = 0


def dataload_rollback(args, api, configs):
    """
    Creates threads to delete records that were imported.

    Args:
        args: arguments captured from CLI
        api: object to perform the API calls
        configs: shared configuration variables used across the script
    """
    print("\n\nStarting the rollback process.\n")

    data_file = args.data_file
    record_count = count_lines_in_file(data_file)

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        logger.info("Loading data from file into the '{}' entity type."
                    .format(args.type_name))

        print("\tValidating UTF-8 encoding and checking for Byte Order Mark\n")
        # Create a CSV reader which will read the CSV file and return a line.
        reader = CsvReader(data_file)

        # TQDM Progress Bar.
        pbar = tqdm(total=record_count, unit="rec")
        pbar.set_description("Delete Records.")

        # Calculate minimum time per worker thread
        if args.rate_limit > 0:
            min_time = round(args.workers / args.rate_limit, 2)
        else:
            min_time = 0
        logger.debug("Minimum processing time per worker: {}".format(min_time))

        # Iterate over records of rows in the CSV and dispatch delete_record()
        # calls to the worker threads.
        futures = []
        for _, row in enumerate(reader):
            kwargs = {
                'api': api,
                'args': args,
                'uuid': row[2],
                'email': row[3],
                'batch_id': row[0],
                'line': row[1],
                'pbar': pbar,
                'min_time': min_time
            }
            futures.append(executor.submit(delete_record, **kwargs))

        # Iterate over the future results to raise any uncaught exceptions.
        # Note that this means uncaught exceptions will not be raised until
        # AFTER all workers are dispatched.
        logger.info("Waiting for workers to finish")
        for future in futures:
            future.result()

        pbar.close()
        logger.info("Rollback finished!")


def delete_record(api, args, uuid, email, batch_id, line, pbar, min_time):
    """
    Call the entity.delete API endpoint to delete the user record.

    Args:
        api              - A janrain.capture.Api instance
        args             - An dictionary with arguments
            type_name       - Entity type name (eg. "user")
            timeout         - Seconds for the HTTP timeout (10 recommended)
            dry_run         - Set to True to skip making API calls
        batch_id         - The batch identifier of the original batch process
        line             - Original file line
        pbar             - Progress bar object
    """

    start_thread_time = time.time()

    global success_count
    global fail_count
    results = []
    row = {
        'id': batch_id,
        'start_line': line,
        'uuid': uuid,
        'email': email
    }

    try:
        if args.dry_run:
            log_error(row, "Dry run. Skipping delete call.")
            logger.debug("Dry run mode detected. Skipping delete call.")
        else:
            result_delete = api.call(
                'entity.delete',
                type_name=args.type_name,
                uuid=uuid,
                timeout=args.timeout
            )

            results.append(result_delete)
            log_result(row, results)

    except ApiResponseError as error:
        error_message = "API Error {}: {}".format(error.code, str(error))
        logger.warning(error_message)
        log_error(row, error_message)
    except requests.HTTPError as error:
        logger.warning(str(error))
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
        fail_logger.info("{},{},{}".format(
            row['id'],
            row['start_line'],
            error_message
        ))
        with lock:
            fail_count += 1
    except Exception as error:
        logger.error(str(error))


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

        return

    if error_result:
        fail_logger.info("{},{},{}".format(
            row['id'],
            row['start_line'],
            error_msg
        ))
        with lock:
            fail_count += 1
        return

    success_logger.info("{},{},{},{}".format(
        row['id'],
        row['start_line'],
        row['uuid'],
        row['email']
    ))
    with lock:
        success_count += 1


def finalize(args, api, configs):
    """
    Analyzes the results of previous steps and summarize in output

    Args:
        args: arguments captured from CLI
        api: object to perform the API calls
        configs: shared configuration variables used across the script
    """

    logger.info("Checkign results")
    print("\nDATALOAD ROLLBACK RESULTS")

    success_result = configs["success_rollback_handler_filename"]
    fail_result = configs["fail_rollback_handler_filename"]

    print("\t[{}] Total processed users\n".format(configs['total_records']))

    print("\t[{}] Delete success. Number of records deleted in database"
          .format(count_lines_in_file(success_result)))
    print("\t[{}] Delete failures".format(count_lines_in_file(fail_result)))

    result_files = [success_result, fail_result]

    result = api.call('entity.count', type_name=args.type_name,
                      timeout=args.timeout)
    print("\t[{}] Total number of records in Entity Type [{}] after execution"
          .format(result["total_count"], args.type_name))

    print("\nPlease check detailed results in the files below:")
    for file in result_files:
        print("\t{}".format(file))
