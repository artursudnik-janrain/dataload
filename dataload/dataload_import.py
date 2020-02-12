"""
File to handle the dataload import
"""
import logging
import logging.config
import time
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Lock

import requests
from janrain.capture import ApiResponseError
from tqdm import tqdm

from utils.reader import CsvBatchReader
from utils.utils import rate_limiter
from transformations import (transform_boolean, transform_date,
                             transform_gender, transform_password,
                             transform_plural)

logger = logging.getLogger(__file__)

success_logger = logging.getLogger("success_logger")
fail_logger = logging.getLogger("fail_logger")

lock = Lock()
success_count = 0
fail_count = 0
retry_count = 0


def dataload_import(args, api, configs):
    """
    Creates threads to import records according to arguments provided

    Args:
        args: arguments captured from CLI
        api: object to perform the API calls
        configs: shared configuration variables used across the script
    """

    print("\n\nStarting the dataload import\n")

    # The CSV file is processed faster than API calls can be made. When
    # loading large amounts of records this can result in a work queue that
    # uses up a very large amount of memory. The optimal queue size limit is
    # the nearly the same as the maximum concurrent API calls (API Limit).
    # Setting as 2 times it to have an extra buffer.
    queue_maxsize = 2 * args.rate_limit

    # Metric to Progress bar(AVG per minute of imported records)
    start_time = time.time()
    configs["start_time"] = start_time

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        print("\tLoading data from {} into the '{}' entity type\n"
              .format(args.data_file, args.type_name))

        logger.info("Loading data from {} into the '{}' entity type"
                    .format(args.data_file, args.type_name))

        # Create a CSV "batch" reader which will read the CSV file in batches
        # of records converted to the JSON structure expected by the API.
        print("\tValidating UTF-8 encoding and checking for Byte Order Mark\n")
        reader = CsvBatchReader(args.data_file, args.batch_size, args.start_at)

        # Add header to the retry file
        header = reader.get_header()
        csv_retry_writer = configs['csv_retry_writer']
        csv_retry_writer.write_row(header)

        # Any column in the CSV file can have a "transformation" function
        # defined to transform that data into the format needed for the API to
        # consume that data. See the example transformations in the
        # file: transformations.py
        reader.add_transformation("password", transform_password)
        reader.add_transformation("birthday", transform_date)
        reader.add_transformation("gender", transform_gender)
        reader.add_transformation("optIn.status", transform_boolean)
        reader.add_transformation("clients", transform_plural)
        reader.add_transformation("shippingAddresses", transform_plural)

        if args.delta_migration:
            # Get the plural fields to be updated
            logger.debug("Updating config with plural fields")
            plurals_to_update = reader.get_plurals()
            configs.update({'plurals': plurals_to_update})

        # Calculate minimum time per worker thread
        if args.rate_limit > 0:
            min_time = round(args.workers / args.rate_limit, 2)
        else:
            min_time = 0
        logger.debug("Minimum processing time per worker: {}".format(min_time))

        # Iterate over batches of rows in the CSV and dispatch load_batch()
        # calls to the worker threads.
        futures = []

        # Progress Bar Legends
        print("Labels: Total Success(S) | Total Fails(F) | Total Retries(R) | \
Success Rate(SR) | Average Records per Minute(AVG)\n")

        total_records = configs["total_records"]
        # TQDM Progress Bar.
        pbar = tqdm(total=total_records, unit="rec")
        pbar.set_description("S:- F:- R:- SR:% AVG:-")
        for batch in reader:
            # Adjust throughput of items being added into the queue to optimize
            # memory consumption
            queue_size = executor._work_queue.qsize()

            while queue_size >= queue_maxsize:
                logger.debug("Maximum queue size reached, waiting 1 second.")
                time.sleep(1)
                queue_size = executor._work_queue.qsize()

            logger.debug(batch.records)
            logger.debug(batch.original_records)
            kwargs = {
                'api': api,
                'batch': batch,
                'args': args,
                'configs': configs,
                'min_time': min_time,
                'pbar': pbar
            }
            futures.append(executor.submit(load_batch, **kwargs))

        # Iterate over the future results to raise any uncaught exceptions.
        # Note that this means uncaught exceptions will not be raised until
        # AFTER all workers are dispatched.
        logger.info("Waiting for workers to finish")
        for future in futures:
            future.result()
        pbar.close()

        configs['csv_retry_writer'].close_file()

        logger.info("Import finished!")

        if args.delta_migration:
            # Close the CSV opened file
            configs['csv_tmp_writer'].close_file()


def log_error(batch, error_message):
    """
    Log a row to the failure CSV log file.

    Args:
        batch          - A utils.reader.CsvBatch instance
        error_message  - Error message describing why the row was not imported
    """
    global fail_count

    try:
        for i in range(len(batch.records)):
            fail_logger.info("{},{},{},{}".format(
                batch.id,
                batch.start_line + i,
                batch.records[i]['email'],
                error_message
            ))
            with lock:
                fail_count += 1
    except Exception as error:
        logger.error(str(error))

# --- Results Logging ---------------------------------------------------------
# The following functions are used to process a result from an
# entity.bulkCreate API call or an error message and log the result for each
# record in a batch to the appropriate CSV log file.
#
# Successfully loaded records are logged to 'logs/success.csv' and records
# which fail are logged to 'logs/fail.csv'.


def log_result(batch, result, delta_migration, configs):
    """
    Log a row for each record in a batch result to the success or failure CSV
    log. A single batch in a result may contain both records that succeeded and
    record that did not succeed.

    Args:
        batch           - A utils.reader.CsvBatch instance
        result          - A dict representing the JSON result from the API
        delta_migration - Define if duplicate entites must be updated
        configs         - The dataload config dict for loggers and files
    """
    # Must use global variables so each thread can increment it. Using Lock()
    # so it's thread-safe
    global success_count
    global fail_count

    if 'stat' not in result or result['stat'] != 'ok':
        logger.error("Unexpected API response")
        return

    for i, uuid_result in enumerate(result['uuid_results']):
        if isinstance(uuid_result, dict) and uuid_result['stat'] == "error":
            # If error is unique_violation and delta_migration arg is
            # enable We must skip the fail log and use the update log file.
            if uuid_result['error'] == "unique_violation" and delta_migration:
                configs['csv_tmp_writer'].write_row([batch.id,
                                                    batch.start_line + i,
                                                    batch.records[i]])
            else:
                fail_logger.info("{},{},{},{}".format(
                    batch.id,
                    batch.start_line + i,
                    batch.records[i]['email'],
                    uuid_result['error_description']
                ))
            with lock:
                fail_count += 1
        else:
            success_logger.info("{},{},{},{}".format(
                batch.id,
                batch.start_line + i,
                uuid_result,
                batch.records[i]['email']
            ))
            with lock:
                success_count += 1


def handle_exception(message, code, batch, configs, batch_size, type):
    global retry_count

    error_message = "{} on Batch #{}".format(message, batch.id)
    logger.warning(error_message)

    error_codes = configs['error_codes']
    if code in error_codes[type] or type not in error_codes:
        for _, record in enumerate(batch.original_records):
            configs['csv_retry_writer'].write_row(record)
        with lock:
            retry_count += batch_size
    else:
        log_error(batch, message)


def load_batch(api, batch, args, configs, min_time, pbar):
    """
    Call the entity.bulkCreate API endpoint to create a batch of user records.

    Args:
        api              - A janrain.capture.Api instance
        batch            - A utils.reader.CsvBatch instance
        args             - An dictionary with arguments
            type_name       - Entity type name (eg. "user")
            timeout         - Seconds for the HTTP timeout (10 recommended)
            dry_run         - Set to True to skip making API calls
            delta_migration - Set to True to update duplicate records
        configs          - The dataload config dict for loggers and files
        min_time         - Minimum number of seconds to wait before returning
    """
    start_thread_time = time.time()

    global success_count
    global fail_count

    logger.info("Batch #{} (lines {}-{})"
                .format(batch.id, batch.start_line, batch.end_line))

    if args.dry_run:
        log_error(batch, "Dry run. Record was skipped.")
    else:
        try:
            result = api.call('entity.bulkCreate', type_name=args.type_name,
                              timeout=args.timeout,
                              all_attributes=batch.records)
            log_result(batch, result, args.delta_migration, configs)
        except ApiResponseError as error:
            error_message = "API Error {}: {}".format(error.code, str(error))
            handle_exception(error_message, error.code, batch, configs,
                             args.batch_size, 'api')
        except requests.HTTPError as error:
            error_message = str(error)
            error_code = error.response.status_code
            handle_exception(error_message, error_code, batch, configs,
                             args.batch_size, 'http')
    pbar.update(len(batch.records))
    end_time = time.time()
    start_time = configs["start_time"]

    # Data for Progress bar.
    total_processed = int(success_count + fail_count + retry_count)

    success_rate = round(100 - (((fail_count + retry_count) / total_processed)
                                * 100), 2)

    avg_records_per_min = round((
        total_processed / ((end_time - start_time) / 60)
    ))

    pbar.set_description(
        "S:{} F:{} R:{} SR:{}% AVG:{}rec/m ".format(
            success_count,
            fail_count,
            retry_count,
            success_rate,
            avg_records_per_min
        ))

    # As a very crude rate limiting mechanism, sleep if processing the batch
    # did not use all of the minimum time.
    rate_limiter(start_thread_time, min_time)
