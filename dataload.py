#!/usr/bin/env python3
"""
Command-line tool to load user profile data from a CSV data source into
Entity Type.
"""
import datetime
import json
import logging
import logging.config
import sys
import tempfile

from utils.cli import DataLoadArgumentParser
from dataload.dataload_finalize import dataload_finalize
from dataload.dataload_import import dataload_import
from dataload.dataload_update import dataload_update
from utils.reader import CsvWriter
from utils.utils import count_lines_in_file

logger = logging.getLogger(__file__)

if sys.version_info[0] < 3:
    logger.error("Error: dataload requires Python 3.")
    sys.exit(1)


def set_logger_config(args, dataload_config):
    # Get the datetime formatted to use on filenames
    format_date = datetime.datetime.now().strftime("%b_%d_%Y_%H_%M_%S")

    # Setup logging based on the configuration in 'logging_config.json'.
    # See: https://docs.python.org/3/howto/logging.html
    with open("logging_config.json", 'r') as f:
        config = json.loads(f.read())
        # Define the final log filename, using the pattern on config.
        log_handlers = ['success_handler', 'fail_handler']
        # Mapping for update handlers and loggers to be appended when using the
        # delta migration argument or removed if not.
        log_update = {
            "update_success_handler": "update_success_logger",
            "update_fail_handler": "update_fail_logger"
        }

        # Check if the delta migration is enable to add the logs.
        if args.delta_migration:
            for key, value in log_update.items():
                log_handlers.append(key)
        else:
            # Remove the handlers and loggers before generate the files.
            # It's necessary to remove both to prevent any issue.
            for key, value in log_update.items():
                del config["handlers"][key]
                del config["loggers"][value]

        for handler in log_handlers:
            filename = config["handlers"][handler]["filename"]
            filename_key = "{}_filename".format(handler)
            config["handlers"][handler]["filename"] = filename.format(
                format_date)
            dataload_config.update({filename_key:
                                   config["handlers"][handler]["filename"]})

    # Initialize the logging using the config
    logging.config.dictConfig(config)

    # Initialize the loggers add header row the the success and failure
    # CSV logs.
    success_logger = logging.getLogger("success_logger")
    fail_logger = logging.getLogger("fail_logger")
    success_logger.info("batch,line,uuid,email")
    fail_logger.info("batch,line,email,error")

    # Update the dataload config with total records count
    prepare_pbar_total_records(args, dataload_config)

    # Create the retry file writer.
    retry_filename = 'retry_{}.csv'.format(format_date)
    csv_retry_writer = CsvWriter(retry_filename)

    # Update the dataload config with retry file writer
    dataload_config.update({'csv_retry_writer': csv_retry_writer})

    # If delta migration argument is enable, we must start the logging handlers
    # and files. Also we must create a temporary file for possible updates.
    if args.delta_migration:
        prepare_delta_migration(dataload_config)


def prepare_delta_migration(dataload_config):
    # The temporary file can't be delete, because we must use it to
    # populate with the entities to be updated.
    update_tmp_file = tempfile.NamedTemporaryFile(delete=False, mode='w', newline='')
    update_tmp_file_name = update_tmp_file.name

    # Initialize the CSV Temp file writer and add the header on file
    csv_tmp_writer = CsvWriter(update_tmp_file_name)
    header = ['batch', 'original_line', 'record']
    csv_tmp_writer.write_row(header)

    # Update the dataload config with tmp file writer
    dataload_config.update({'csv_tmp_writer': csv_tmp_writer})

    # Initialize the update loggers and add header row the the success and
    # failure CSV logs.
    update_success_logger = logging.getLogger("update_success_logger")
    update_fail_logger = logging.getLogger("update_fail_logger")
    update_success_logger.info("batch,line,{}".format(args.primary_key))
    update_fail_logger.info("batch,line,email,error")


def prepare_pbar_total_records(args, dataload_config):
    # Calculating total number of records to be processed and store metric
    total_records = count_lines_in_file(args.data_file)

    if args.start_at > total_records:
        logger.info("No records to be imported after {}".format(
            total_records))
        print("\tNo records to be imported after {} records.".format(
            total_records))
        sys.exit()

    # Check if start at arg is > than number of records in csv file.
    if args.start_at > 1 and args.start_at <= total_records:
        total_records = (total_records - args.start_at) + 1

    dataload_config.update({'total_records': total_records})


if __name__ == "__main__":
    """ Main entry point for script being executed from the command line. """
    parser = DataLoadArgumentParser()
    args = parser.parse_args()
    api = parser.init_api()

    dataload_config = {
        'error_codes': {
            'api': [403, 500, 504, 510],
            'http': [403, 500, 501, 502]
        }
    }

    # Set the logger's configuration for dataload.
    set_logger_config(args, dataload_config)

    kwargs = {
        "args": args,
        "api": api,
        "configs": dataload_config
    }

    dataload_import(**kwargs)

    dataload_update(**kwargs)

    dataload_finalize(**kwargs)
