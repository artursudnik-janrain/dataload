#!/usr/bin/env python3
"""
Command-line tool to rollback user profiles loaded in a previous
dataload execution from a CSV data source.
"""

import datetime
import json
import logging
import logging.config
import sys

from utils.utils import count_lines_in_file
from utils.cli import RollbackArgumentParser
from rollback.dataload_rollback import dataload_rollback, finalize

logger = logging.getLogger(__file__)

if sys.version_info[0] < 3:
    logger.error("Error: dataload_rollback requires Python 3.")
    sys.exit(1)

success_logger = logging.getLogger("success_rollback_logger")
fail_logger = logging.getLogger("fail_rollback_logger")


def setup_logging():
    dataload_config = {}
    logging_config_file = "logging_rollback_config.json"

    # Setup logging based on the configuration in
    # 'logging_rollback_config.json'.
    # See: https://docs.python.org/3/howto/logging.html
    with open(logging_config_file, 'r') as f:
        config = json.loads(f.read())
        format_date = datetime.datetime.now().strftime("%b_%d_%Y_%H_%M_%S")

        # Define the final log filename, using the pattern on config.
        log_handlers = ['success_rollback_handler', 'fail_rollback_handler']

        for handler in log_handlers:
            filename = config["handlers"][handler]["filename"]
            filename_key = "{}_filename".format(handler)
            config["handlers"][handler]["filename"] = filename.format(
                format_date)
            dataload_config.update({
                filename_key: config["handlers"][handler]["filename"]
            })

    logging.config.dictConfig(config)

    # Add header row the the success and failure CSV logs
    success_logger.info("batch,line,uuid,email")
    fail_logger.info("batch,line,error")

    return dataload_config


if __name__ == "__main__":
    """ Main entry point for script being executed from the command line. """
    parser = RollbackArgumentParser()
    args = parser.parse_args()
    api = parser.init_api()

    dataload_config = setup_logging()

    # Calculating total number of records to be processed and store metric
    total_records = count_lines_in_file(args.data_file)

    dataload_config.update({'total_records': total_records})

    kwargs = {
        "args": args,
        "api": api,
        "configs": dataload_config
    }

    dataload_rollback(**kwargs)
    finalize(**kwargs)
