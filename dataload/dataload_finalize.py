"""
File to handle the final output
"""
import logging
import logging.config

from utils.utils import count_lines_in_file, delete_file

logger = logging.getLogger(__file__)

success_logger = logging.getLogger("success_logger")
fail_logger = logging.getLogger("fail_logger")
update_success_logger = logging.getLogger("update_success_logger")
update_fail_logger = logging.getLogger("update_fail_logger")


def dataload_finalize(args, api, configs):
    """
    Analyzes the results of previous steps and summarize in output

    Args:
        args: arguments captured from CLI
        api: object to perform the API calls
        configs: shared configuration variables used across the script
    """

    logger.info("Checkign results")
    print("\nDATALOAD RESULTS")

    success_result = configs["success_handler_filename"]
    fail_result = configs["fail_handler_filename"]
    retry_result = configs["csv_retry_writer"].get_filename()

    print("\t[{}] Total processed users\n".format(configs['total_records']))

    print("\t[{}] Import success. Number of new records inserted in database"
          .format(count_lines_in_file(success_result)))
    print("\t[{}] Import failures".format(count_lines_in_file(fail_result)))

    result_files = [success_result, fail_result]

    # If retry file is not empty, add it to the result list and print the info,
    # otherwise, remove the file.
    retry_line_number = count_lines_in_file(retry_result)
    if retry_line_number > 0:
        print("\t[{}] Import retries\n".format(count_lines_in_file(
            retry_result)))
        result_files.append(retry_result)
    else:
        print("\n")
        retry_filename = configs["csv_retry_writer"].get_filename()
        delete_file(retry_filename, logger)

    # Delta migration is enable, get the update log files.
    if args.delta_migration:
        # Append to an existing logger list.
        update_success_result = configs["update_success_handler_filename"]
        update_fail_result = configs["update_fail_handler_filename"]
        result_files.extend((update_success_result,
                             update_fail_result))

        print("\t[{}] Update success. Existing users that were updated"
              .format(count_lines_in_file(update_success_result)))
        print("\t[{}] Update failures\n".format(count_lines_in_file(
            update_fail_result)))

    result = api.call('entity.count', type_name=args.type_name,
                      timeout=args.timeout)
    print("\t[{}] Total number of records in Entity Type [{}] after execution"
          .format(result["total_count"], args.type_name))

    print("\nPlease check detailed results in the files below:")
    for file in result_files:
        print("\t{}".format(file))
