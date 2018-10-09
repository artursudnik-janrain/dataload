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
import csv
from dataload.reader import *
from dataload.cli import DataLoadArgumentParser
from transformations import *
from janrain.capture import ApiResponseError
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger(__file__)

if sys.version_info[0] < 3:
    logger.error("Error: janrain-dataload requires Python 3.")
    sys.exit(1)


# --- Global Variables ---------------------------------------------------------
## used for calculating progress and status of the migration during run

global totalErrorCount
totalErrorCount = 0

global totalBatchCount
totalBatchCount = 0

global total500Count 
total500Count = 0

global extraRateLimit
extraRateLimit =  0

global new_min_time
new_min_time = 0


# --- Results Logging ----------------------------------------------------------

# The following functions are used to process a result from an entity.bulkCreate
# API call or an error message and log the result for each record in a batch to
# the appropriate CSV log file.
#
# Successfully loaded records are logged to 'success.csv' and records which
# fail are logged to 'fail.csv'.  Some records that fail due to certain API errors 
# are logged in 500.csv, which is in a format that can be manually retried by dataload.   

success_logger = logging.getLogger("success_logger")
fail_logger = logging.getLogger("fail_logger")

def log_error(batch, error_message):
    """
    Log a row to the failure CSV log file.

    Args:
        batch          - A dataload.reader.CsvBatch instance
        error_message  - Error message describing why the row was not imported
    """

    # remove email from this block if it is not being imported as a unique attribute

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

def log_result(batch, result):
    """
    Log a row for each record in a batch result to the success or failure CSV
    log. A single batch in a result may contain both records that succeeded and
    record that did not succeed.

    Args:
        batch   - A dataload.reader.CsvBatch instance
        result  - A dictionary representing the JSON result from the Janrain API
    """

    # remove email from this block if it is not being imported as a unique attribute

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

def count_result(result):
    successCount = 0
    errorCount = 0
    for item in result['results']:
        try: 
            iter(item)
            if 'error' in item:
                errorCount = errorCount + 1
            else:    
                successCount = successCount +1
        except TypeError:
            if item > 0:
                successCount = successCount +1
    return errorCount   

def log_500(file,batch): 
    ### write 500+ errors to another file in same format is import csv so that it can be retried with janrain dataload
    ## this should be refactored not to consume log flag 

    write_csvfile = open(file, 'a', newline='')
    filewriter = csv.writer(write_csvfile, delimiter=',',quotechar='"', quoting=csv.QUOTE_ALL)
    row_num = batch.start_line
    global rawRows
    for record in (batch.records):
        row = rawRows[row_num]
        if row_num == batch.start_line:
            logger.info("Writing Batch #"+str(batch.id)+" to 500 exception csv")
        filewriter.writerow(row)
        del rawRows[row_num]
        row_num  = row_num + 1
    
    write_csvfile.close()

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

    parser = DataLoadArgumentParser()  ## make additional arguments available inside this function.  Refactor : use kwargs instead.
    args = parser.parse_args()

    last_time = time.time()
    logger.info("Batch #{} (lines {}-{})" \
        .format(batch.id, batch.start_line, batch.end_line))

    global totalErrorCount 
    global totalBatchCount
    global totalLines
    global total500Count

    batchCount = batch.end_line - batch.start_line + 1
    totalBatchCount = batchCount + totalBatchCount

    linesLeft = totalLines - totalBatchCount


    ## make entity bulk create call, log results in fail.csv , success.scv or 500.csv 


    log = 0

    if dry_run:
        log_error(batch, "Dry run. Record was skipped.")
    else:
        try:
            result = api.call('entity.bulkCreate', type_name=type_name,
                              timeout=timeout, all_attributes=batch.records)
            log_result(batch, result)
            totalErrorCount = count_result(result) + totalErrorCount
        except ApiResponseError as error:
            error_message = "API Error {}: {}".format(error.code, str(error))
            if ((error.code == 510) or (error.code == 403) or (error.code == 500) or (error.code == 504)): 
                logger.info("500 exception block - API")
                log = 1
                total500Count = total500Count + batchCount
                if error.code == 510:                        ### detect API rate limiting from capture
                    global extraRateLimit
                    extraRateLimit =  extraRateLimit + 1     ### increment extra rate limit global variable.  Each thread will now sleep +1 more seconds before working again
            else:
                log_error(batch, error_message)
                totalErrorCount = batchCount + totalErrorCount
            logger.warn(error_message)
        except requests.HTTPError as error:
            if ((error.response.status_code > 499) or (error.response.status_code == 403)):
                logger.info("500 exception block - HTTP")
                log = 1
                total500Count = total500Count + batchCount
            else:
                log_error(batch, str(error)) 
                totalErrorCount = batchCount + totalErrorCount
            logger.warn(str(error))                      
        if log == 1:
            log_500("500.csv",batch)   #### write to 500 csv 

    errorRate = totalErrorCount/totalBatchCount * 100
    errorRateDisplay = args.error_rate_display_interval

    if batch.id % errorRateDisplay == 0:     ####   calculate update during run at interval specified in arguments
             
        global startTime

        currentTime = time.time()
        elapsedTimeSec = currentTime - startTime
        processRate = (totalBatchCount/elapsedTimeSec)*60

        remainingTimeMin = (linesLeft/processRate)
        elapsedTimeMin = elapsedTimeSec *.016667
        remainingTimeMinRound = round(remainingTimeMin,1)
        elapsedTimeMinRound = round(elapsedTimeMin,1)
        processRateRound = round(processRate,1)
        errorRateRound = round(errorRate,4)

        logger.info("Time Elapsed: "+str(elapsedTimeMinRound)+" mins  "+"500 error Count: "+str(total500Count)+"  Error Count: "+str(totalErrorCount)+"/"+str(totalBatchCount)+"  "+str(errorRateRound)+" %   "+str(linesLeft)+" records to process  "+str(processRateRound)+ " records per minute  "+str(remainingTimeMinRound)+" mins remaining" )   

    logger.info("Batch #{} complete" \
        .format(batch.id))

    # As a very crude rate limiting mechanism, sleep if processing the batch
    # did not use all of the minimum time.
    # add extra rate limit in to total sleep time

    global new_min_time

    if log == 1:
        new_min_time = min_time + 60
        logger.info("Pausing to clear API congestion.  Extra Rate Limit: "+str(extraRateLimit))
    else:
        if new_min_time > min_time:
            new_min_time = new_min_time - 1 
        else:
            new_min_time = min_time + extraRateLimit
    if (time.time() - last_time) < new_min_time:
        sleepTime = new_min_time - (time.time() - last_time) + extraRateLimit
        sleepTimeRound = round(sleepTime,2)
        if sleepTime > 0:
            logger.info("Sleep: "+str(sleepTimeRound))
            time.sleep(sleepTime)

def main():
    
    """ Main entry point for script being executed from the command line. """
    parser = DataLoadArgumentParser()
    args = parser.parse_args()

    ###### create 500.csv from header 

    read_csvfile = open(args.data_file, 'r+', newline='', encoding='utf-8')
    newFile = csv.reader(read_csvfile, delimiter=',', quotechar='|')
    newHeader = next(newFile)
    logger.info("Counting total lines" )        
    global totalLines
    totalLines = sum(1 for line in read_csvfile)
    read_csvfile.close()
    logger.info(str(totalLines)) 

    global startTime
    startTime = time.time()

    write_csvfile = open('500.csv' , 'w')
    fieldnames = newHeader
    filewriter = csv.DictWriter(write_csvfile, fieldnames=fieldnames, delimiter=',',
            quotechar='"', quoting=csv.QUOTE_MINIMAL)
    filewriter.writeheader()
    write_csvfile.close()

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
                time.sleep(args.MAX_QUEUE_SIZE_SLEEP)

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
    logging.config.dictConfig(config)

    # Add header row the the success and failure CSV logs
    # email is added here because it is usually a unique identifier for import.  Remove email if it isn't a unique attribute being imported.
    success_logger.info("batch,line,uuid,email")
    fail_logger.info("batch,line,email,error")    

    main()
