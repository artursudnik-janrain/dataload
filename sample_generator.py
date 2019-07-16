#!/usr/bin/env python3
"""
Command-line tool to generate a sample to be loaded using the dataload.
"""
import json
import sys

from tqdm import tqdm

import sample
from utils.cli import SampleGeneratorArgumentParser

if sys.version_info[0] < 3:
    sys.exit(1)


def main():
    """ Main entry point for script being executed from the command line. """

    # Initialize the Sample Class.
    parser = SampleGeneratorArgumentParser()
    args = parser.parse_args()

    configs = load_config()

    try:
        # Initialize the file generator.
        file_generator = sample.SampleFileGenerator(configs)
    except (IOError, KeyboardInterrupt, EOFError) as ex:
        print("Error on file creation. Exception: {}".format(ex))
        sys.exit(1)

    try:
        # Initialize the sample generator.
        record_generator = sample.SampleRecordGenerator(args, configs)

        # TQDM Progress Bar.
        pbar = tqdm(total=int(args.sample_size), unit="rec")
        pbar.set_description("Generating Records")

        # Loop on records to generate the rows.
        for index in range(int(args.sample_size)):
            # Loop on fields from sample config file.
            record_data = record_generator.generate_record(index)
            file_generator.write_sample_records(record_data)
            pbar.update(1)
        # Close progress bar.
        pbar.close()
        print("\nPlease check the generated sample file below:")
        print("\t{}".format(file_generator.filewrapper.name))
        # print(filewrapper)

    except (ValueError, KeyboardInterrupt) as ex:
        print("Error on generate sample file. Exception: {}".format(ex))
    finally:
        # Close the sample file.
        file_generator.close_sample_file()


def load_config():
    """
    Load the configs from file.
    """
    with open("sample/sample_config.json", 'r') as config_file:
        config = json.loads(config_file.read())
    return config


if __name__ == "__main__":
    # Setup logging based on the configuration in 'sample_config.json'.
    main()
