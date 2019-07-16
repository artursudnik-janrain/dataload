from janrain.capture.cli import ApiArgumentParser
from janrain.capture import config
from argparse import ArgumentParser

import logging
logger = logging.getLogger(__name__)

class SampleGeneratorArgumentParser(ArgumentParser):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.add_argument('-n', '--sample-size', type=int, default=1,
                          help="number of records on sample file (default: 1)")
        self.add_argument('-r', '--only-required', action="store_true",
                          help="only required attributes will be generated")

    def parse_args(self, args=None, namespace=None):
        args = super().parse_args(args, namespace)
        self._parsed_args = args
        return self._parsed_args

class DataLoadArgumentParser(ApiArgumentParser):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.add_argument('-t', '--type-name', default="user",
                          help="entity type name (default: user)")
        self.add_argument('-b', '--batch-size', type=int, default=100,
                          help="number of records per batch (default: 100)")
        self.add_argument('-a', '--start-at', type=int, default=1,
                          help="record number to start at (default: 1)")
        self.add_argument('data_file', metavar="DATA_FILE",
                          help="full path to the data file being loaded")
        self.add_argument('-w', '--workers', type=int, default=10,
                          help="number of worker threads (default: 10)")
        self.add_argument('-o', '--timeout', type=int, default=10,
                          help="timeout in seconds for API calls (default: 10)")
        self.add_argument('-r', '--rate-limit', type=float, default=4.0,
                          help="max API calls per second (default: 4)")
        self.add_argument('-x', '--dry-run', action="store_true",
                          help="process data without making any API calls")

        dm_group = self.add_argument_group(title='Delta Migration Arguments')
        dm_group.add_argument('-m', '--delta-migration', action="store_true",
                          help="import the new entity and update any existing entity")
        dm_group.add_argument('-p', '--primary-key', default="email",
                          help="use an existing attribute in the target\
                          Entity Type to identify a duplicate record that will\
                          be updated. The attribute must be unique on schema \
                          (default: email)")

    def parse_args(self, args=None, namespace=None):
        args = super().parse_args(args, namespace)
        # Parse the YAML configuration here so that init_api() does not need to
        # read the config each time it's called which would not be thread safe.
        if args.config_key:
            credentials = config.get_settings_at_path(args.config_key)
        elif args.default_client:
            credentials = config.default_client()

        if args.config_key or args.default_client:
            args.client_id = credentials['client_id']
            args.client_secret = credentials['client_secret']
            args.apid_uri = credentials['apid_uri']

        logger.debug(args.apid_uri)
        self._parsed_args = args
        return self._parsed_args

class RollbackArgumentParser(ApiArgumentParser):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.add_argument('-t', '--type-name', default="user",
                            help="entity type name (default: user)")
        self.add_argument('data_file', metavar="DATA_FILE",
                            help="full path to the data file being loaded")
        self.add_argument('-w', '--workers', type=int, default=10,
                            help="number of worker threads (default: 10)")
        self.add_argument('-o', '--timeout', type=int, default=10,
                            help="timeout in seconds for API calls (default: 10)")
        self.add_argument('-r', '--rate-limit', type=float, default=4.0,
                            help="max API calls per second (default: 4)")
        self.add_argument('-x', '--dry-run', action="store_true",
                            help="process data without making any API calls")

    def parse_args(self, args=None, namespace=None):
        args = super().parse_args(args, namespace)
        # Parse the YAML configuration here so that init_api() does not need to
        # read the config each time it's called which would not be thread safe.
        if args.config_key:
            credentials = config.get_settings_at_path(args.config_key)
        elif args.default_client:
            credentials = config.default_client()

        if args.config_key or args.default_client:
            args.client_id = credentials['client_id']
            args.client_secret = credentials['client_secret']
            args.apid_uri = credentials['apid_uri']

        logger.debug(args.apid_uri)
        self._parsed_args = args
        return self._parsed_args
