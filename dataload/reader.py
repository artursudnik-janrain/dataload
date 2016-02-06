import csv
import codecs
from dataload.utils import expand_objects

import logging
logger = logging.getLogger(__name__)


class BaseBatch(object):
    def __init__(self, records, batch_id=None):
        self.id = batch_id
        self.records = records


class CsvBatch(BaseBatch):
    def __init__(self, records, batch_id=None, start_line=1, end_line=101):
        super(CsvBatch, self).__init__(records, batch_id)
        self.start_line = start_line
        self.end_line = end_line


class BaseBatchReader(object):
    def __init__(self):
        self._transformations = {}

    def add_transformation(self, attribute, transformation_func):
        self._transformations[attribute] = transformation_func

    def transform(self, column, value):
        if not value:
            return None
        if column in self._transformations:
            new_value = self._transformations[column](value)
            logger.debug("Transform '{}': {} => {}".format(column, value, new_value))
            return new_value

        return value


class CsvBatchReader(BaseBatchReader):
    def __init__(self, csv_file, batch_size=100, start_at=1, delimiter=","):
        super(CsvBatchReader, self).__init__()
        self.delimiter=delimiter
        self.csv_file = csv_file
        self.batch_size = batch_size
        self.header = None
        self.start_at = start_at
        self.plural_processor = None

        if self.batch_size <=2:
            raise Exception("Batch size must be greater than 2.")

    def utf8_validate(self):
        logger.info("Validating UTF-8 encoding")
        f = codecs.open(self.csv_file, encoding='utf8', errors='strict')
        line_number = 1
        try:
            for i, line in enumerate(f):
                line_number += 1
        except UnicodeDecodeError as error:
            logger.error("Line {}: {}".format(line_number, str(error)))
            raise error

    def __iter__(self):
        self.utf8_validate()
        with open(self.csv_file, encoding="utf-8") as f:
            reader = csv.reader(f, delimiter=self.delimiter)
            batch = []
            batch_number = 0

            for i, row in enumerate(reader):
                line = i + 1
                start_line = line - len(batch)
                end_line = line - 1
                if (i == 0):
                    self.header = row
                    continue
                elif (line < (self.start_at + 1)):
                    continue
                elif line > 2 and ((line - 2) % self.batch_size == 0):
                    batch_number += 1
                    yield CsvBatch(batch, batch_number, start_line, end_line)
                    batch = []

                # process the row
                transformed = [self.transform(self.header[i], value)
                    for i, value in enumerate(row)]
                record = expand_objects(dict(zip(self.header, transformed)))
                batch.append(record)

            batch_number += 1
            yield CsvBatch(batch, batch_number, start_line, end_line + 1)

