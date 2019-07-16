import csv
import codecs
from utils.utils import expand_objects

import logging
logger = logging.getLogger(__name__)


class BaseBatch(object):
    def __init__(self, records, original_records, batch_id=None):
        self.id = batch_id
        self.records = records
        self.original_records = original_records


class CsvBatch(BaseBatch):
    def __init__(self, records, original_records, batch_id=None, start_line=1,
                 end_line=101):
        super(CsvBatch, self).__init__(records, original_records, batch_id)
        self.start_line = start_line
        self.end_line = end_line


class BaseUtf8Reader(object):
    def __init__(self):
        self._transformations = {}

    def add_transformation(self, attribute, transformation_func):
        self._transformations[attribute] = transformation_func

    def transform(self, column, value):
        # If there is a transformation, let it handle the value.
        # Otherwise return the own value or None.
        # Note: All values received from CSV are formatted as a string.
        if column in self._transformations:
            try:
                new_value = self._transformations[column](value)
            except ValueError as e:
                raise ValueError("{} on attribute {}".format(str(e), column)
                                 ) from None
            logger.debug("Transform '{}': {} => {}".format(column, value,
                                                           new_value))
            return new_value
        elif not value:
            return None
        else:
            return value

    def get_plurals(self):
        """
        Returns a list of Plural attributes defined in current schema,
        identified by the existance of a transform_plural associated
        with it.
        """
        plurals_to_update = []
        for field_name in self._transformations:
            def_name = self._transformations[field_name].__name__
            if "transform_plural" in def_name:
                plurals_to_update.append(field_name)
        return plurals_to_update

    def utf8_validate(self, csv_file):
        logger.info("Validating UTF-8 encoding and checking for Byte Order\
                    Mark")
        # check for BOM first - usually appears if file was exported from
        # MS Excel
        with open(csv_file, "rb") as f:
            if (f.read(3) == b'\xef\xbb\xbf'):
                logger.info("Byte Order Mark detected.")
                self.file_has_bom = True

        f = codecs.open(csv_file, encoding='utf8', errors='strict')
        line_number = 1
        try:
            for _, _ in enumerate(f):
                line_number += 1
        except UnicodeDecodeError as error:
            logger.error("Line {}: {}".format(line_number, str(error)))
            raise error


class CsvBatchReader(BaseUtf8Reader):
    def __init__(self, csv_file, batch_size=100, start_at=1, delimiter=","):
        super(CsvBatchReader, self).__init__()
        self.delimiter = delimiter
        self.csv_file = csv_file
        self.batch_size = batch_size
        self.header = None
        self.start_at = start_at
        self.plural_processor = None
        self.file_has_bom = False
        self.file_descriptor = open(csv_file, encoding="utf-8")

        if self.batch_size <= 2:
            raise Exception("Batch size must be greater than 2.")

    def get_header(self):
        self.header = self.file_descriptor.readline()
        self.header = self.header.strip()
        return self.header.split(self.delimiter)

    def __iter__(self):
        self.utf8_validate(self.csv_file)
        f = self.file_descriptor
        # Reset the file to the initial position
        f.seek(0)
        # if the file has a BOM, start reading after those bytes
        if self.file_has_bom:
            f.seek(3)
        reader = csv.reader(f, delimiter=self.delimiter)
        batch = []
        batch_original = []
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
                yield CsvBatch(batch, batch_original, batch_number,
                               start_line, end_line)
                batch = []
                batch_original = []

            # process the row
            try:
                transformed = [self.transform(self.header[i], value)
                               for i, value in enumerate(row)]
            except ValueError as e:
                # Log a more clear message on where the error is located in
                # the CSV
                logger.error("{} error on CSV line {}: {}".format(type(e),
                                                                  line, e))
                raise e
            record = expand_objects(dict(zip(self.header, transformed)))
            batch.append(record)
            batch_original.append(row)

        batch_number += 1
        yield CsvBatch(batch, batch_original, batch_number, start_line,
                       end_line + 1)


class CsvReader(BaseUtf8Reader):
    def __init__(self, csv_file, delimiter=","):
        super(CsvReader, self).__init__()
        self.delimiter = delimiter
        self.csv_file = csv_file
        self.file_has_bom = False

    def __iter__(self):
        self.utf8_validate(self.csv_file)
        with open(self.csv_file, encoding="utf-8") as f:
            # if the file has a BOM, start reading after those bytes
            if self.file_has_bom:
                f.seek(3)
            reader = csv.reader(f, delimiter=self.delimiter)

            for i, row in enumerate(reader):
                if (i == 0):
                    continue
                yield row


class CsvWriter():
    def __init__(self, csv_filename, mode):
        self.csv_filename = csv_filename
        self.file_stream = open(csv_filename, mode)
        self.csv_writer = csv.writer(self.file_stream, delimiter=',',
                                     quotechar='"')

    def get_filename(self):
        return self.file_stream.name

    def write_row(self, row):
        self.csv_writer.writerow(row)

    def close_file(self):
        self.file_stream.close()
