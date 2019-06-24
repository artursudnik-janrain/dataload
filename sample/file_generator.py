"""
Sample file  to be used on data-load.
"""
import datetime
import csv


class SampleFileGenerator():
    """
    Class used to generate a sample file to data load.
    """
    def __init__(self, configs):
        self.configs = configs['sample']
        self.filewrapper = self.create_sample_file()
        self.fields = self.configs['fields']

        # Generate the sample file with headers.
        self.__csv_headers = self.__generate_sample_header()
        self.__csv_writer = self.__create_csv_writer()

    def close_sample_file(self):
        """
        Closes the samples file.
        """
        self.filewrapper.close()

    def create_sample_file(self):
        """
        Creates the sample file using timestamp on the name.
        """
        sample_file = self.configs['file']
        file_pattern = sample_file['filename_pattern']
        pattern_replace = sample_file['filename_pattern_replace']
        date_format = datetime.datetime.now().strftime(pattern_replace)
        filename = file_pattern.format(date_format)
        filewrapper = open(filename, "w")
        return filewrapper

    # Create the CSV writer with headers.
    def __create_csv_writer(self):
        delimiter = self.configs['separator']
        csv_header = self.__csv_headers
        csv_writer = csv.DictWriter(self.filewrapper, delimiter=delimiter,
                                    fieldnames=csv_header)
        csv_writer.writeheader()
        return csv_writer

    # Add the header on the sample file, using fields on config file.
    def __generate_sample_header(self):
        csv_headers = []
        for i in range(0, len(self.fields)):
            csv_headers.append(self.fields[i]['name'])
        return csv_headers

    def write_sample_records(self, sample_record):
        """
        Write the receive sample record row into the csv file.

        Args:
            sample_record: The generated sample record row.
        """
        self.__csv_writer.writerow(sample_record)
