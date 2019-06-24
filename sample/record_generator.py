"""
Sample record generator to be used on data-load.
"""
import sample.randomize


class SampleRecordGenerator():
    """
    Class used to generate a sample record to data load.
    """
    def __init__(self, args, configs):
        self.configs = configs['sample']
        self.fields = self.configs['fields']
        self.__max_length = self.configs['max_length']
        self.__min_length = self.configs['min_length']
        self.__only_required = args.only_required

    # Get the max length of a field. If not set, use the default.
    def __get_max_lenght(self, field):
        if 'max_length' in field:
            return field['max_length']
        return self.__max_length

    # Get the max length of a field. If not set, use the default.
    def __get_min_length(self, field):
        if 'min_length' in field:
            return field['min_length']
        return self.__min_length

    # Generate the random values using the defined fields.
    def __generate_random_value(self, field, index):
        # Add the necessary field attributes to generate
        # the random field value.
        field.update({
            'max_length': self.__get_max_lenght(field),
            'min_length': self.__get_min_length(field)
        })
        only_required = self.__only_required
        func_name = "generate_random_{}".format(field['type'])
        random_gen_func = getattr(sample.randomize, func_name,
                                  'Method {} not found!'.format(func_name))
        # Check if method exists and is callable before call it.
        if callable(random_gen_func):
            return random_gen_func(field, only_required=only_required,
                                   index=index)
        return random_gen_func

    def generate_field_data(self, field, index):
        """
        Generate the field record based on config file.

        Args:
            field: The field object.
            index: The index from records number receive on arguments.

        Returns:
            The field value using the format or a random value.
        """
        field_value = self.__generate_random_value(field, index=index)
        return field_value

    def generate_record(self, index):
        """
        Generate the sample record row using the field configuratio.

        Args:
            index: The index from records number receive on arguments.

        Returns:
            The record complete row, populated with generated data.
        """
        record_row = {}
        for field in self.fields:
            field_name = field['name']
            record_row[field_name] = self.generate_field_data(field, index)
        return record_row
