from .data_generator import DataGenerator

class CustomDataGenerator(DataGenerator):
    def __init__(self, measure_templates: list, dimension_templates: list):
        self.measure_templates = measure_templates
        self.dimension_templates = dimension_templates
