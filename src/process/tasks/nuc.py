import luigi

# Create is_nuc column on IRImage and populate
from core import ForcibleTask


class NUCDetectorTask(ForcibleTask):
        def output(self):
            pass # charts and csv file of nucs

        def cleanup(self):
            pass

        def run(self):
            pass


class LoadNUCSToDatabaseTask(ForcibleTask):
    def requires(self):
        pass

    def output(self):
        pass

    def cleanup(self):
        pass

    def run(self):
        pass