from .pandas import Pandas


class CSV(Pandas):
    def transform(self, data_frame, slicer, dimensions, references):
        result_df = super(CSV, self).transform(data_frame, slicer, dimensions, references)
        return result_df.to_csv()
