from .pandas import Pandas


class CSV(Pandas):
    def transform(self, data_frame, slicer, dimensions):
        result_df = super(CSV, self).transform(data_frame, slicer, dimensions)
        return result_df.to_csv()
