from gtfs_builder.gtfs_core.core import OpenGtfs


class Shapes(OpenGtfs):

    def __init__(self, geo_tools_core, input_file="shapes.txt", use_original_epsg=False):

        super(Shapes, self).__init__(geo_tools_core, geo_tools_core.path_data, input_file, use_original_epsg)

        self.__build_vehicule_trace()

        self._input_data = self._reproject_gdf(
            self._input_data
        )

        if self.is_df_empty(self._input_data):
            raise ValueError(f"'{input_file}' is empty")

    def __build_vehicule_trace(self):
        self._input_data = self.gdf_from_df_long_lat(
            self._input_data,
            "shape_pt_lon",
            "shape_pt_lat"
        )

        self._input_data = self.group_by_id_from_point_to_create_linestring(
            self._input_data,
            "shape_id",
            "shape_pt_sequence"
        )

