# Hyundai_Score

    class Score(builtins.object)
     |  Methods defined here:
     |
     |  __init__(self)
     |      Initialize self.  See help(type(self)) for accurate signature.
     |
     |  dataProcessing(self, all_data)
     |      Data ETL.
     |
     |      :param all_data:
     |      :return:
     |
     |  dataToSql(self, data_to_psgsql)
     |      Save data to Sql database.
     |
     |      :param data_to_psgsql:
     |      :return:
     |
     |  data_conversion(self, org_data)
     |
     |  data_integrate(self, org_data)
     |
     |  df_list_generate(self, df_generate)
     |      :param df_generate:
     |      :return:
     |
     |  final_score_cal(self, vehicle_type, all_data, weight_value)
     |      :param vehicle_type:
     |      :param all_data:
     |      :param weight_value:
     |      :return:
     |
     |  generate_grade_config_pd(self, vehicle_type, weight_list, list_generate)
     |      :param vehicle_type:
     |      :param weight_list:
     |      :param list_generate:
     |      :return:
     |
     |  listToInterval(self, data_list)
     |      Method to turn lists into intervals.
     |
     |      :param data_list:
     |      :return:
     |
     |  run(self)
     |
     |  score_calculation(self, all_data, data_item, weight_list, map_list)
     |      :param all_data:
     |      :param data_item:
     |      :param weight_list:
     |      :param map_list:
     |      :return:
     |
     |  sql_generater(self, table, type)
     |
     |  weightCal(self, all_data)
     |      Calculate weight to a dataframe using rawdata and percentile method.
     |
     |      :param all_data:
     |      :return:
     |
     |  weightGenerater(self, weight_bound_df)
     |      Generate lists of weight vars
     |
     |      :param weight_df:
     |      :return:
     |
     |  ----------------------------------------------------------------------
     |  Data descriptors defined here:
     |
     |  __dict__
     |      dictionary for instance variables (if defined)
     |
     |  __weakref__
     |      list of weak references to the object (if defined)
