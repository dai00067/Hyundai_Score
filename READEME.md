现代UBI模型权重计算项目

"""
    作者:       Liu Yuzhang, Fan Yuliang
    版本:       1.1
    日期:       2019/05/24
    项目名称：   HYUNDAI_Score_Calculation
    python环境： 3.5
"""

***************************************************
模块功能描述：
hyundai_score:主程序，进行边界计算，生成ubi_grade xml文档。同时，可对各维度得分、总分进行计算。
connect_to_postgre:连接postgre数据。
to_xml:生成xml。

*待增加基于机器学习模型的权重计算模块。

****************************************************
Help on module hyundai_score:

NAME
    hyundai_score
    
SERVER DATABASE
    HOST 127.0.0.1
    PORT 5498
    USER postgres
    PASSWORD postgres

DESCRIPTION
    作者:       Liu Yuzhang
    版本:       1.1
    日期:       2019/05/23
    项目名称：   HYUNDAI_Score_Calculation
    python环境： 3.5

CLASSES
    builtins.object
        Score

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


****************************************************
Help on module connect_to_postgre:

NAME
    connect_to_postgre

DESCRIPTION
    None

CLASSES
    builtins.object
        db_pg

    class db_pg(builtins.object)
     |  Methods defined here:
     |
     |  __del__(self)
     |
     |  __init__(self, host, db, user, pwd, port)
     |      Initialize self.  See help(type(self)) for accurate signature.
     |
     |  close(self)
     |
     |  common(self, sqlCode)
     |
     |  select(self, sqlCode)
     |
     |  ----------------------------------------------------------------------
     |  Data descriptors defined here:
     |
     |  __dict__
     |      dictionary for instance variables (if defined)
     |
     |  __weakref__
     |      list of weak references to the object (if defined)


****************************************************
Help on module to_xml:

NAME
    to_xml

FUNCTIONS
    creat_dict(root)
        xml生成为dict：，
        将tree中个节点添加到list中，将list转换为字典dict_init
        叠加生成多层字典dict_new

    df_to_dict(df)

    dict_to_xml(input_dict, root_tag, node_tag)
        定义根节点root_tag，定义第二层节点node_tag
        第三层中将字典中键值对对应参数名和值
           return: xml的tree结构

    fn(_dict, depth)
        遍历多重dict

        :param _dict:
        :param depth:
        :return:

    out_xml(root, out_file)
        格式化root转换为xml文件

    read_xml(in_path)
        读取并解析xml文件
        in_path: xml路径
        return: tree

DATA
    start = 0.0
    
Hive
create table vcrm_ubi_t2 (
vin STRING,
start_time STRING,
end_time STRING,
drive_duration INT,
drive_mileage FLOAT,
acc_count INT,
break_count INT,
avg_speed FLOAT,
speed0_40 FLOAT,
speed40_80 FLOAT,
speed80_100 FLOAT,
speedover120 FLOAT,
speed_variance FLOAT,
sharp_turn_count INT,
Vehicle_type STRING,
prj_vehl_cd STRING,
start_gps STRING,
end_gps STRING,
p_date STRING,
q_speed STRING,
version STRING
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;

use hyundai;

DROP TABLE IF EXISTS vcrm_ubi_t2;

select * from vcrm_ubi_t2 limit 1;

load data local inpath '/home/hedaofeng/upfile/hyundiFile.csv' into table vcrm_ubi_t2;

