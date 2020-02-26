# -*- coding: utf-8 -*-
"""
    作者:       Liu Yuzhang
    版本:       1.1
    日期:       2019/06/13
    项目名称：   HYUNDAI_Score_Calculation
    python环境： 3.5
"""

import numpy as np
import pandas as pd
from connect_to_hive import db_hv
from to_xml import df_to_dict, dict_to_xml, out_xml
from intervals import IntInterval
from sqlalchemy import create_engine
from datetime import datetime
import datetime as dt
import logging
import yaml
from LogUtils import setup_logging
import warnings
warnings.filterwarnings('ignore')


class Score:
    def __init__(self):
        config = open('defult_config.yaml')
        config = yaml.load(config)
        self.xml_out_file = config['xml_out_file']
        self.df_generate_path = config['df_generate_path']
        self.process_feature = ['total_miles', 'over_speed_mile', 'acceleration_count',
                                'deceleration_count', 'speed0_40', 'speed40_80', 'speed80_100',
                                'sharp_turn_count', 'avg_speed', 'speed_variance']
        self.features = ['total_miles', 'over_speed_mile', 'acceleration_count',
                         'deceleration_count', 'speed0_40', 'speed40_80', 'speed80_100',
                         'sharp_turn_count', 'avg_speed', 'speed_variance',
                         'time0to4_count', 'time7to9_count', 'time17to19_count',
                         'gt4h_count']
        self.columns = ['0%', '10%', '20%', '30%', '40%', '50%', '60%', '70%', '80%', '90%', '100%']
        self.score_columns = ['id', 'vid', 'odo_mile_score', 'safty_score', 'smoothly_score',
                              'vehicle_use_score', 'fatigue_driving_score', 'final_score']
        self.host = config['host']
        self.db = config['db']
        self.user = config['user']
        self.pswd = config['pswd']
        self.port = config['port']
        self.table = config['table']
        self.auth = 'PLAIN'
        self.existprocess = 'replace'
        self.features_score = ['total_miles_score', 'over_speed_mile_score', 'acceleration_count_score',
                               'deceleration_count_score', 'speed0_40_score', 'speed40_80_score', 'speed80_100_score',
                               'sharp_turn_count_score', 'avg_speed_score', 'speed_variance_score',
                               'time0to4_count_score', 'time7to9_count_score', 'time17to19_count_score',
                               'gt4h_count_score', 'odo_mile_score', 'safty_score', 'smoothly_score',
                               'vehicle_use_score', 'fatigue_driving_score']
        self.count_list = ['total_miles', 'over_speed_mile', 'acceleration_count',
                           'deceleration_count', 'speed0_40', 'speed40_80', 'speed80_100',
                           'sharp_turn_count', 'time0to4_count', 'time7to9_count', 'time17to19_count',
                           'gt4h_count']
        self.avg_list = ['avg_speed', 'speed_variance']
        self.odo_mile_score_features = self.features_score[:0]
        self.safty_score_features = self.features_score[1:7]
        self.smoothly_score_features_2 = self.features_score[8:10]
        self.smoothly_score_features_3 = self.features_score[9:10]
        self.vehicle_use_score_features = self.features_score[11:13]
        self.fatigue_driving_score_features = self.features_score[14]
        self.final_score_features = self.features_score[15:19]
        self.weight_factor = 'factor'
        self.map_list = [{0: 1.5, 1: 0.8},
                         {0: 1.5, 1: 0.8, 2: 0.6},
                         {0: 1.5, 1: 1.0, 2: 0.8, 3: 0.6},
                         {0: 1.5, 1: 0.9, 2: 0.8, 3: 0.7, 4: 0.6},
                         {0: 1.5, 1: 1.2, 2: 1.0, 3: 0.8, 4: 0.7, 5: 0.6},
                         {0: 1.5, 1: 1.3, 2: 1.0, 3: 0.9, 4: 0.8, 5: 0.7, 6: 0.6},
                         {0: 1.5, 1: 1.4, 2: 1.2, 3: 1.0, 4: 0.9, 5: 0.8, 6: 0.7, 7: 0.6},
                         {0: 1.5, 1: 1.4, 2: 1.2, 3: 1.1, 4: 1.0, 5: 0.9, 6: 0.8, 7: 0.7, 8: 0.6},
                         {0: 1.5, 1: 1.4, 2: 1.3, 3: 1.2, 4: 1.1, 5: 1.0, 6: 0.9, 7: 0.8, 8: 0.7, 9: 0.6}]
        self.vehicle_type = ['NU', 'NC', 'ADc', 'TLc', 'ND', 'JFc', 'DMc', 'LFc', 'OSc', 'NP', 'SQ',
                             'TMc', 'QLc']
        self.previous_year_month = dt.date(dt.date.today().year, dt.date.today().month-2, 1)
        self.last_year_month = dt.date(dt.date.today().year, dt.date.today().month-1, 1)

    def weight_cal(self, all_data):
        '''Calculate weight to a dataframe using rawdata and percentile method.

        :param all_data:
        :return:
        '''
        try:
            L = []
            matrix = []

            for feature in self.features:
                L.append(0)
                for i in list(range(10, 100, 10)):
                    L.append(np.percentile(all_data[feature], i))
                L.append(100000000)

            for j in range(0, len(L), len(self.columns)):
                matrix.append(L[j:j + len(self.columns)])
            matrix = np.array(matrix)

            weight_bound_df = pd.DataFrame(matrix, index=self.features, columns=self.columns)
            return weight_bound_df

        except Exception as err:
            logging.error('Error in weight calculation:', err)

    @staticmethod
    def weight_generater(weight_bound_df):
        '''Generate lists of weight vars

        :param weight_df:
        :return:
        '''
        try:
            createVar = globals()
            list_name = weight_bound_df.index.values.tolist()

            for i, element in enumerate(list_name):
                if weight_bound_df.ix[i, 1] == 0.0:
                    createVar[str(element) + '_weight_list'] = [int(x) for x in
                                                                weight_bound_df.ix[i, 0:].drop_duplicates().values.tolist()]
                    createVar[str(element) + '_weight_list'].insert(0, 0)
                elif weight_bound_df.ix[i, 1] != 0.0:
                    createVar[str(element) + '_weight_list'] = [int(x) for x in
                                                                weight_bound_df.ix[i, 0:].drop_duplicates().values.tolist()]

            return total_miles_weight_list, over_speed_mile_weight_list, acceleration_count_weight_list, deceleration_count_weight_list, \
                   speed0_40_weight_list,  speed40_80_weight_list, speed80_100_weight_list, \
                   sharp_turn_count_weight_list, avg_speed_weight_list, speed_variance_weight_list, \
                   time0to4_count_weight_list, time7to9_count_weight_list, time17to19_count_weight_list, gt4h_count_weight_list

        except Exception as err:
            logging.error('Error in weight generating：', err)

    @staticmethod
    def df_list_generate(df_generate):
        '''

        :param df_generate:
        :return:
        '''
        try:
            createVar = globals()
            list_name = df_generate.index.values.tolist()

            for i, element in enumerate(list_name):
                createVar[str(element) + '_df_list'] = [x for x in df_generate.ix[i, 0:].
                                                        values.tolist()]

            return total_miles_df_list, over_speed_mile_df_list, acceleration_count_df_list, deceleration_count_df_list, \
                   speed0_40_df_list, speed40_80_df_list, speed80_100_df_list, sharp_turn_count_df_list, avg_speed_df_list, \
                   speed_variance_df_list, time0to4_count_df_list, time7to9_count_df_list, time17to19_count_df_list, \
                   gt4h_count_df_list

        except Exception as err:
            logging.error('Error in df list generating：', err)

    @staticmethod
    def null_detect(all_data):
        sharp_turn_null_detect = np.isnan(all_data['sharp_turn_count']).all()
        return sharp_turn_null_detect

    def data_preprocessing(self, all_data):
        '''Data ETL.

        :param all_data:
        :return:
        '''

        try:
            for i in self.process_feature:
                all_data[i][all_data[i] < 0] = 0
                all_data[i].fillna(0, inplace=True)

            print('Data Preprocessing Success!', flush=True)
            return all_data

        except Exception as err:
            logging.error('Error in Data Preprocessing:', err)

    @staticmethod
    def list_to_interval(data_list):
        '''Method to turn lists into intervals.

        :param data_list:
        :return:
        '''
        try:
            list_interval = []
            for i in range(len(data_list)):
                if i != len(data_list) - 1:
                    data_range = IntInterval.closed_open(data_list[i], data_list[i + 1])
                list_interval.append(data_range)

            print("Success in list to interval", datetime.now(), flush=True)
            return list_interval

        except Exception as err:
            logging.error('Error in list to interval:', err)

    def sql_generator(self, type):
        '''

        :param table:
        :param type:
        :return:
        '''
        try:
            # org_hive_sql = "select vin, start_time, end_time, drive_duration, drive_mileage as total_miles, \
            #                 acc_count as acceleration_count, break_count as deceleration_count, sharp_turn_count, avg_speed, \
            #                 speed0_40, speed40_80, speed80_100, speedover120 as over_speed_mile, speed_variance from "+\
            #                 self.table+" where Vehicle_type == " + "'"+type+"'" + " and start_time >= '2019-05-01' and \
            #                 start_time <='2019-06-01' and end_time >= '2019-05-01' and end_time <='2019-06-01'"
            org_hive_sql = "select vin, start_time, end_time, drive_duration, drive_mileage as total_miles, \
                            acc_count as acceleration_count, break_count as deceleration_count, sharp_turn_count, avg_speed, \
                            speed0_40, speed40_80, speed80_100, speedover120 as over_speed_mile, speed_variance from "+\
                            self.table+" where Vehicle_type == " + "'"+type+"'" + " and start_time >= '"\
                            +str(self.previous_year_month)+"' and \
                            start_time <='"+str(self.last_year_month)+"' and end_time >= '"+str(self.previous_year_month)\
                            +"' and end_time <='"+str(self.last_year_month)+"'"
            print(org_hive_sql)
            print("Success in sql generating, vehicle type is " + type, datetime.now(), flush=True)
            return org_hive_sql

        except Exception as err:
            logging.error('Error in sql generating:', err)

    @staticmethod
    def time_convert(x):
        s = str(datetime.strptime(str(x), "%Y-%m-%d %H:%M:%S"))[11:13]
        return s

    @staticmethod
    def time0to4_count_convert(x):
        if x in ['00', '01', '02', '03']:
            s = 1
        else:
            s = 0
        return s

    @staticmethod
    def time7to9_count_convert(x):
        if x in ['07', '08']:
            s = 1
        else:
            s = 0
        return s

    @staticmethod
    def time17to19_count_convert(x):
        if x in ['17', '18']:
            s = 1
        else:
            s = 0
        return s

    @staticmethod
    def gt4h_count_convert(x):
        if x >= 14400:
            s = 1
        else:
            s = 0
        return s

    def data_conversion(self, all_data):
        '''

        :param all_data:
        :return:
        '''
        try:
            all_data['time'] = all_data['start_time'].apply(self.time_convert)
            all_data['time0to4_count'] = all_data['time'].apply(self.time0to4_count_convert)
            all_data['time7to9_count'] = all_data['time'].apply(self.time7to9_count_convert)
            all_data['time17to19_count'] = all_data['time'].apply(self.time17to19_count_convert)
            all_data['gt4h_count'] = all_data['drive_duration'].apply(self.gt4h_count_convert)
            return all_data

        except Exception as err:
            logging.error('Error in data conversion:', err)

    def data_integrate(self, org_data):
        '''

        :param org_data:
        :return:
        '''
        try:
            org_data_1 = org_data.groupby(['vin'])[self.count_list].sum()
            org_data_2 = org_data.groupby(['vin'])[self.avg_list].mean()
            org_data = pd.merge(org_data_1, org_data_2, left_index=True, right_index=True)
            all_data = org_data.reset_index('vin')
            print("Success in data integration!", datetime.now(), flush=True)
            return all_data

        except Exception as err:
            logging.error('Error in data conversion:', err)

    def data_to_sql(self, data_to_psgsql):
        '''Save data to Sql database.

        :param data_to_psgsql:
        :return:
        '''
        try:
            connect = create_engine(
                'postgresql+psycopg2://' + self.user + ':' + self.pswd + '@' + self.host + ':' + str(self.port) + '/' + self.db)
            pd.io.sql.to_sql(data_to_psgsql, self.table, connect, schema='public',  if_exists=self.existprocess)
            connect.dispose()
            print("Success in save data to PostgreSql!", datetime.now(), flush=True)

        except Exception as err:
            logging.error('Error in save data to PostgreSql:', err)

    def score_calculation(self, all_data, data_item, weight_list, map_list):
        '''Flash charge score calculator.

        :param all_data:
        :return:
        '''
        try:
            all_data[data_item + '_score_dict'] = None
            all_data[data_item + '_score'] = None
            data_list = weight_list
            length = len(data_list)

            if data_list[1] == 0.0:
                list_interval = self.list_to_interval(data_list[1: length])
                for single_interval in list_interval:
                    for i in range(len(all_data[data_item])):
                        if all_data.loc[i, data_item] == 0:
                            all_data.loc[i, data_item + '_score_dict'] = 0
                        elif all_data.loc[i, data_item] in single_interval:
                            all_data.loc[i, data_item + '_score_dict'] = list_interval.index(single_interval) + 1
            else:
                list_interval = self.list_to_interval(data_list)
                for single_interval in list_interval:
                    list_interval = self.list_to_interval(data_list)
                    for i in range(len(all_data[data_item])):
                        if all_data.loc[i, data_item] in single_interval:
                            all_data.loc[i, data_item + '_score_dict'] = list_interval.index(single_interval)

            all_data[data_item + '_score'] = all_data[data_item + '_score_dict'].map(map_list[length - 3])

            return all_data

        except Exception as err:
            logging.error('Error in score calculation:', err)

    def final_score_cal(self, vehicle_type, all_data, weight_value):
        '''Final score calculator.

        :param all_data:
        :return:
        '''
        try:
            all_data['odo_mile_score'] = None
            all_data['safty_score'] = None
            all_data['smoothly_score'] = None
            all_data['vehicle_use_score'] = None
            all_data['fatigue_driving_score'] = None
            all_data['final_score'] = None

            for i in range(len(all_data['flash_charge_count'])):
                all_data.loc[i, 'odo_mile_score'] = 0
                all_data.loc[i, 'safty_score'] = 0
                all_data.loc[i, 'smoothly_score'] = 0
                all_data.loc[i, 'vehicle_use_score'] = 0
                all_data.loc[i, 'fatigue_driving_score'] = 0
                all_data.loc[i, 'final_score'] = 0

                all_data.loc[i, 'odo_mile_score'] = all_data.loc[i, 'total_miles_score'] * 65 * \
                                                    weight_value.loc['total_miles_score', self.weight_factor]
                for features in self.safty_score_features:
                    all_data.loc[i, 'safty_score'] = all_data.loc[i, 'safty_score']+ \
                                                     all_data.loc[i, features] * 65 * weight_value.loc[features, self.weight_factor]
                if vehicle_type == 'SQ':
                    for features in self.smoothly_score_features_2:
                        all_data.loc[i, 'smoothly_score'] = all_data.loc[i, 'smoothly_score'] + \
                                                         all_data.loc[i, features] * 65 * weight_value.loc[features, self.weight_factor]
                else:
                    for features in self.smoothly_score_features_3:
                        all_data.loc[i, 'smoothly_score'] = all_data.loc[i, 'smoothly_score'] + \
                                                            all_data.loc[i, features] * 65 * weight_value.loc[
                                                                features, self.weight_factor]
                for features in self.vehicle_use_score_features:
                    all_data.loc[i, 'vehicle_use_score'] = all_data.loc[i, 'vehicle_use_score'] + \
                                                                all_data.loc[i, features] * 65 * weight_value.loc[features, self.weight_factor]
                all_data.loc[i, 'fatigue_driving_score'] = all_data.loc[i, 'fatigue_driving_score'] * 65 * \
                                                             weight_value.loc['fatigue_driving_score', self.weight_factor]
                for features in self.final_score_features:
                    all_data.loc[i, 'final_score'] = all_data.loc[i, 'final_score'] +\
                                                     all_data.loc[i, features] * weight_value.loc[features, self.weight_factor]

            return all_data

        except Exception as err:
            logging.error('Error in final score calculation:', err)

    def generate_grade_config_pd(self, vehicle_type, sharp_turn_isnull, weight_list, list_generate):
        '''

        :param vehicle_type:
        :param weight_list:
        :param list_generate:
        :return:
        '''
        try:
            df = pd.DataFrame(columns=('gid', 'ubi_score_gid', 'vehicle_type', 'time_limit', 'base_factor',
                                       'factor', 'fix_weight', 'limit_min', 'limit_max', 'score'))
            cols = ['gid', 'ubi_score_gid', 'vehicle_type', 'time_limit', 'base_factor',
                    'factor', 'fix_weight', 'limit_min', 'limit_max', 'score']
            df = df.ix[:, cols]

            for i in range(len(weight_list)-1):
                gid = int(list_generate[0])
                ubi_score_gid = int(list_generate[1])
                vehicle_type = vehicle_type
                time_limit = '30'
                base_factor = '65'
                if sharp_turn_isnull is True:
                    factor = list_generate[4]
                    fix_weight = list_generate[5]
                else:
                    factor = list_generate[2]
                    fix_weight = list_generate[3]
                limit_min = weight_list[i]
                limit_max = weight_list[i+1]
                score = (self.map_list[len(weight_list)-3])[i]
                df = df.append(pd.DataFrame({'gid': [gid], 'ubi_score_gid': [ubi_score_gid],
                                             'vehicle_type': [vehicle_type], 'time_limit': [time_limit],
                                             'base_factor': [base_factor], 'factor': [factor],
                                             'fix_weight': [fix_weight], 'limit_min': [limit_min],
                                             'limit_max': [limit_max], 'score': [score]}), ignore_index=True)

            print(vehicle_type + " grade config df generate success!", datetime.now(), flush=True)
            return df

        except Exception as err:
            print('Error in generate grade config pd: ', err, ', vehicle tpye is: '+ vehicle_type, flush=True)
            logging.error('Error in generate grade config pd:', err)

    def run(self):
        setup_logging(default_path="logging.yaml")
        df_all = pd.DataFrame()

        for type in self.vehicle_type:
            try:
                print('******************Begin to run type', type, '...******************', flush=True)
                pg_conn = db_hv(host=self.host, db=self.db, user=self.user, pwd=self.pswd, port=self.port,
                                auth=self.auth)
                hive_sql = self.sql_generator(type)
                rs = pg_conn.select(hive_sql)
                org_data = pd.DataFrame(list(rs.get('data')), columns=rs.get('head'), dtype=np.float)
                sharp_turn_isnull = self.null_detect(org_data)

                all_data = self.data_preprocessing(org_data)
                all_data = self.data_conversion(all_data)
                all_data = self.data_integrate(all_data)
                weight_bound_df = self.weight_cal(all_data)

                df_generate = pd.read_table(self.df_generate_path, index_col=[0], encoding='utf-8', sep=',',
                                            low_memory=False)

                weight_list = self.weight_generater(weight_bound_df)
                df_list = self.df_list_generate(df_generate)
                para_list = list(zip(weight_list, df_list))

                df_type = pd.DataFrame()

                try:
                    if sharp_turn_isnull is True:
                        for i in range(len(para_list)):
                            if i == 7:
                                pass
                            else:
                                df_item = self.generate_grade_config_pd(type, sharp_turn_isnull, para_list[i][0],
                                                                        para_list[i][1])
                                if df_type is None:
                                    df_type = df_item
                                else:
                                    df_type = df_type.append(df_item, ignore_index=True)
                    else:
                        for i in range(len(para_list)):
                            df_item = self.generate_grade_config_pd(type, sharp_turn_isnull, para_list[i][0],
                                                                    para_list[i][1])
                            if df_type is None:
                                df_type = df_item
                            else:
                                df_type = df_type.append(df_item, ignore_index=True)

                    print('Success in generate grade config df, type is:', type, flush=True)
                    logging.info('Success in generate grade config df, type is: '+type)

                except Exception as err:
                    print('Run error：', err, 'Type is : ', type, flush=True)
                    logging.error('Grade config df generating error：', err, ', type is : ', type)

                if df_all is None:
                    df_all = df_type
                else:
                    df_all = df_all.append(df_type, ignore_index=True)

                logging.info(type + ' ubi config dataframe generated!')
                print('******************', type, 'runing completed!...******************', flush=True)

            except Exception as err:
                logging.error('Error in ubi config dataframe', err, ', vehicle type is' + type)
                print('Error in ubi config dataframe', err, ', vehicle type is' + type, flush=True)

        cols = ['gid', 'ubi_score_gid', 'vehicle_type', 'time_limit', 'base_factor',
                'factor', 'fix_weight', 'limit_min', 'limit_max', 'score']
        df_all = df_all.ix[:, cols]

        dict = df_to_dict(df_all)
        root = dict_to_xml(dict, "root", "item")
        out_xml(root, self.xml_out_file)
        print('Xml generated!', flush=True)
        print('Over!', flush=True)


if __name__ == '__main__':
    p = Score()
    a = datetime.now()
    print('begin time:', datetime.now())
    p.run()
    b = datetime.now()
    print('stop time:', datetime.now())
    print('score calculating total run time:', b-a)


