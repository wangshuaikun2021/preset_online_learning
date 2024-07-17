# -*- coding:utf-8 -*-
# @FileName  :get_preset_class.py
# @Time      :2023/10/16 14:41
# @Author    :wsk
import numpy as np
import pandas as pd
from icecream import ic
from scipy.special import softmax
from tqdm import tqdm
# from logger_config import logger


class GeneratePreset:
    def __init__(self, df, LIMIT=200, LIMIT2=100):
        self.preset_excel = None
        self.preset_df_data = None
        self.LIMIT2 = LIMIT2
        self.df = df
        self.calfeature = ['WRB_1', 'IRB_1', 'IRS_1',
                           'WRB_2', 'IRB_2', 'IRS_2',
                           'WRB_3', 'IRB_3', 'IRS_3',
                           'WRB_4', 'IRB_4', 'IRS_4',
                           'WRB_5', 'IRB_5', 'IRS_5',
                           'IU_50'
                           ]  # 统计调控机构

        self.LIMIT = LIMIT

    def remove_fliers(self, temp, feature):
        """
        去除箱线图的异常点，即去除1.5倍四分位距的数据
        :param temp:用到的数据，dataframe格式
        :param feature:需要删除的特征
        :return:
        """
        line75 = temp[feature].describe()['75%']
        line25 = temp[feature].describe()['25%']
        miny = line25 - 1.5 * (line75 - line25)
        maxy = line75 + 1.5 * (line75 - line25)
        return temp[(temp[feature] >= miny) & (temp[feature] <= maxy)]

    def cal_preset_mean(self, data, row):

        data = self.remove_fliers(data, 'IU_50').reset_index(drop=True)

        count = len(data)
        if count < 5:
            return None
        row[1] = count
        data = data[self.calfeature[:-1]]
        temp = list(np.mean(data, axis=0))
        row.extend(temp)
        return np.array(row)

    def cal_preset_softmax(self, data, row):
        count = len(data)
        if count < 5:
            return None
        data = self.remove_fliers(data, 'IU_50').reset_index(drop=True)

        count = len(data)
        if count < 5:
            return None

        data = data[self.calfeature]
        # softmax
        temp = self.w_softmax(data)

        row[2] = count
        row.extend(temp)
        return np.array(row)

    def w_softmax(self, df):
        df = np.array(df)
        iu = df[:, -1]
        weights = softmax(-iu)
        weights = weights.reshape(1, -1)
        pre_df = df[:, :-1]
        return (weights @ pre_df)[0]

    def cal_filter(self, df_start, rbcols, flag='IRS', isCoff=False):
        """
        去除大于0.5的行
        :param df_start:
        :param rbcols: 弯辊的变量名，列表
        :return:
        """
        df_isr = df_start[rbcols]
        if isCoff:
            if flag == 'IRS':
                df_start = df_start[(abs(df_isr) <= 0.51).all(axis=1)]
            else:
                df_start = df_start[(abs(df_isr) <= 1.01).all(axis=1)]
        else:
            if flag == 'IRS':
                df_start = df_start[(abs(df_isr) <= 143).all(axis=1)]
            elif flag == 'WRB':
                df_start = df_start[((df_isr < 1000) & (df_isr > -700)).all(axis=1)]
            else:
                df_start = df_start[((df_isr < 2600) & (df_isr > -1800)).all(axis=1)]
        return df_start.reset_index(drop=True)

    def data_model(self):
        isr_cols = [f'IRS_{i}' for i in range(1, 6)]
        wrb_cols = [f'WRB_{i}' for i in range(1, 6)]
        irb_cols = [f'IRB_{i}' for i in range(1, 6)]
        self.df = self.cal_filter(self.df, isr_cols, flag='IRS', isCoff=True)
        self.df = self.cal_filter(self.df, wrb_cols, flag='WRB', isCoff=True)
        self.df = self.cal_filter(self.df, irb_cols, flag='IRB', isCoff=True)

        policy = set(self.df['aDirNoAi'].values)

        pre_df = []

        for p in tqdm(policy):
            df_p = self.df[self.df['aDirNoAi'] == p]
            iu_3_left = df_p[df_p["IU_50"] <= 3]
            iu_3_right = df_p[df_p["IU_50"] > 3]
            count_l, count_r = iu_3_left.shape[0], iu_3_right.shape[0]
            row = [p, count_l, count_l + count_r]

            if count_l > 2000:
                # 取iu_3_left的后2000个样本
                iu_3_left = iu_3_left.iloc[-2000:, :]

            if count_l > self.LIMIT:
                # 计算iu_3_left均值
                temp = self.cal_preset_mean(iu_3_left, row)
                if temp is not None:
                    pre_df.append(temp)
            elif count_l > self.LIMIT2:
                # 取 mean 和 softmax加权平均
                row1 = list(row)
                row2 = list(row)
                temp1 = self.cal_preset_softmax(df_p, row1)
                temp2 = self.cal_preset_mean(iu_3_left, row2)
                temp = [p, row2[1], row1[2]]

                w = 0.5
                if temp1 is not None and temp2 is not None:
                    temp.extend(w * temp1[3:] + (1 - w) * temp2[3:])
                    pre_df.append(np.array(temp))
            else:
                # 使用softmax加权平均
                temp = self.cal_preset_softmax(df_p, row)
                if temp is not None:
                    pre_df.append(temp)

        pre_df = np.array(pre_df)
        self.preset_df_data = pd.DataFrame(pre_df,
                                           columns=['aDirNoAi', 'count_l', 'count_all'] + self.calfeature[
                                                                                          :-1]).sort_values(
            by='aDirNoAi')

    # def condat_ans(self):
    #     df1 = pd.read_excel(r'STDADIRPASS_AI.xlsx')
    #     col2 = df1.columns.tolist()
    #     print(col2)
    #     df1 = df1.values
    #     # df = df[col].values
    #     _df = self.preset_df_data.reset_index(drop=True)
    #     _df.iloc[:, 0] = _df.iloc[:, 0].astype(np.int64)
    #     _df = _df.values
    #
    #     def trans(arr):
    #         for i in range(1, len(arr)):
    #             if i % 3 == 1:
    #                 if arr[i] > 0:
    #                     arr[i] = arr[i] * 1000
    #                 else:
    #                     arr[i] = arr[i] * 700
    #             elif i % 3 == 2:
    #                 if arr[i] > 0:
    #                     arr[i] = arr[i] * 2600
    #                 else:
    #                     arr[i] = arr[i] * 1800
    #             elif i % 3 == 0:
    #                 arr[i] = arr[i] * 285
    #         return np.around(arr, decimals=3)
    #
    #     ids = _df[:, 0]
    #     for i in range(df1.shape[0]):
    #         if df1[i, -11] == 1:    # 人工标记
    #             continue
    #         id = df1[i, 0]
    #         if id in ids:
    #             preset = _df[ids == id][0, :]
    #             trans(preset[2:])
    #             df1[i, 2:17] = preset[3:]
    #             df1[i, -13:-11] = preset[1:3]
    #         else:
    #             if df1[i, 2] != 9999:
    #                 df1[i, 2:17] = np.array([9999] * 15)
    #     # df1[:, -13: -15].fillna(0, inplace=True)
    #     self.preset_excel = pd.DataFrame(df1, columns=col2)
    #     self.preset_excel.iloc[:, -13:-11].fillna(0, inplace=True)
    def condat_ans(self):
        df1 = pd.read_excel(r'STDADIRPASS_AI.xlsx')
        col2 = df1.columns.tolist()
        # print(col2)
        df1_values = df1.values

        _df = self.preset_df_data.reset_index(drop=True)
        _df.iloc[:, 0] = _df.iloc[:, 0].astype(np.int64)
        _df_values = _df.values

        def trans(arr):
            arr[1::3] = np.where(arr[1::3] > 0, arr[1::3] * 1000, arr[1::3] * 700)
            arr[2::3] = np.where(arr[2::3] > 0, arr[2::3] * 2600, arr[2::3] * 1800)
            arr[3::3] = arr[3::3] * 285
            return np.around(arr, decimals=3)

        ids = _df_values[:, 0]
        for i in range(df1_values.shape[0]):
            if df1_values[i, -11] == 1:  # 人工标记
                continue
            id = df1_values[i, 0]
            if id in ids:
                preset = _df_values[ids == id][0, :]
                preset[2:] = trans(preset[2:])
                df1_values[i, 2:17] = preset[3:18]
                df1_values[i, -13:-11] = preset[1:3]
            else:
                if df1_values[i, 2] != 9999:
                    df1_values[i, 2:17] = np.full(15, 9999)

        self.preset_excel = pd.DataFrame(df1_values, columns=col2)
        self.preset_excel.iloc[:, -13:-11].fillna(0, inplace=True)


if __name__ == "__main__":
    print(1)

    # path = r"F:\my_objects\预设定相关工作的数据\0股份数据\数据"
    # df = pd.DataFrame()
    # for i in range(2021, 2024):
    #     df_tmp = pd.read_csv(rf'{path}\cold_rolling_{i}.csv')
    #     df = pd.concat([df, df_tmp])
    # df = df.reset_index(drop=True)
    # preset_cols = ['生产开始时刻(S11_0)', '入口卷号(S11_0)',
    #                'aDirNoAi',
    #                '出钢标记(S11_0)', '入口厚度(S11_0)', '实际目标厚度(S11_0)', '入口宽度(S11_0)',
    #                '带头板形平均值(S11_0)',
    #                '1号机架轧制力平均值(S11_0)', '2号机架轧制力平均值(S11_0)', '3号机架轧制力平均值(S11_0)',
    #                '4号机架轧制力平均值(S11_0)', '5号机架轧制力平均值(S11_0)',
    #                '1号机架工作辊弯辊实际值(S11_0)', '1号机架中间辊弯辊实际值(S11_0)',
    #                '1号机架中间上辊窜辊实际值(S11_0)',
    #                '2号机架工作辊弯辊实际值(S11_0)', '2号机架中间辊弯辊实际值(S11_0)',
    #                '2号机架中间上辊窜辊实际值(S11_0)',
    #                '3号机架工作辊弯辊实际值(S11_0)', '3号机架中间辊弯辊实际值(S11_0)',
    #                '3号机架中间上辊窜辊实际值(S11_0)',
    #                '4号机架工作辊弯辊实际值(S11_0)', '4号机架中间辊弯辊实际值(S11_0)',
    #                '4号机架中间上辊窜辊实际值(S11_0)',
    #                '5号机架工作辊弯辊实际值(S11_0)', '5号机架中间辊弯辊实际值(S11_0)',
    #                '5号机架中间上辊窜辊实际值(S11_0)',
    #                # 'C40'
    #                ]
    # standard_cols = ['time', 'steel', 'aDirNoAi', 'alloyCode',
    #                  'hEntry', 'hExit', 'wEntry', 'IU_50',
    #                  'RF_1', 'RF_2', 'RF_3', 'RF_4', 'RF_5',
    #                  'WRB_1', 'IRB_1', 'IRS_1',
    #                  'WRB_2', 'IRB_2', 'IRS_2',
    #                  'WRB_3', 'IRB_3', 'IRS_3',
    #                  'WRB_4', 'IRB_4', 'IRS_4',
    #                  'WRB_5', 'IRB_5', 'IRS_5',
    #                  # 'C40_base'
    #                  ]

    df = pd.read_csv(r'F:\my_objects\预设定相关工作\预设定表格生成\初值统计表\初值统计21-24.csv')
    standard_cols = ['steel', 'aDirNoAi', 'IU_50',
                     'WRB_1', 'WRB_2', 'WRB_3', 'WRB_4', 'WRB_5',
                     'IRB_1', 'IRB_2', 'IRB_3', 'IRB_4', 'IRB_5',
                     'IRS_1', 'IRS_2', 'IRS_3', 'IRS_4', 'IRS_5',
                     'alloyCode'
                     ]

    df.columns = standard_cols
    ic(df)

    gp = GeneratePreset(df)
    gp.data_model()
    gp.condat_ans()
    gp.preset_df_data.to_csv(r'预设定表格\设定表格更新1016.csv', index=False)
    gp.preset_excel.to_excel(r'预设定表格\设定表格更新1016(含有限元).xlsx', index=False)
    gp.preset_excel.to_csv(r'预设定表格\设定表格更新1016(含有限元).csv', index=False)
