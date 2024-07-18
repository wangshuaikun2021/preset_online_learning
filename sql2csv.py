"""
多进程读平台数据
"""
from multiprocessing import Pool
import os

import numpy as np
import pandas as pd
import pymysql
from tqdm import tqdm
from logger_config import setup_logger

import warnings

warnings.filterwarnings('ignore')


def presetNormalize(df):
    """
    设定值 -> 百分比
    WRB: 1000 700
    IRB: 2600 1800
    IRS: 285
    :param df:
    :return:
    """
    logger = setup_logger()

    cols = ['SY_PTM_STD1_WR_BendRoll_SV',
            'SY_PTM_STD1_WR_BendRoll_ACT',
            'SY_PTM_STD1_IMR_BendRoll_SV',
            'SY_PTM_STD1_IMR_BendRoll_ACT',
            'SY_PTM_STD2_WR_BendRoll_SV',
            'SY_PTM_STD2_WR_BendRoll_ACT',
            'SY_PTM_STD2_IMR_BendRoll_SV',
            'SY_PTM_STD2_IMR_BendRoll_ACT',
            'SY_PTM_STD3_WR_BendRoll_SV',
            'SY_PTM_STD3_WR_BendRoll_ACT',
            'SY_PTM_STD3_IMR_BendRoll_SV',
            'SY_PTM_STD3_IMR_BendRoll_ACT',
            'SY_PTM_STD4_WR_BendRoll_SV',
            'SY_PTM_STD4_WR_BendRoll_ACT',
            'SY_PTM_STD4_IMR_BendRoll_SV',
            'SY_PTM_STD4_IMR_BendRoll_ACT',
            'SY_PTM_STD5_WR_BendRoll_SV',
            'SY_PTM_STD5_WR_BendRoll_ACT',
            'SY_PTM_STD5_IMR_BendRoll_SV',
            'SY_PTM_STD5_IMR_BendRoll_ACT',
            'SY_PTM_STD1_IMR_TopRollShift_SV',
            'SY_PTM_STD1_IMR_TopRollShift_ACT',
            'SY_PTM_STD1_IMR_BotRollShift_ACT',
            'SY_PTM_STD2_IMR_TopRollShift_SV',
            'SY_PTM_STD2_IMR_TopRollShift_ACT',
            'SY_PTM_STD2_IMR_BotRollShift_ACT',
            'SY_PTM_STD3_IMR_TopRollShift_SV',
            'SY_PTM_STD3_IMR_TopRollShift_ACT',
            'SY_PTM_STD3_IMR_BotRollShift_ACT',
            'SY_PTM_STD4_IMR_TopRollShift_SV',
            'SY_PTM_STD4_IMR_TopRollShift_ACT',
            'SY_PTM_STD4_IMR_BotRollShift_ACT',
            'SY_PTM_STD5_IMR_TopRollShift_SV',
            'SY_PTM_STD5_IMR_TopRollShift_ACT',
            'SY_PTM_STD5_IMR_BotRollShift_ACT']
    # try:
    #     for c in cols:
    #         if 'WR_BendRoll' in c:
    #             for i in range(df[c].shape[0]):
    #                 if df[c][i] > 0:
    #                     df[c][i] = df[c][i] / 1000
    #                 else:
    #                     df[c][i] = df[c][i] / 700
    #         elif 'IMR_BendRoll' in c:
    #             for i in range(df[c].shape[0]):
    #                 if df[c][i] > 0:
    #                     df[c][i] = df[c][i] / 2600
    #                 else:
    #                     df[c][i] = df[c][i] / 1800
    #         elif 'IMR_BotRollShift' in c or 'IMR_TopRollShift' in c:
    #             for i in range(df[c].shape[0]):
    #                 df[c][i] = df[c][i] / 285
    #     return df
    # except:
    #     return None
    try:
        for c in cols:
            if 'WR_BendRoll' in c:
                df[c] = df[c].apply(lambda x: x / 1000 if x > 0 else x / 700)
            elif 'IMR_BendRoll' in c:
                df[c] = df[c].apply(lambda x: x / 2600 if x > 0 else x / 1800)
            elif 'IMR_BotRollShift' in c or 'IMR_TopRollShift' in c:
                df[c] = df[c] / 285
        return df
    except Exception as e:
        # print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")
        return None


def concat_high_ptm(df_ptm, df_high):
    """
    合并high ptm 表单
    :param df_ptm:
    :param df_high:
    :return:
    """
    # col_sql = pd.read_excel(f'SQL与PDA变量名对应.xlsx').dropna()['SQL']  # SQL与PDA变量名对应
    col_sql = ['SY_PTM_ExitWidth', 'SYEN_PTM_F5_Strip_Length', 'SYEN_PTM_F5_Flatness_Error', 'SYEN_PTM_F5_Release_Tilt_Control', 'SYEN_PTM_F5_Tilt_Control_Active', 'SYEN_PTM_F5_Release_WR_Bend_Control', 'SYEN_PTM_F5_WR_Bend_Control_Active', 'SYEN_PTM_F5_Release_IR_Bend_Control', 'SYEN_PTM_F5_IR_Bend_Control_Active', 'SYEN_PTM_F5_Release_IR_Shift_Control', 'SYEN_PTM_F5_IR_Shift_Control_Active', 'SYEN_PTM_F5_IR_Bend_Control_Active_MONI', 'SYEN_PTM_F5_IR_Shift_Control_Active_MONI', 'SYEN_PTM_F5_Flatness_Error_Tilt', 'SYEN_PTM_F5_Flatness_Error_WR_Bend', 'SYEN_PTM_F5_Flatness_Error_IR_Bend', 'SYEN_PTM_F5_Flatness_Error_IR_Shift', 'SYEN_PTM_F5_Add_Tilt', 'SYEN_PTM_F5_Add_WR_Bend', 'SYEN_PTM_F5_Add_IR_Bend', 'SYEN_PTM_F5_Add_IR_Shift', 'SY_PTM_STD1_EnThickness_SV', 'SY_PTM_STD1_EnThickness_ACT', 'SY_PTM_STD1_ExitThickness_ACT', 'SY_PTM_STD5_ExitThickness_SV', 'SY_PTM_STD5_ExitThickness_ACT', 'SY_PTM_STD1_ForwardSlip_ACT', 'SY_PTM_STD2_ForwardSlip_ACT', 'SY_PTM_STD3_ForwardSlip_ACT', 'SY_PTM_STD4_ForwardSlip_ACT', 'SY_PTM_STD5_ForwardSlip_ACT', 'SY_PTM_STD1_ForwardSlip_SV', 'SY_PTM_STD2_ForwardSlip_SV', 'SY_PTM_STD3_ForwardSlip_SV', 'SY_PTM_STD4_ForwardSlip_SV', 'SY_PTM_STD5_ForwardSlip_SV', 'SYEN_PTM_SLC_ACT_Speed_Bridle52', 'SYEN_PTM_SLC_Laser_Speed_Behind_S1', 'SYEN_PTM_SLC_Laser_Speed_Behind_S4', 'SYEN_PTM_SLC_Laser_Speed_Behind_S5', 'SYEN_PTM_SLC_ACT_Speed_Exit_Roll', 'SY_PTM_STD1_RollSpeed_ACT', 'SY_PTM_STD2_RollSpeed_ACT', 'SY_PTM_STD3_RollSpeed_ACT', 'SY_PTM_STD4_RollSpeed_ACT', 'SY_PTM_STD5_RollSpeed_ACT', 'SYEN_PTM_SLC_Strip_Speed_S1_S2', 'SYEN_PTM_SLC_Strip_Speed_S2_S3', 'SYEN_PTM_SLC_Strip_Speed_S3_S4', 'SYEN_PTM_SLC_Strip_Speed_S4_S5', 'SYEN_PTM_SLC_Strip_Speed_Afer_S5', 'SYEN_PTM_N1_ITC_Tension_ACT_DS', 'SYEN_PTM_N1_ITC_Tension_ACT_OS', 'SYEN_PTM_N2_ITC_Tension_ACT_DS', 'SYEN_PTM_N2_ITC_Tension_ACT_OS', 'SYEN_PTM_N3_ITC_Tension_ACT_DS', 'SYEN_PTM_N3_ITC_Tension_ACT_OS', 'SYEN_PTM_N4_ITC_Tension_ACT_DS', 'SYEN_PTM_N4_ITC_Tension_ACT_OS', 'SYEN_PTM_N5_ITC_Tension_ACT_DS', 'SYEN_PTM_N5_ITC_Tension_ACT_OS', 'SYEN_PTM_N5_Tension_ACT_DS_S5', 'SYEN_PTM_N5_Tension_ACT_OS_S5', 'SYEN_PTM_F5_ACT_S5_Smoothed', 'SY_PTM_STD1_RollGap_ACT', 'SY_PTM_STD1_RollForce_ACT', 'SY_PTM_STD1_TiltDStoOS_ACT', 'SY_PTM_STD2_RollGap_ACT', 'SY_PTM_STD2_RollForce_ACT', 'SY_PTM_STD2_TiltDStoOS_ACT', 'SY_PTM_STD3_RollGap_ACT', 'SY_PTM_STD3_RollForce_ACT', 'SY_PTM_STD3_TiltDStoOS_ACT', 'SY_PTM_STD4_RollGap_ACT', 'SY_PTM_STD4_RollForce_ACT', 'SY_PTM_STD4_TiltDStoOS_ACT', 'SY_PTM_STD5_RollGap_ACT', 'SY_PTM_STD5_RollForce_ACT', 'SY_PTM_STD5_TiltDStoOS_ACT', 'SY_PTM_STD1_WR_BendRoll_SV', 'SY_PTM_STD1_IMR_BendRoll_SV', 'SY_PTM_STD1_IMR_BendRoll_ACT', 'SY_PTM_STD2_WR_BendRoll_SV', 'SY_PTM_STD2_IMR_BendRoll_SV', 'SY_PTM_STD2_IMR_BendRoll_ACT', 'SY_PTM_STD3_WR_BendRoll_SV', 'SY_PTM_STD3_IMR_BendRoll_SV', 'SY_PTM_STD3_IMR_BendRoll_ACT', 'SY_PTM_STD4_WR_BendRoll_SV', 'SY_PTM_STD4_IMR_BendRoll_SV', 'SY_PTM_STD4_IMR_BendRoll_ACT', 'SY_PTM_STD5_WR_BendRoll_SV', 'SY_PTM_STD5_IMR_BendRoll_SV', 'SY_PTM_STD5_IMR_BendRoll_ACT', 'SY_PTM_STD1_IMR_TopRollShift_SV', 'SY_PTM_STD1_IMR_TopRollShift_ACT', 'SY_PTM_STD1_IMR_BotRollShift_ACT', 'SY_PTM_STD2_IMR_TopRollShift_SV', 'SY_PTM_STD2_IMR_TopRollShift_ACT', 'SY_PTM_STD2_IMR_BotRollShift_ACT', 'SY_PTM_STD3_IMR_TopRollShift_SV', 'SY_PTM_STD3_IMR_TopRollShift_ACT', 'SY_PTM_STD3_IMR_BotRollShift_ACT', 'SY_PTM_STD4_IMR_TopRollShift_SV', 'SY_PTM_STD4_IMR_TopRollShift_ACT', 'SY_PTM_STD4_IMR_BotRollShift_ACT', 'SY_PTM_STD5_IMR_TopRollShift_SV', 'SY_PTM_STD5_IMR_TopRollShift_ACT', 'SY_PTM_STD5_IMR_BotRollShift_ACT', 'SY_PTM_STD1_ScrewDw_Ratio', 'SY_PTM_STD2_ScrewDw_Ratio', 'SY_PTM_STD3_ScrewDw_Ratio', 'SY_PTM_STD4_ScrewDw_Ratio', 'SY_PTM_STD5_ScrewDw_Ratio', 'SYEN_PTM_ROLCH111_PTMRoll23_DB1016_REAL84', 'SYEN_PTM_ROLCH112_PTMRoll24_DB1016_REAL84', 'SYEN_PTM_ROLCH113_PTMRoll25_DB1016_REAL84', 'SYEN_PTM_ROLCH114_PTMRoll26_DB1016_REAL84', 'SYEN_PTM_ROLCH115_PTMRoll27_DB1016_REAL84', 'SY_PTM_STD1_WR_BendRoll_ACT', 'SY_PTM_STD2_WR_BendRoll_ACT',
    'SY_PTM_STD3_WR_BendRoll_ACT', 'SY_PTM_STD4_WR_BendRoll_ACT', 'SY_PTM_STD5_WR_BendRoll_ACT']
    col_sql = pd.Series(col_sql)
    ptmC = df_ptm.columns
    highC = df_high.columns
    # columns_ptm = []  # ptm变量
    # columns_high = []  # high变量
    # for c in col_sql:
    #     if c in ptmC:
    #         columns_ptm.append(c)
    #     elif c in highC:
    #         columns_high.append(c)
    #     else:
    #         continue
    columns_ptm = col_sql[col_sql.isin(ptmC)].tolist()
    columns_high = col_sql[col_sql.isin(highC)].tolist()
    iuCol = []  # 板形仪变量
    for i in range(1, 63):
        iuCol.append(f'SY_PTM_Flatness_IU{i}')
    columns_high.extend(iuCol)

    df_ptm = df_ptm[columns_ptm]  # ptm表单
    df_high = df_high[columns_high]  # high表单

    df_ptm = pd.DataFrame(np.repeat(df_ptm.values, 2, axis=0), columns=df_ptm.columns)  # 频率协同
    df_ptm = df_ptm.reset_index(drop=True)
    df_high = df_high.reset_index(drop=True)
    df = pd.concat([df_ptm, df_high], axis=1)
    l = df['SYEN_PTM_F5_Strip_Length'].dropna().shape[0]  # 根据轧制长度截断
    # print(l)
    df = df.iloc[:l - 1, :]
    df = df.fillna(0)  # 填空值
    return df


def sqlToCsv(steel,high_table, ptm_table, save_path='csv', host='localhost', port=3306, user='root', passwd='root', db='shougang_cold_rolling', save=False):
    """
    :param steel: 热轧钢卷号, 以H开头, 形如H122C26800500_1, H122C26800500, H122C26800500_1_0, H122C26800500_1.csv等
    :param save_path: 储存路径，不包含文件名
    :return:
    """
    logger = setup_logger()

    if not high_table or not ptm_table:
        return None
    conn = pymysql.connect(
        host=host,
        port=port,
        user=user,
        passwd=passwd,  # password也可以
        db=db,
        charset='utf8')
    cur = conn.cursor()
    cur.execute(
        "select SY_PTM_HotCoilID, SY_PTM_ColdCoilID from sy_ptm_das_l3_singledata where SY_PTM_HotCoilID = '%s'" % steel[
                                                                                                                   :13])
    temp = cur.fetchall()
    cur.close()
    if len(temp) == 0:
        # print(f"未找到{steel}")
        logger.warning(f"未找到{steel}")
    else:
        for i in range(len(temp)):
            coldId = temp[i][-1]
            # print(coldId)
            df = pd.read_sql("select * from %s where STEEL_Coil_Id = '%s'" % (high_table, coldId), con=conn)
            df = presetNormalize(df)
            if df is None:
                return None
            df2 = pd.read_sql("select * from %s where STEEL_Coil_Id = '%s'" % (ptm_table, coldId), con=conn)

            df = concat_high_ptm(df2, df)
            # print(df)
            if save:
                df.to_csv(fr'{save_path}\{temp[i][0]}_{i + 1}_sql.csv', index=False)
            break
    conn.close()
    return df


def getSteelsFromT(start_time, end_time):
    """
    获取指定时间段的冷轧、热轧钢卷号
    :param start_time: like 2023-01-01 06:54:33
    :param end_time: like 2023-01-03 06:54:33
    :return:    dataframe:
                HotId  ColdId
                H....  S.....
    """
    conn = pymysql.connect(
        host='localhost',
        port=3306,
        user='root',
        passwd='root',  # password也可以
        db='shougang_cold_rolling',
        charset='utf8')
    steels = pd.read_sql("SELECT SY_PTM_HotCoilID, SY_PTM_ColdCoilID, high_table, ptm_table "
                         "FROM sy_ptm_das_l3_singledata "
                         "WHERE SY_PTM_Prod_StartTime >= '%s' AND SY_PTM_Prod_StartTime <= '%s'"
                         % (start_time, end_time), con=conn)
    conn.close()
    return steels


if __name__ == "__main__":
    start_time = '2023-08-01 06:54:33'
    end_time = '2023-08-03 06:54:33'
    steels = getSteelsFromT(start_time, end_time)
    size = 12
    allNum = steels.shape[0]
    for i in tqdm(range(0, allNum, size)):
        pool = Pool(processes=size)
        end = i + size
        if end >= allNum:
            end = allNum
        steel = steels.values[:, 0][i:end]  # 同时处理的钢卷
        pool.map(sqlToCsv, steel)
        pool.close()
        pool.join()
