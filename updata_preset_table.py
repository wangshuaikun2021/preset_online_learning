import time

import numpy as np
import pymysql

from sql2csv import sqlToCsv
from logger_config import setup_logger



def get_width_level(val, isMm=True):
    """宽度等级"""
    width_lev = ['[700,900)', '[900,1100)', '[1100,1300)', '[1300,1500)', '[1500,1700)', '[1700,1900)']
    res = None
    index = None
    if isMm:
        coff = 1
    else:
        coff = 1e3
    for width in width_lev:
        start, end = list(map(int, width.replace('[', '').replace(')', '').split(',')))
        if start <= val * coff < end:
            res = width
            index = width_lev.index(res)
            continue
    return res, index


def get_thick_level(val, isEntry=True, isMm=True):
    """厚度等级"""
    if isEntry:
        thick_lev = ['[1.45, 2.75)', '[2.75, 3.25)', '[3.25, 3.75)', '[3.75, 4.25)', '[4.25, 4.75)', '[4.75, 5.25)',
                     '[5.25, 6.55)']
    else:
        thick_lev = ['[0.160,0.340)', '[0.340,0.500)', '[0.500,0.650)', '[0.650,0.750)', '[0.750,0.850)',
                     '[0.850,0.950)', '[0.950,1.250)', '[1.250,1.600)', '[1.600,2.050)', '[2.050,2.600)']

    res = None
    index = None
    if isMm:
        coff = 1
    else:
        coff = 1e3
    for thick in thick_lev:
        start, end = list(map(float, thick.replace('[', '').replace(')', '').split(',')))
        if start <= val * coff < end:
            res = thick
            index = thick_lev.index(res)
            break
    return res, index


def serialNum(steel_lev, thick_num_in, thick_num_out, width_num):
    """return: 带钢序列号"""
    if None in (steel_lev, thick_num_in, thick_num_out, width_num):
        return None
    else:
        aDirNoAi = f'{steel_lev}{thick_num_in}{thick_num_out}{width_num}0'
        return aDirNoAi


def ST_NO(steel, host='localhost',
          port=3306,
          user='root',
          passwd='123456',
          db='shougang'):
    """
    get 出钢标记
    :return:
    """
    logger = setup_logger()

    conn = pymysql.connect(
        host=host,
        port=port,
        user=user,
        passwd=passwd,
        db=db,
        charset='utf8')
    cur = conn.cursor()
    # steel = 'H123116400700'
    sql = "SELECT ST_NO FROM hmmhr01tm WHERE MAT_NO = '%s'" % steel
    cur.execute(sql)
    cgbjs = cur.fetchall()

    C40s = ""
    sql = "SELECT FM_C40_AVG FROM qa_hsm2_fm1_monodrome WHERE Coil_ID = '%s'" % steel
    sql2 = "SELECT C40_Mean FROM qa_hsm1_fm1_monodrome WHERE Coil_ID = '%s'" % steel
    cur.execute(sql)
    C40_1 = cur.fetchall()
    cur.execute(sql2)
    C40_2 = cur.fetchall()
    C40s = C40_1 if len(C40_1) > 0 else C40_2
    if len(cgbjs) == 0:
        # print('未找到出钢标记')
        logger.warning(f"未找到出钢标记 {steel}")
        return None, None
    elif len(C40s) == 0:
        # print('未找到C40')
        logger.warning(f"未找到C40 {steel}")
        return cgbjs[0][0], None
    else:
        cur.close()
        conn.close()
        return cgbjs[0][0], C40s[0][0]


def getCols():
    """
    cols_all: 全部变量名,
    col_preset: 15个设定值,
    col_policy: 宽度和两个厚度
    :return: cols_all, col_preset, col_policy
    """
    col_preset = ['SY_PTM_STD1_WR_BendRoll_SV',
                  'SY_PTM_STD2_WR_BendRoll_SV',
                  'SY_PTM_STD3_WR_BendRoll_SV',
                  'SY_PTM_STD4_WR_BendRoll_SV',
                  'SY_PTM_STD5_WR_BendRoll_SV',
                  'SY_PTM_STD1_IMR_BendRoll_SV',
                  'SY_PTM_STD2_IMR_BendRoll_SV',
                  'SY_PTM_STD3_IMR_BendRoll_SV',
                  'SY_PTM_STD4_IMR_BendRoll_SV',
                  'SY_PTM_STD5_IMR_BendRoll_SV',
                  'SY_PTM_STD1_IMR_TopRollShift_SV',
                  'SY_PTM_STD2_IMR_TopRollShift_SV',
                  'SY_PTM_STD3_IMR_TopRollShift_SV',
                  'SY_PTM_STD4_IMR_TopRollShift_SV',
                  'SY_PTM_STD5_IMR_TopRollShift_SV']  # 设定值
    col_policy = ['SY_PTM_ExitWidth', 'SY_PTM_STD1_EnThickness_ACT', 'SY_PTM_STD5_ExitThickness_ACT']
    col_others = ['SYEN_PTM_F5_Flatness_Error', 'SYEN_PTM_F5_Strip_Length',
                  'SY_PTM_STD1_RollForce_ACT', 'SY_PTM_STD2_RollForce_ACT', 'SY_PTM_STD3_RollForce_ACT',
                  'SY_PTM_STD4_RollForce_ACT', 'SY_PTM_STD5_RollForce_ACT']

    cols_all = []
    cols_all.extend(col_preset)
    cols_all.extend(col_policy)
    cols_all.extend(col_others)
    return cols_all, col_preset, col_policy


# def dropHead(length):
#     """
#     :param length:
#     :return: idx_start, idx_end
#     """
#     length = np.array(length)
#     idx_start, idx_end = 0, -1
#     for i in range(len(length)):
#         if length[i] < 10:
#             idx_start = i
#             break
#     length = length[idx_start:]
#     l_max = length.max()
#     for i in range(len(length)):
#         if length[-(i + 1)] == l_max:
#             idx_end = -i - 1
#             break
#     return idx_start, idx_end


def dropHead(length):
    """
    :param length: 输入的长度列表或数组
    :return: idx_start, idx_end
    """
    length = np.array(length)
    # 找到第一个小于10的元素索引
    idx_start = np.argmax(length < 10)
    # 截取从 idx_start 开始的数组
    length = length[idx_start:]
    # 找到截取后数组的最大值
    l_max = length.max()
    # 找到最后一个等于最大值的元素索引
    idx_end = len(length) - np.argmax(length[::-1] == l_max) - 1
    return idx_start, idx_end


def get_start(steel, high_table, ptm_table, host, port, user, passwd, db):
    logger = setup_logger()

    cols_all, col_ctrl, col_policy = getCols()  # all, 设定值, 策略号的特征
    # steel = 'H123116309800'
    row = [steel]  # 值
    feature_name = ['steel']  # 名
    df = sqlToCsv(steel, high_table, ptm_table, save_path='csv', host=host, port=port, user=user, passwd=passwd, db=db,
                  save=False)  # sql2csv
    if df is None:
        # print("设定值有问题")
        logger.warning(f"设定值有问题 {steel}")
        return None
    if df.shape[0] > 100:
        length = df['SYEN_PTM_F5_Strip_Length']
        idx_start, idx_end = dropHead(length)  # 去除带头带尾不属于这一卷的样本
        if idx_end == -1:
            df = df.iloc[idx_start:, :].reset_index(drop=True)
        else:
            df = df.iloc[idx_start:idx_end + 1, :].reset_index(drop=True)
        df = df[cols_all]
        length = df['SYEN_PTM_F5_Strip_Length'].values
        n_l = len(length)
        if length.max() > 600:
            width, h_in, h_out = df[col_policy].values[0]
            cgbj, C40 = ST_NO(steel, host, port, user, passwd, db)  # 获取出钢标记
            if not cgbj:
                return None
            else:
                cgbj2bpph = np.load('cgbj2bpph.npy', allow_pickle=True).item()
                if cgbj in cgbj2bpph.keys():
                    bpph = cgbj2bpph[cgbj]
                    row.extend([bpph, h_in, h_out, width])  # 添加策略信息
                    feature_name.extend(['alloyCode', 'hEntry', 'hExit', 'wEntry'])
                    policyNo = policy_generate(bpph, h_in, h_out, width)  # 生成策略号
                    if policyNo is not None:
                        row.append(policyNo)
                        feature_name.append('policyNo')
                    else:
                        # print(f"策略号为空")
                        logger.warning(f"策略号为空 {steel}")
                        return None
                else:
                    # print(f"出钢标记{cgbj}不存在")
                    logger.warning(f"出钢标记{cgbj}不存在 {steel}")
                    return None

            controls = df[col_ctrl].values[0]
            row.extend(controls)
            col_ctrl_another = ['WRB1', 'WRB2', 'WRB3', 'WRB4', 'WRB5',
                                'IRB1', 'IRB2', 'IRB3', 'IRB4', 'IRB5',
                                'IRS1', 'IRS2', 'IRS3', 'IRS4', 'IRS5']
            feature_name.extend(col_ctrl_another)
            iu = df['SYEN_PTM_F5_Flatness_Error'].values
            iu_mean = iu.mean()
            iu50_mean = iu[length <= 50].mean()
            iu100_mean = iu[length <= 100].mean()
            row.extend([iu_mean, iu50_mean, iu100_mean])  # 添加IU
            feature_name.extend(['iu_mean', 'iu50_mean', 'iu100_mean'])
            for k in range(1, 6, 1):
                rf = df[f'SY_PTM_STD{k}_RollForce_ACT'].values
                rf_mean = rf.mean()
                rf50_mean = rf[length <= 50].mean()
                rfMid_mean = rf[int(0.1 * n_l):int(0.8 * n_l)].mean()
                row.extend([rf_mean, rf50_mean, rfMid_mean])  # 添加轧制力
                feature_name.extend([f'rf_mean_{k}', f'rf50_mean_{k}', f'rfMid_mean_{k}'])
            row.append(C40)  # 添加C40
            feature_name.append('C40_average')
            return row, feature_name

        else:
            # print("带钢长度过短")
            logger.warning(f"带钢长度过短 {steel}")
            return None
    else:
        # print(f"带钢样本点过少：{df.shape[0]}")
        logger.warning(f"带钢{steel}样本点过少：{df.shape[0]}")
        return None


def policy_generate(bpph, h_in, h_out, width):
    """
    :param bpph: 板坯牌号
    :param h_in:
    :param h_out:
    :param width:
    :return: 策略号
    """
    bpph2gd = np.load('bpph2gd.npy', allow_pickle=True).item()
    if bpph in bpph2gd.keys():
        gd = bpph2gd[bpph]
    else:
        return None
    hEntryBound, hEntryId = get_thick_level(h_in)
    hExitBound, hExitId = get_thick_level(h_out, isEntry=False)
    wBound, wId = get_width_level(width)
    policyNo = serialNum(gd, hEntryId, hExitId, wId)  # 策略号
    return policyNo


if __name__ == '__main__':
    steel = 'H123116309800'
    start = time.time()
    row = get_start(steel)
    print(row)
    end = time.time()
    print(end - start)
