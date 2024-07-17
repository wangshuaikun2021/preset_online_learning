import functools
import multiprocessing
import os
import threading
import time

import numpy as np
import pandas as pd
import pymysql
import schedule as schedule
# from icecream import ic
from loguru import logger

from get_preset_class import GeneratePreset
# from tqdm import tqdm
from updata_preset_table import get_start
from logger_config import logger


def multi_func(row, host, port, user, passwd, db):
    last_id, hotId, coldId, high_table, ptm_table = row
    # print(last_id, hotId, coldId, high_table, ptm_table)
    logger.info(f"Processing {hotId}")
    start_value = get_start(hotId, high_table, ptm_table, host, port, user, passwd, db)  # 获取初值表
    # print(start_value)
    if start_value:
        # print(start_value)
        start_df = start_value
        fea_cols = start_value[1]
        return start_df


# def read_db(last_id, save_path, size=12, host='localhost',
#             port=3306,
#             user='root',
#             passwd='123456',
#             db='shougang', ):
#     conn = pymysql.connect(
#         host=host,
#         port=port,
#         user=user,
#         passwd=passwd,
#         db=db,
#         charset='utf8')
#     # print(fr"{last_id=}")
#     steels = pd.read_sql(
#         f"SELECT id, SY_PTM_HotCoilID, SY_PTM_ColdCoilID, high_table, ptm_table FROM sy_ptm_das_l3_singledata WHERE id > {last_id}",
#         con=conn)
#     conn.close()
#     steels = steels.values
#     if len(steels) > 1000:
#         steels = steels[:1000]
#
#     last_id = steels[-1][0] if len(steels) > 0 else last_id
#     start_df = []
#
#     fea_cols = []
#     flag_col = True
#     for i in range(0, len(steels), size):
#         pool = multiprocessing.Pool(processes=size)
#         if i + size <= len(steels):
#             row = steels[i:i + size]
#         else:
#             row = steels[i:]
#         multi_func_ = functools.partial(multi_func, host=host, port=port, user=user, passwd=passwd, db=db)
#         start_df_ = pool.map(multi_func_, row)
#         # print(start_df_)
#         temp = []
#         for a_start in start_df_:
#             if a_start:
#                 temp.append(a_start[0])
#                 if flag_col:
#                     fea_cols = a_start[1]
#                     ic(fea_cols)
#                     flag_col = False
#         # temp = [a_start[0] for a_start in start_df_ if a_start]
#         start_df.extend(temp)
#         ic(len(start_df))
#         # ic(fea_cols)
#
#         pool.close()
#         pool.join()
#     print(len(start_df))
#     if len(start_df) > 0:
#         start_df = np.array(start_df)
#         start_df = pd.DataFrame(start_df, columns=fea_cols)
#         print(start_df)
#         start_df.to_csv(fr"{save_path}\{last_id}.csv", index=False)
#     return last_id
def read_db(last_id, save_path, size=12, host='localhost', port=3306, user='root', passwd='123456', db='shougang'):
    """
    从数据库中读取数据并进行处理

    :param last_id: 上次读取的最后一个ID
    :param save_path: 保存文件的路径
    :param size: 处理批量大小
    :param host: 数据库主机
    :param port: 数据库端口
    :param user: 数据库用户名
    :param passwd: 数据库密码
    :param db: 数据库名称
    :return: 最新的最后一个ID
    """
    conn = pymysql.connect(
        host=host,
        port=port,
        user=user,
        passwd=passwd,
        db=db,
        charset='utf8'
    )

    # 从数据库读取数据
    steels = pd.read_sql(
        f"SELECT id, SY_PTM_HotCoilID, SY_PTM_ColdCoilID, high_table, ptm_table FROM sy_ptm_das_l3_singledata WHERE id > {last_id}",
        con=conn
    )
    conn.close()

    steels = steels.values
    if len(steels) > 1000:
        steels = steels[:1000]

    last_id = steels[-1][0] if len(steels) > 0 else last_id
    start_df = []

    fea_cols = []
    flag_col = True

    for i in range(0, len(steels), size):
        with multiprocessing.Pool(processes=size) as pool:
            row = steels[i:i + size] if i + size <= len(steels) else steels[i:]
            multi_func_ = functools.partial(multi_func, host=host, port=port, user=user, passwd=passwd, db=db)
            start_df_ = pool.map(multi_func_, row)

        temp = [a_start[0] for a_start in start_df_ if a_start]

        if flag_col and temp:
            fea_cols = start_df_[0][1]
            # ic(fea_cols)
            flag_col = False

        start_df.extend(temp)
        # ic(len(start_df))

    # print(len(start_df))
    if start_df:
        start_df = pd.DataFrame(start_df, columns=fea_cols)
        logger.info(f"Saving the data to {save_path}/{last_id}.csv")
        start_df.to_csv(f"{save_path}/{last_id}.csv", index=False)
        logger.info(f"Data saved to {save_path}/{last_id}.csv")
    return last_id


def generate_table(startValPath, savepath_tabel, num_limit):
    # 获取文件夹下的所有文件
    files = os.listdir(startValPath)

    # 构建文件路径列表，并获取每个文件的修改时间
    file_paths = [os.path.join(startValPath, file) for file in files]
    file_times = [os.path.getmtime(file) for file in file_paths]

    # 将文件列表按照修改时间排序
    files = [file for _, file in sorted(zip(file_times, files))]
    name = files[-1].split('.')[0]
    df = []
    need_cols = ['steel', 'policyNo', 'iu50_mean',
                 'WRB1', 'WRB2', 'WRB3', 'WRB4', 'WRB5',
                 'IRB1', 'IRB2', 'IRB3', 'IRB4', 'IRB5',
                 'IRS1', 'IRS2', 'IRS3', 'IRS4', 'IRS5',
                 'alloyCode']

    def is_in(sublist, mainlist):
        return all(item in mainlist for item in sublist)

    for f in files:
        if '.csv' in f:
            temp_df = pd.read_csv(fr'{startValPath}\{f}')[need_cols]
            if is_in(need_cols, temp_df.columns.tolist()):
                df.append(temp_df)
    df = pd.concat(df).reset_index(drop=True)
    df = df.drop_duplicates(subset='steel', keep='last').reset_index(drop=True)
    if len(files) > 100:
        # 请空startValPath文件夹
        for f in files:
            os.remove(fr'{startValPath}\{f}')
        df.to_csv(fr'{startValPath}\{name}.csv', index=False)
    # while True:

    standard_cols = ['steel', 'aDirNoAi', 'IU_50',
                     'WRB_1', 'WRB_2', 'WRB_3', 'WRB_4', 'WRB_5',
                     'IRB_1', 'IRB_2', 'IRB_3', 'IRB_4', 'IRB_5',
                     'IRS_1', 'IRS_2', 'IRS_3', 'IRS_4', 'IRS_5',
                     'alloyCode'
                     ]
    df.columns = standard_cols
    # df = df.drop_duplicates(subset='steel', keep='last').reset_index(drop=True)
    gp = GeneratePreset(df)
    gp.data_model()
    gp.condat_ans()
    # gp.preset_df_data.to_csv(r'预设定表格\设定表格更新1016.csv', index=False)
    gp.preset_excel.to_excel(fr'{savepath_tabel}\STDADIRPASS_AI.xlsx', index=False)


# def mainFunc(last_id, savepath, savepath_tabel, size, update_t, host, port, user, passwd, db, num_limit):
#     """
#     主函数，循环运行
#     :param last_id:
#     :return:
#     """
#
#     class SharedData:
#         lastId = 0
#
#     SD = SharedData()
#     SD.lastId = last_id
#     read_database_with_last_id = functools.partial(read_db, save_path=savepath, size=size, host=host,
#                                                    port=port,
#                                                    user=user,
#                                                    passwd=passwd,
#                                                    db=db)
#
#     # def run_task(last_id):
#     #     # global last_id  # 声明全局变量
#     #     last_id = read_database_with_last_id(last_id=last_id)
#     #     # savepath_tabel = ''
#     #     generate_table(savepath, savepath_tabel, num_limit)
#     #     print("Last ID:", last_id)
#     #     return last_id
#
#     def run_task():
#         try:
#             SD.lastId = read_database_with_last_id(SD.lastId)
#             # savepath_tabel = ''
#             generate_table(savepath, savepath_tabel, num_limit)
#             print("Last ID:", SD.lastId)
#         except Exception as e:
#             print(e)
#         # return last_id
#
#     schedule.every().day.at("20:26").do(run_task)
#     # schedule.every(5).seconds.do(run_task)
#     # 运行任务调度
#     while True:
#         run_task()
#         # schedule.run_pending()
#         time.sleep(1)


def mainFunc(last_id, savepath, savepath_tabel, size, update_t, host, port, user, passwd, db, num_limit):
    """
    主函数，循环运行
    :param last_id:
    :return:
    """

    class SharedData:
        lastId = 0

    SD = SharedData()
    SD.lastId = last_id
    read_database_with_last_id = functools.partial(read_db, save_path=savepath, size=size, host=host,
                                                   port=port,
                                                   user=user,
                                                   passwd=passwd,
                                                   db=db)

    task_lock = threading.Lock()

    def run_read_task():
        if task_lock.locked():
            logger.info("Read task is already running. Skipping this run.")
            return
        with task_lock:
            try:
                SD.lastId = read_database_with_last_id(SD.lastId)
                # logger.info(f"Last ID: {SD.lastId}")
                # print(111)
            except Exception as e:
                logger.error(e)

    def run_generate_task():
        if task_lock.locked():
            logger.info("Generate task is already running. Skipping this run.")
            return
        with task_lock:
            try:
                generate_table(savepath, savepath_tabel, num_limit)
                logger.info("Table generated.")
            except Exception as e:
                logger.error(e)

    # Schedule the read task to run every 2 hours
    schedule.every(12).hours.do(run_read_task)

    # Schedule the generate task to run every 30 days
    schedule.every(30).days.do(run_generate_task)

    while True:
        schedule.run_pending()
        run_read_task()
        time.sleep(1)


if __name__ == '__main__':
    # logger.add("logs/log_{time}.log",
    #            rotation="100 MB",  # 每当日志文件达到 100 MB 时，新建一个日志文件
    #            retention=100,  # 保留最多 100 个日志文件
    #            compression="zip")  # 压缩旧的日志文件为zip
    logger.info("Starting the program.")
    # save_path_start = os.path.dirname(__file__) + r'\初值表保存'
    save_path_start = r'初值表保存'
    # print(save_path_start)
    files = os.listdir(save_path_start)

    # 构建文件路径列表，并获取每个文件的修改时间
    file_paths = [os.path.join(save_path_start, file) for file in files]
    file_times = [os.path.getmtime(file) for file in file_paths]

    # 将文件列表按照修改时间排序
    files = [file for _, file in sorted(zip(file_times, files))]
    name = files[-1].split('.')[0]
    last_id = int(name)
    last_id = 338200
    save_path_preset = r'预设定表格保存'
    # save_path_preset = os.path.dirname(__file__) + r'\预设定表格保存'
    # print(save_path_preset)
    logger.info(f"save_path_start: {save_path_start}")
    logger.info(f"save_path_preset: {save_path_preset}")
    mainFunc(last_id, save_path_start, save_path_preset, size=6, update_t='15:15', host='localhost', port=3306,
             user='root', passwd='root', db='shougang_cold_rolling', num_limit=2000)
