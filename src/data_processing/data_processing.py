import pandas as pd
import numpy as np
import os


def is_winter_month(file_name):
    # 提取文件名中的月份
    month = int(file_name[4:6])
    # 检查是否为11月到次年2月的数据
    return month in [11, 12, 1, 2]


def clean_csv(file_path):
    # 加载 CSV 文件
    df = pd.read_csv(file_path, encoding='GBK')

    df['时间'] = pd.to_datetime(df['时间'], format='%Y/%m/%d %H:%M')
    # 替换错误值 -99 和 -99.9 为 NaN
    df.replace([-99, -99.9], np.nan, inplace=True)

    data_columns = df.columns.drop('时间')

    # 应用拉依达准则来删除异常值
    # 计算平均值和标准差
    # 仅对数值列计算平均值和标准差
    numeric_df = df.select_dtypes(include=[np.number])
    mean = numeric_df.mean()
    std = numeric_df.std()

    # 定义异常值的界限
    lower_limit = mean - 3 * std
    upper_limit = mean + 3 * std

    # 删除异常值
    for column in data_columns:
        df = df[(df[column] > lower_limit[column]) & (df[column] < upper_limit[column]) | df[column].isna()]

    return df


def process_directory(source_directory, target_directory):
    # 创建目标处理目录
    if not os.path.exists(target_directory):
        os.makedirs(target_directory)

    # 遍历每个服务区目录
    for service_area in os.listdir(source_directory):
        service_area_path = os.path.join(source_directory, service_area)

        if os.path.isdir(service_area_path):
            for file in os.listdir(service_area_path):
                # 检查文件名是否符合条件
                if file.endswith('.csv') and is_winter_month(file):
                    file_path = os.path.join(service_area_path, file)
                    df_cleaned = clean_csv(file_path)

                    # 目标子目录
                    target_subdir = os.path.join(target_directory, service_area)
                    if not os.path.exists(target_subdir):
                        os.makedirs(target_subdir)

                    # 目标文件路径
                    target_file_path = os.path.join(target_subdir, file)

                    # 保存清洗后的数据
                    df_cleaned.to_csv(target_file_path, index=False, encoding='GBK')


# 输入目录和输出目录
source_directory = 'E:/Pave-Temperature-Sense-Calculate-Platform/src/data_collection/2022_2023'
target_directory = 'data_processing/2022_2023'

# 处理指定目录中的文件
process_directory(source_directory, target_directory)
