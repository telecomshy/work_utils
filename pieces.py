import json
import logging.config
import shutil
import csv
import yaml
import warnings
from datetime import datetime
from functools import partial
from operator import sub
from pathlib import Path
from collections.abc import Mapping
from openpyxl import Workbook

warnings.simplefilter('always')


def read_yaml(file):
    """
    读取yaml配置文件

    :param file: 文件路径
    :return: 返回配置项组成的字典
    """
    with open(file, 'rt', encoding='utf8') as f:
        return yaml.load(f, Loader=yaml.FullLoader)


def kafka_key_serializer():
    """序列化kafka的key值"""
    return partial(str.encode, encoding='utf8')


def kafka_value_serializer(value):
    """序列化kafka的value值"""
    return json.dumps(value, ensure_ascii=False).encode('utf8')


def abs_path(*path, anchor):
    """
    以当前文件所在路径为锚点，返回相对路径的绝对路径

    :param anchor: 绝对路径的锚点
    :param path: 相对路径
    :return: 绝对路径
    """
    path, anchor = Path(*path), Path(anchor)
    if path.is_absolute():
        raise TypeError('Path must be Relative path.')
    if not anchor.is_absolute():
        raise TypeError('Anchor must be absolute path.')
    path = anchor / path
    return path.resolve()  # 消除相对目录中的.和..


def get_logger(name, setting_dict, file_anchor=None):
    """
    根据配置字典和名称创建相应的logger对象

    :param name: logger名称
    :param setting_dict: logger的配置字典
    :param file_anchor: 文件锚点，某些情况下，比如crontab定时任务，所有路径必须为绝对路径，该参数会和配置文件中的路径拼接形成
                        最终的路径，如果配置文件中的路径已经为绝对路径，则会抛出警告，并以配置文件为准
    :return: logger对象
    """
    for handler in setting_dict['handlers'].values():
        filename = handler.get('filename', '')
        if filename:
            filename = Path(filename)
            if filename.is_absolute():
                warnings.warn(f"{name} logger's file already is absolute path!", ResourceWarning)
            else:
                if file_anchor:
                    filename = abs_path(filename, anchor=file_anchor)
                    handler['filename'] = str(filename)
            filename.parent.mkdir(parents=True, exist_ok=True)
    logging.config.dictConfig(setting_dict)
    return logging.getLogger(name)


def empty_dir(path, folder=True, file=True):
    """清空文件夹下所有内容"""
    path = Path(path)
    for p in path.iterdir():
        if folder and p.is_dir():
            shutil.rmtree(p, ignore_errors=True)
        if file and p.is_file():
            p.unlink(missing_ok=True)


def get_time_delta(new_time, old_time, unit='second', absolute=False, time_format='%Y-%m-%d %H:%M:%S'):
    """返回两个时间点之间的间隔"""
    time_unit = {
        'second': 1,
        'minute': 60,
        'hour': 3600,
        'day': 21600
    }

    try:
        delta = new_time - old_time
    except TypeError:
        # strptime不能采用关键字调用的方式，否则会报错
        delta = sub(*[datetime.strptime(t, time_format) for t in (new_time, old_time)])

    if absolute:
        delta = abs(delta)

    return delta.seconds / time_unit[unit]


def write_to_csv(csv_name, rows, header=None):
    """
    将数据写入csv文件

    :param csv_name: 文件名
    :param rows: 待写入的数据
    :param header: 表头
    :return: None
    """
    csv_name = Path(csv_name).with_suffix('.csv')

    with open(csv_name, 'wt', encoding='utf8', newline='') as f:
        writer = csv.writer(f)
        if header is not None:
            writer.writerow(header)
        for row in rows:
            if isinstance(row, Mapping):
                writer.writerow(row.values())
            else:
                writer.writerow(row)


def write_to_xlsx(xlsx_name, rows, header=None):
    """
    将数据写入xslx文件

    :param xlsx_name: 文件名
    :param rows: 待写入的数据
    :param header: 表头
    :return: None
    """
    wb = Workbook()
    ws = wb.active
    if header is not None:
        ws.append(header)
    for row in rows:
        if isinstance(row, Mapping):
            ws.append(row.values())
        else:
            ws.append(row)

    xlsx_name = Path(xlsx_name).with_suffix('xlsx')
    wb.save(filename=xlsx_name)


def select_column(rows, col, flatten=True):
    """
    选择二纬结构中的指定列

    :param rows: 二维数组结构
    :param col: 列名或者索引号
    :param flatten: 是否扁平化
    :return: 单列元素构成的数组或者单列构成的二维数组
    """
    if flatten:
        return [row[col] for row in rows]
    else:
        return [[row[col]] for row in rows]
