# -*- coding: utf-8 -*-

"""
    作者:       Liu Yuzhang
    版本:       1.1
    日期:       2019/05/24
    项目名称：   HYUNDAI_Score_Calculation
    python环境： 3.5
"""

import time
import xml.etree.ElementTree as ET
import xml.dom.minidom as minidom
import pandas as pd
start = time.clock()  # 记录处理开始时间；与最后一行一起使用，来判断输出运行时间。

def read_xml(in_path):
    """读取并解析xml文件
       in_path: xml路径
       return: tree"""
    tree = ET.parse(in_path)
    return tree

def creat_dict(root):
    """xml生成为dict：，
    将tree中个节点添加到list中，将list转换为字典dict_init
    叠加生成多层字典dict_new"""
    dict_new = {}
    for key, valu in enumerate(root):
        dict_init = {}
        list_init = []
        for item in valu:
            list_init.append([item.tag, item.text])
            for lists in list_init:
                dict_init[lists[0]] = lists[1]
        dict_new[key] = dict_init
    return dict_new

def dict_to_xml(input_dict, root_tag, node_tag):
    """ 定义根节点root_tag，定义第二层节点node_tag
    第三层中将字典中键值对对应参数名和值
       return: xml的tree结构 """
    root_name = ET.Element(root_tag)
    for (k, v) in input_dict.items():
        node_name = ET.SubElement(root_name, node_tag)
        for key, val in v.items():
            key = ET.SubElement(node_name, key)
            key.text = val
    return root_name

# def trans_dict_to_xml(data_dict):
#     #字典转换为xml字符串
#     data_xml = []
#     for k in data_dict.keys():  # 遍历字典排序后的key
#         v = data_dict.get(k)  # 取出字典中key对应的value
#         data_xml.append('<{key}>{value}</{key}>'.format(key=k, value=v))
#     xml = ''.join(data_xml)
#     xml1 = '<xml>{}</xml>'.format(xml)
#     return xml1

def out_xml(root, out_file):
    """格式化root转换为xml文件"""
    rough_string = ET.tostring(root, 'utf-8')
    reared_content = minidom.parseString(rough_string)
    with open(out_file, 'w+') as fs:
        reared_content.writexml(fs, addindent=" ", newl="\n", encoding="utf-8")
    return True

def df_to_dict(df):
    # data = pd.read_csv(data_path, header=[0])
    data = df
    data = data.T
    data = data.applymap(str)
    dict = data.to_dict()
    return dict

def fn(_dict, depth):
    '''遍历多重dict

    :param _dict:
    :param depth:
    :return:
    '''
    for k, v in _dict.items():
        if depth == 1:
            yield k, v
        else:
            yield from ((k, *q) for q in fn(v, depth - 1))

# if __name__ == '__main__':
#     # in_files = r"baspool_read.xml"
#     out_file = r"ubi_grade.xml"
#     # tree = read_xml(in_files)
#     # node_new = creat_dict(tree.getroot())  # 将xml转换为dict
#     dict = df_to_dict('ubi_grade.csv')
#     print(dict)
#     for k, v, x in fn(dict, 2):
#         print(k, v, x)

#     root = dict_to_xml(dict, "root", "item")  # 将dict转换为xml
#     # root = trans_dict_to_xml(dict)
#     print(root)
#     out_xml(root, out_file)
# end = time.clock()
# print("read: %f s" % (end - start))