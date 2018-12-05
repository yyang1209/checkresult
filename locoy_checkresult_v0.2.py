# -*- coding: utf-8 -*-
"""
@author: MonsterHe
@contact: yuntian.hee@gmail.com
@version: python3.6
@file: open_sql.py
@time: 2018/11/3 10:26
@tools: Pycharm 2018.1
"""

"""
当前检查的功能：（*标记的为未实现）
1.非内容字段包含HTML标签  
2.内容标签截断
3.发布时间格式错误
4.标签采集结果为空
6.非标讯检查
11.内容中包含input标签，未把标签内的值替换出来（在后续流程中标签会被去掉，这样会导致未提取的值也会被去掉造成内容不全
*5.采集非本站数据
*7.数据处理（空格替换为空）
*8.cookie不是用脚本获取的
*9.任务备注里面包含 ""或''
*10.内容是json格式或源页面json解析异常而未处理
"""

import sqlite3
import re

# 连接数据库
def connect_db(db_path): 
    return sqlite3.connect(db_path)

# 检查规则
def check_rule(db_path):
    siteid = int(input("Please input siteid: "))

    # 校验是否为当前检查的组
    sqlcheck = '''SELECT Job.jobId, Job.JobName, Site.SiteId, Site.SiteName FROM Job, Site
    WHERE Job.SiteId = Site.SiteId
    AND Job.SiteId = %d
    LIMIT 1'''%siteid



    # 检查发布时间为系统时间是否标记-A
    sqldatecheck = """
    SELECT jobid,jobname FROM "Job" t
    where t.siteid  in (%s)
    and (instr(t.xmldata,'ManualTimeStr="yyyy-MM-dd"')
    and JobName NOT LIKE '%%%%-A%%%%'
    )
    """%siteid

    # 检查标签是否完整
    sqllabelcheck = '''SELECT j.jobid, j.jobname, s.siteid, s.sitename FROM "Job" j, "Site" s
    WHERE j.SiteId = S.SiteId
    and j.siteid  in (%s)
    AND (instr(j.XmlData, '<Rule LabelName="标题"')=0
    OR instr(j.XmlData, '<Rule LabelName="发布时间"')=0
    OR instr(j.XmlData, '<Rule LabelName="采购编号"')=0
    OR instr(j.XmlData, '<Rule LabelName="地区"')=0
    OR instr(j.XmlData, '<Rule LabelName="来源"')=0
    OR instr(j.XmlData, '<Rule LabelName="数据类型"')=0
    OR instr(j.XmlData, '<Rule LabelName="公告类型"')=0
    OR instr(j.XmlData, '<Rule LabelName="内容"')=0)
    '''%siteid

    sql2 = """
        SELECT jobid,jobname FROM "Job" t
        where t.siteid  in (%s)
        and instr(xmldata,'Rule LabelName="公告类型" GetDataType="0" StartStr="" EndStr="" RegexContent="" RegexCombine="" XpathContent="" XPathAttribue="0" TextRecognitionType="0" TextRecognitionCodeReturnType="1" LengthFiltOpertar="0" LengthFiltValue="0" LabelContentMust="" LabelContentForbid="" FileUrlMust="" FileSaveDir="" FileSaveFormat="" ManualType="0" ManualString="type01') =0
        and instr(xmldata,'Rule LabelName="公告类型" GetDataType="0" StartStr="" EndStr="" RegexContent="" RegexCombine="" XpathContent="" XPathAttribue="0" TextRecognitionType="0" TextRecognitionCodeReturnType="1" LengthFiltOpertar="0" LengthFiltValue="0" LabelContentMust="" LabelContentForbid="" FileUrlMust="" FileSaveDir="" FileSaveFormat="" ManualType="0" ManualString="type02') =0
        """ % siteid
    sql3 = """
        SELECT jobid,jobname FROM "Job" t
        where t.siteid  in (%s)
        and instr(XmlData,'MaxSpiderPerNum="0" MaxOutPerNum="0"') = 0
        """ % siteid
    sql4 = """
        SELECT jobid,jobname FROM "Job" t
        where t.siteid  in (%s)
        and instr(t.XmlData,'CheckUrlRepeat=""') = 0
        """ % siteid
    sql5 = """
        SELECT jobid,jobname FROM "Job" t
        where t.siteid  in (%s)
        and(instr(t.XmlData,'new="201') > 0
        or instr(t.XmlData,'<FillBothEnd Start="201') > 0)
        """ % siteid
    sql6 = """
        SELECT jobid,jobname FROM "Job" t
        where t.siteid  in (%s)
        and( instr(xmldata,'UrlRepeat=""') =0
        or instr(xmldata,'CheckUrlRepeat=""') =0)
        """ % siteid

    # 连接数据库并获得cursor
    conn = connect_db(db_path)
    cursor = conn.cursor()
    try:
        rules = cursor.execute(sqlcheck)
        for rule_info in rules:    
            print("校验信息：\n ",rule_info,"\n----------------")

        labelcheck = cursor.execute(sqllabelcheck)
        for item1 in labelcheck:
            print(item1, '缺少标签')
        sdatacheck = cursor.execute(sqldatecheck)
        for item2 in sdatacheck:
            print(item2, '系统时间未标记-A')
        row2 = cursor.execute(sql2)
        for x2 in row2:
            print(x2, '公告类型写死')
        row3 = cursor.execute(sql3)
        for x3 in row3:
            print(x3, '误设最大页数')
        row4 = cursor.execute(sql4)
        for x4 in row4:
            print(x4, '检查重复没有勾上')
        row5 = cursor.execute(sql5)
        for x5 in row5:
            print(x5, '规则里面有前缀后缀')
        row6 = cursor.execute(sql6)
        for x6 in row6:
            print(x6, '检查网址重复未选上')
    except NotImplementedError as error:
        print(str(error))
    finally:
        cursor.close()
        conn.close()

# 检查内容
def check_content(job_path):
    # 检查非内容字段有无截断HTML标签
    sql1 = '''SELECT ID, 标题, 发布时间 FROM content
    WHERE 已采 = 1
    AND (标题 LIKE '%<%'
    OR 标题 LIKE '%>%'
    OR 发布时间 LIKE '%<%'
    OR 发布时间 LIKE '%>%')
    '''
    
    # 检查有空内容的字段
    sql2 = '''SELECT ID, 标题, 内容, 发布时间 FROM Content WHERE 已采=1
    AND (标题 IS NULL
    OR 内容 IS NULL
    OR 发布时间 IS NULL
    OR 内容=''
    OR 发布时间=''
    OR 标题='')
    '''

    # 检查内容标签中包含有input标签
    sql3 = '''SELECT ID, 标题 FROM Content WHERE 已采=1
    AND 内容 LIKE '%<input%'
     '''
    # 检查内容中有截断HTML标签
    sql4 = '''SELECT ID, 标题, 内容 FROM Content WHERE 已采=1
    '''
    # 检查采集结果是否不为标讯
    sql5 = '''SELECT ID, 标题, 内容 FROM Content WHERE 已采=1
    AND 标题 IS NOT NULL
    AND 标题 != ''
    AND 内容 IS NOT NULL
    AND 内容 != ''
    '''


    conn = connect_db(job_path)
    cursor = conn.cursor()
    try:
        print('---------开始检查非内容字段是否有HTML标签--------')
        contents1 = cursor.execute(sql1)
        for job1 in contents1:
            print('{}\t {}\t标题或时间有HTML标签'.format(job1[0], job1[1]))

        print('\n---------开始检查内容、标题、发布时间是否有空--------')
        contents2 = cursor.execute(sql2)
        for job2 in contents2:
            print('{}\t {}\t内容或标题或发布时间有空值'.format(job2[0], job2[1]))

        print('\n---------开始检查内容字段是否有input标签--------')
        contents3 = cursor.execute(sql3)
        for job3 in contents3:
            print('{}\t {}\t内容有input标签没有过滤'.format(job3[0], job3[1]))

        print('\n---------开始检查内容字段是否有截断标签（开头处）--------')
        # 检查内容是否有截断HTML标签（目前只能检查开头）
        contents4 = cursor.execute(sql4)
        for job4 in contents4:
            content = job4[2]
            if isinstance(content, str):
                result = re.match(r"^.*?>", content)
                if result != None:
                    if result.group(0).count('<') != result.group(0).count('>'):
                        print("{}\t {}\t内容可能有截断标签".format(job4[0], job4[1]))
                    else:pass
                else:pass
            else:pass

        print('\n---------开始检查采集内容是否不为标讯（仅做参考）--------')
        contents5 = cursor.execute(sql5)
        for job5 in contents5:
            title = job5[1]
            content = job5[2]
            judge_value = check_title(title)
            # 标题若无法判断是否为标讯，则进入内容判断
            if judge_value != 1:
                judge_value = check_bidding(content)
                if judge_value != 1:
                    print('{}\t {}\t {tip}'.format(job5[0], job5[1], tip='采集内容不是标讯(经作参考)'))
            else:pass
    except sqlite3.OperationalError as e:
        pass
    except NotImplementedError as e:
        print(str(e))
    finally:
        cursor.close()
        conn.close()

# 检查非标讯
def check_title(s_title):
    targets = ['中标','招标','议标','废标','流标','邀标','询价','谈判','采购',
              '比选','磋商','成交','竞价','竞投','中选','邀请招标','投标',
              '竞谈','单一来源','竞争性','竞选','评标','项目结果公示','竞标',
              '摇珠','开标','招商','确标','询比价','补遗','中选','发包','补遗',
              '答疑','项目限价','竞标','竞选','议价','竟标','招租','询标','遴选','标段','工程'
               ]

    not_targets = ['条例','规定','工作','规范','标准','荣获','招聘','试行','行动']

    for target in targets:
        if target in s_title:
            return 1
        else:pass
    return -1

# 检查标讯是否被过滤
def check_bidding(s_content):
    targets = ['招标条件', '项目名称', '招标范围', '投标人资格', '投标人', '招标文件', '投标',
               '招标人', '标段名称', '中标人', '评标结果', '投标', '采购人', '单一来源编号', '招标编号',
               '采购组织', '标项名称', '采购项目', '采购方式', '成交结果', '原采购公告', '采购公告',
               '采购内容', '合格供应商', '采购文件', '保证金', '开标时间', '询价项目编号', '询价供应商',
               '标项名称', ]
    for target in targets:
        if target in s_content:
            return 1
        else:pass
    return None


def check_db():
    rule_path = r'''D:\7个火车头\标讯采集\Configuration\config.db3'''
    i = 1
    while i==1:
        check = int(input("退出请输入0，检查规则请输入1，检查内容请输入2："))
        if check == 1:
            check_rule(rule_path)
            print('\n-----本次规则检查结束-----\n')
            i = int(input('返回请输入1，退出请输入2：'))
        elif check == 2:
            mark = int(input('执行单任务检查请输入1，执行多任务检查请输入0：'))
            if mark == 1:
                while mark==1:
                    jobid = input("请输入任务ID：")
                    job_path = ("D:\练习Hyt\火车采集器V8 2\Data\%s\SpiderResult.db3" % jobid)
                    check_content(job_path)
                    mark = int(input("退出输入0， 继续输入1，返回上一步输入2："))
                    if mark == 0:
                        return print('检查结束')
                    elif mark == 1:
                        pass
                    else:
                        i = 1
            else:
                mark = 1
                while mark==1:
                    startid = int(input('请输入任务开始的ID：'))
                    endid = int(input('请输入任务结束的ID：'))
                    print('---------开始进行批量检查---------\n')
                    count = endid - startid
                    for id in range(count+1):
                        jobid = str(startid + id)
                        print('\n*********开始检查ID为：%s的任务:*********\n' % jobid)
                        job_path = "D:\练习Hyt\火车采集器V8 2\Data\%s\SpiderResult.db3" % jobid
                        check_content(job_path)
                    mark = int(input("退出输入0， 继续输入1，返回上一步输入2："))
                    if mark == 0:
                        return print('检查结束')
                    elif mark == 1:
                        pass
                    else:
                        i = 1
        else:
            return print('\n-----结束检查------\n')




def main():
    check_db()

if __name__ == '__main__':
    main()



    

        





