#!/usr/bin/python
# -*- coding:utf-8 -*-
import sys
import paramiko
import datetime
import json
import os
from prpcrypt import prpcrypt
import threading
import cx_Oracle

"""
@purpose: backup remote host file and database
@author : chenli
@date   : 2020-05-08  version:v0.10  new create
          2020-05-11  version:v0.11  1. .tar -> .tar.gz  2. 修复日期错误 3.修复命令执行超时卡住不动问题
          2020-05-12  version:v0.12  增加config.json密码配置为AES加密后的串功能
          2020-05-12  version:v0.13  1.增加多线程支持 2.修改打印日志可读性 3.修改备份文件目录组织方式 程序名/日期
          2020-05-14  version:v0.14  1.重要oracle数据库表数据备份(exp-> dmp) 2.增加数据库过程等对象转储成文本文件
@caution: 1. 在config.json中请保持“programname” 唯一，否则不同程序打包后造成冲突
"""

# 定义当前时间格式
detailtime = datetime.datetime.now().strftime("%Y%m%d")
thread_max_num = threading.Semaphore(5)

def progress_bar(transferred, toBeTransferred, suffix='',prograName=''):
    bar_len = 100
    filled_len = int(round(bar_len * transferred / float(toBeTransferred)))
    percents = round(100.0 * transferred / float(toBeTransferred), 1)
    bar = '\033[32;1m%s\033[0m' % '>' * filled_len + '-' * (bar_len - filled_len)
    sys.stdout.write('[%s] downloading [%s] %s%s %s\r' % (prograName,bar, '\033[32;1m%s\033[0m' % percents, '%', suffix))
    sys.stdout.flush()


""""读取配置"""
def read_config():
    with open("../config/config.json") as json_file:
        config = json.load(json_file)
    return config

def checkConfig(config):
    programName = []
    for object in config["source_dir"]:
        if object["programname"] not in programName:
            programName.append(object["programname"])
        else:
            print("参数programname=[" + str(object["programname"]) + "]存在重复！请保持唯一！")
            exit()

"""去除字符串末尾特定字符"""
def trip(text,sstr):
    new_text = ''
    if text[-1] == sstr:
        new_text = text[0:-1]
    else:
        new_text = text
    return new_text

"""
执行前检查
"""
def runCheck(args):
    if len(args) < 3:
        print("请输入16位解密密钥!和备份类型(1:HOST 2:ORACLE_TAB 3:ORACLE_PROC 4:ALL)")
        exit()
    if len(args[1]) <16:
        print("密钥输入不足16位！")
        exit()
    if str(args[2]) not in ("1","2","3","4"):
        print("备份类型为[1,2,3,4](其中 1:HOST 2:ORACLE_TAB 3:ORACLE_PROC 4:ALL)")
        exit()
"""
备份主机上的文件(目录)：压缩/传输文件操作
"""
def backup_hostdir(host,pc):
    with thread_max_num:
        print("开始登陆远程主机[%s]……" %str(host["host"]))
        #登陆远程主机
        try:
            paramiko.util.log_to_file("../log/paramiko.log")
            ssh_client = paramiko.SSHClient()
            ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh_client.connect(hostname=str(host["host"]),port=int(host["port"]),username=str(host["username"]), password=str(pc.decrypt(host["password"])))
            print("成功登陆远程主机[%s]." %str(host["host"]))
        except:
            print("[ERROR]登陆远程主机[%s]失败退出！[ERROR]" % str(host["host"]))
            exit()
        #要压缩的目录
        dir_paths = ""
        for dir in host["backupdir"]:
            dir_paths += " "+ trip(dir,'/')

        #排除的目录或者文件
        exclude_paths = ""
        for dir in host["exclude_dirs"]:
            exclude_paths += " --exclude="+ trip(dir,'/')

        #封装tar打包压缩命令
        tar_filename = str(host["programname"])+"_"+detailtime
        #print("tar_filename=%s" %tar_filename )
        tar_commands = ('cd {0}'.format(str(host["program_dir"])),'tar  {0} -zcf  {1}.tar.gz {2}'.format(exclude_paths,tar_filename, dir_paths))

        try:
            print("程序[{0}]开始压缩，对应文件目录[{1}]……".format(str(host["programname"]),dir_paths))
            stdin, stdout, stderr = ssh_client.exec_command(' \n '.join(tar_commands),timeout=20*60,bufsize=-1)
            print('{0} Compression started'.format(str(host["programname"])))
            stdout.read()
            stdout.channel.recv_exit_status()  # Wait for tar to complete

            print('{0} Compression is done. '.format(str(host["programname"])))
        except:
            print("[ERROR]程序[{0}]压缩失败退出!!对应文件目录[{1}]！[ERROR]".format(str(host["programname"]),dir_paths))
            exit()

        #创建本地目录
        local_backup_dir = trip(config["local_dir"],'/') +"/" + host["programname"]+"/"+ detailtime
        #print("local_backup_dir="+local_backup_dir)
        if not os.path.exists(local_backup_dir):
            os.makedirs(local_backup_dir)
        os.chdir(local_backup_dir)

        # 下载到本地
        print("程序[{0}]备份文件开始下载！备份文件存放地址[{1}]".format(str(host["programname"]), local_backup_dir + "/" + tar_filename + ".tar.gz"))
        ftp_client = ssh_client.open_sftp()
        ftp_client.get('{0}/{1}.tar.gz'.format(str(host["program_dir"]), tar_filename), r'{0}/{1}.tar.gz'.format(local_backup_dir,tar_filename),callback=progress_bar)
        ftp_client.close()

        #移除远程主机压缩文件
        remove_commands = ('cd {0}'.format(str(host["program_dir"])),"rm {0}.tar.gz".format(tar_filename))
        stdin, stdout, stderr = ssh_client.exec_command(' \n '.join(remove_commands),timeout=20*60,bufsize=-1)
        stdout.read()

        ssh_client.close()

        print("程序[{0}]备份文件下载成功！备份文件存放地址[{1}]".format(str(host["programname"]),local_backup_dir+"/"+tar_filename+".tar.gz"))

"""
exp备份oracle数据库表对象,生成dmp文件
"""
def backup_oracle_TAB(oracle,pc):
    with thread_max_num:
        # 创建本地目录
        local_backup_dir = trip(config["local_dir"], '/') + "/" + host["programname"] + "/" + detailtime

        if not os.path.exists(local_backup_dir):
            os.makedirs(local_backup_dir)
        os.chdir(local_backup_dir)

        bak_command = 'exp ' + oracle["username"] + '/' + str(pc.decrypt(oracle["password"]))+ '@' + oracle["tnsname"] \
                      + ' buffer=409600'\
                      + ' file=' + local_backup_dir + '/' +oracle["programname"] + '_' + detailtime + '.dmp'\
                      + ' tables='+str(oracle["backupObject"]).upper()
        print('bak_command=['+bak_command+"]")
        print('ORACLE数据库[%s]开始备份……' % str(oracle["programname"]))
        if os.system(bak_command) == 0:
            print('ORACLE数据库[%s]备份成功!' %str(oracle["programname"]))
        else:
            print('ORACLE数据库[%s]备份失败!' % str(oracle["programname"]))
            exit()

"""
cx_Oracle方式备份oracle数据库['PROCEDURE', 'FUNCTION', 'PACKAGE' , 'VIEW']对象，导出为文本文件
"""
def backup_oracle_PROC(oracle,pc):
    # 创建本地目录
    local_backup_dir = trip(config["local_dir"], '/') + "/" + host["programname"] + "/" + detailtime

    connectStr = oracle["username"] + '/' + str(pc.decrypt(oracle["password"]))+ '@' + oracle["host"] + ":" + oracle["port"] + "/" + oracle["dbsid"]
    db = cx_Oracle.connect(connectStr)
    cr = db.cursor()  # 创建cursor
    sql = oracle["backupObject"]

    cr.execute(sql)  # 执行sql 语句

    rs = cr.fetchall()  # 一次返回所有结果集
    for row in rs:
        #按对象类型分目录存放
        procTypeDir = local_backup_dir+"/"+row[1]
        if not os.path.exists(procTypeDir):
            os.makedirs(procTypeDir)
        os.chdir(procTypeDir)

        cr1 = db.cursor()
        spoolSql="select DBMS_METADATA.GET_DDL(OBJECT_TYPE, OBJECT_NAME,owner) DDL_statement FROM ALL_OBJECTS WHERE object_id=:object_id"
        pr = {'object_id': row[0]}
        cr1.execute(spoolSql,pr)
        # 命名格式：owner.object_name.txt
        fo = open(row[3] + "." + row[2] + ".txt", "w")
        for ddlrow in cr1:
            fo.write(str(ddlrow[0]))
        fo.close()
        cr1.close()

    cr.close()
    db.close()

if __name__ == '__main__':
    args = sys.argv
    runCheck(args)
    bakType = str(args[2])
    pc = prpcrypt(args[1])
    config = read_config()
    checkConfig(config)
    source_dir = config["source_dir"]
    for host in source_dir:
        if host["type"] == "HOST" and bakType in ("1","4"):
            t_host = threading.Thread(target=backup_hostdir, args=(host,pc))
            t_host.start()
        elif host["type"] == "ORACLE_TAB" and bakType in ("2","4"):
            t_oracle = threading.Thread(target=backup_oracle_TAB, args=(host, pc))
            t_oracle.start()
        elif host["type"] == "ORACLE_PROC" and bakType in ("3", "4"):
            backup_oracle_PROC(host, pc)
        else:
            pass
