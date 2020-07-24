版本发布说明：
"""
@purpose: backup remote host file
@author : chenli
@date   : 2020-05-08  version:v0.10  new create
          2020-05-11  version:v0.11  1. .tar -> .tar.gz  2. 修复日期错误 3.修复命令执行超时卡住不动问题(参见paramiko安装手册.docx中最后部分)
          2020-05-12  version:v0.12  增加config.json密码配置为AES加密后的串功能
          2020-05-12  version:v0.13  1.增加多线程支持 2.修改打印日志可读性
          2020-05-14  version:v0.14  1.增加重要oracle数据库表数据备份(exp-> dmp) 2.增加数据库过程等对象转储成文本文件
@caution: 1. 在config.json中请保持“programname” 唯一，否则不同程序打包后造成冲突
"""