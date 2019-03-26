from datetime import datetime, timedelta
import time, os, cx_Oracle, traceback, logging
from logging import handlers
from apscheduler.schedulers.background import BackgroundScheduler

class Logger(object):
    level_relations = {
        'debug':logging.DEBUG,
        'info':logging.INFO,
        'warning':logging.WARNING,
        'error':logging.ERROR,
        'crit':logging.CRITICAL
    }#日志级别关系映射

    def __init__(self,filename,level='info',when='D',backCount=10,fmt='%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s'):
        self.logger = logging.getLogger(filename)
        format_str = logging.Formatter(fmt)#设置日志格式
        self.logger.setLevel(self.level_relations.get(level))#设置日志级别
        sh = logging.StreamHandler()#往屏幕上输出
        sh.setFormatter(format_str) #设置屏幕上显示的格式
        th = handlers.TimedRotatingFileHandler(filename=filename,when=when,backupCount=backCount,encoding='utf-8')#往文件里写入#指定间隔时间自动生成文件的处理器
        #实例化TimedRotatingFileHandler
        #interval是时间间隔，backupCount是备份文件的个数，如果超过这个个数，就会自动删除，when是间隔的时间单位，单位有以下几种：
        # S 秒
        # M 分
        # H 小时、
        # D 天、
        # W 每星期（interval==0时代表星期一）
        # midnight 每天凌晨
        th.setFormatter(format_str)#设置文件里写入的格式
        self.logger.addHandler(sh) #把对象加到logger里
        self.logger.addHandler(th)


username = os.getenv('ORCL_USERNAME') or 'username'
password = os.getenv('ORCL_PASSWORD') or 'password'
dbUrl = os.getenv('ORCL_DBURL') or '127.0.0.1:1521/orcl'
minutes = os.getenv('SCHED_MINUTES') or 0
seconds = os.getenv('SCHED_SECONDS') or 3
# log = logging.getLogger("sched_modify_invt")
# log.setLevel(logging.INFO)
# formatStr = logging.Formatter("%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s")
# sh = logging.StreamHandler()
# sh.setFormatter(formatStr)
# th = handlers.TimedRotatingFileHandler(filename="modify_invt.log",when='D',backupCount=10,encoding='utf-8')
# th.setFormatter(formatStr)
# log.addHandler(sh)
# log.addHandler(th)
log = Logger('logs/appstatus/modify_invt.log')


def executeSql(sql, fetch=True, **kw):
    log.logger.info('execute sql is: %s' % sql)
    con = cx_Oracle.connect(username, password, dbUrl)
    cursor = con.cursor()
    result = None
    try:
        cursor.prepare(sql)
        cursor.execute(None, kw)
        if fetch:
            result = cursor.fetchall()
        else:
            con.commit()
    except Exception as e:
        traceback.print_exc()
        con.rollback()
    finally:
        cursor.close()
        con.close()
    return result


def selectInvt():
    sql = '''
    select t.invt_no, t1.entry_id, t.head_guid, to_char(t1.message_time, 'yyyyMMddhh24miss'), to_char(t.sys_date, 'yyyyMMddhh24miss')
    from ceb2_invt_head t
    left outer join check_mail_good_head t1 on t1.logistics_no = t.logistics_no
    where t.sys_date > to_date(:startTime, 'yyyy-MM-dd')
    and t.sys_date <= to_date(:endTime, 'yyyy-MM-dd hh24:mi')
    and t.app_status != '800' and t.cus_status = '26'
    '''
    now = datetime.now()
    startTime = now + timedelta(days=-2)
    endTime = now + timedelta(minutes=-30)
    log.logger.info('now is: %s,  startTime is: %s, endTime is: %s' % (now, startTime, endTime))
    result = executeSql(sql, startTime=startTime.strftime('%Y-%m-%d'), endTime=endTime.strftime('%Y-%m-%d %H:%M'))
    log.logger.info('result count is: %s' % len(result))

    for invtInfo in result:
        updateSql = "update ceb2_invt_head t set t.app_status = '800' "
        if invtInfo[0] is None:
            if invtInfo[1] is None:
                continue
            else:
                if abs(int(invtInfo[3]) - int(invtInfo[4])) < 24 * 60 * 60:
                    updateSql += ", t.invt_no = '" + invtInfo[1] + "' "

        updateSql += " where t.head_guid = '" + invtInfo[2] + "'"
        executeSql(updateSql, False)



if __name__ == '__main__':
    scheduler = BackgroundScheduler()
    scheduler.add_job(selectInvt, 'interval', minutes=int(minutes), seconds=int(seconds))
    scheduler.start()
    log.logger.info('Press Ctrl+C to exit')

    try:
        while True:
            time.sleep(10)

    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        print('Exit The job!')
        log.logger.info('Exit The job!')
