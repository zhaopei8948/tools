import os
import traceback
import time
from datetime import datetime, timedelta

from apscheduler.schedulers.background import BackgroundScheduler
from loguru import logger

username = os.getenv('ORCL_USERNAME') or 'username'
password = os.getenv('ORCL_PASSWORD') or 'password'
dbUrl = os.getenv('ORCL_DBURL') or '127.0.0.1:1521/orcl'
minutes = os.getenv('SCHED_MINUTES') or 0
seconds = os.getenv('SCHED_SECONDS') or 3
logger.add("logs/cusstatus/modify_invt_cusstatus.log", rotation="1 day", level="INFO")


def executeSql(sql, fetch=True, **kw):
    logger.info('execute sql is: %s' % sql)
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
    and t.app_status = '800' and t1.status = '26'
    and (t.cus_status is null or t.cus_status != '26')
    '''
    now = datetime.now()
    startTime = now + timedelta(days=-2)
    endTime = now + timedelta(minutes=-45)
    logger.info('now is: %s,  startTime is: %s, endTime is: %s' % (now, startTime, endTime))
    result = executeSql(sql, startTime=startTime.strftime('%Y-%m-%d'), endTime=endTime.strftime('%Y-%m-%d %H:%M'))
    logger.info('result count is: %s' % len(result))

    for invtInfo in result:
        updateSql = "update ceb2_invt_head t set t.cus_status = '26', t.cus_time = sysdate "
        if abs(int(invtInfo[3]) - int(invtInfo[4])) >= 24 * 60 * 60:
            continue

        updateSql += " where t.head_guid = '" + invtInfo[2] + "'"
        executeSql(updateSql, False)



if __name__ == '__main__':
    scheduler = BackgroundScheduler()
    scheduler.add_job(selectInvt, 'interval', minutes=int(minutes), seconds=int(seconds))
    scheduler.start()
    logger.info('Press Ctrl+C to exit')

    try:
        while True:
            time.sleep(10)

    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        print('Exit The job!')
        logger.info('Exit The job!')
