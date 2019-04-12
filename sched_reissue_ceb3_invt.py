import os
import traceback
import time
import cx_Oracle
import redis
import shutil
from datetime import datetime, timedelta

from apscheduler.schedulers.background import BackgroundScheduler
from loguru import logger

username = os.getenv('ORCL_USERNAME') or 'username'
password = os.getenv('ORCL_PASSWORD') or 'password'
dbUrl = os.getenv('ORCL_DBURL') or '127.0.0.1:1521/orcl'
redisHost = os.getenv('REDIS_HOST') or '127.0.0.1'
redisPort = os.getenv('REDIS_PORT') or 6379
reissuePath = os.getenv('REISSUE_PATH') or ''
tmpPath = os.getenv('TMP_PATH') or ''
minutes = os.getenv('SCHED_MINUTES') or 0
seconds = os.getenv('SCHED_SECONDS') or 3
logger.add("logs/reissue/ceb3.log", rotation="1 day", level="INFO")


pool = redis.ConnectionPool(host=redisHost, port=redisPort)
r = redis.Redis(connection_pool=pool)


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
    select t.invt_no
    from ceb3_invt_head t
    left outer join check_mail_good_head t1 on t1.logistics_no = t.logistics_no
    where t.sys_date > to_date(:startTime, 'yyyy-MM-dd')
    and t.sys_date <= to_date(:endTime, 'yyyy-MM-dd hh24:mi')
    and t.app_status = '800'
    and (t.cus_status is null or t.cus_status != '26' or t1.status is null or t1.status != '26')
    and rownum < 1000
    '''
    now = datetime.now()
    startTime = now + timedelta(days=-2)
    endTime = now + timedelta(minutes=-50)
    logger.info('now is: %s,  startTime is: %s, endTime is: %s' % (now, startTime, endTime))
    result = executeSql(sql, startTime=startTime.strftime('%Y-%m-%d'), endTime=endTime.strftime('%Y-%m-%d %H:%M'))
    if result is None:
        return
    logger.info('result count is: %s' % len(result))
    fileName = '{}_BuFaZzck.ceb3'.format(now.strftime("%Y%m%d%H%M%S%f"))
    tmpFile = os.path.join(tmpPath, fileName)
    dstFile = os.path.join(reissuePath, fileName)
    isCopy = False
    with open(tmpFile, 'w') as f:
        for tin in result:
            invtNo = tin[0]
            if r.get(invtNo) is None:
                isCopy = True
                f.write(invtNo + "\n")
                r.set(invtNo, '1')
            elif int(r.get(invtNo)) < 2:
                isCopy = True
                f.write(invtNo + "\n")
                r.set(invtNo, '2')
            else:
                logger.error("{} more than {} reissues have been issued, no more reissues."
                             .format(invtNo, 2))

    if isCopy:
        shutil.copy(tmpFile, dstFile)
        logger.info("{} copy to {} success!".format(os.path.abspath(tmpFile), os.path.abspath(dstFile)))
    else:
        logger.error("no copy is required!")


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
