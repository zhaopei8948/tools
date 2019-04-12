import cx_Oracle
import os
import traceback
from flask import Flask, request
from flask_json import FlaskJSON, as_json
from loguru import logger


app = Flask(__name__)
FlaskJSON(app)


username = os.getenv('ORCL_USERNAME') or 'username'
password = os.getenv('ORCL_PASSWORD') or 'password'
dbUrl = os.getenv('ORCL_DBURL') or '127.0.0.1:1521/orcl'
logger.add("logs/select/oracle.log", rotation="1 day", level="INFO")


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
        logger.error(traceback.format_exc())
        con.rollback()
    finally:
        cursor.close()
        con.close()
    return result


@app.route('/select')
@as_json
def select_data():
    sql = None
    if request.method == 'GET':
        sql = request.args.get('sql')
    else:
        sql = request.form.get('sql')

    if sql is None:
        logger.info("sql is None, no execute.")
    logger.info("execute sql is {}".format(sql))
    result = executeSql(sql)
    return result if result is not None else []
