from flask import Flask, request, jsonify
from flask_cors import CORS
import pymysql
import logging

app = Flask(__name__)
CORS(app)

# 设置日志格式
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

# Doris MySQL 连接配置（请根据实际情况修改）
DORIS_CONFIG = {
    'host': '127.0.0.1',
    'port': 9030,
    'user': 'root',
    'password': '',
    'database': 'doris_udf_test',
    'charset': 'utf8mb4'
}

def query_doris(sql):
    logging.info(f"请求 Doris，SQL: {sql}")
    conn = pymysql.connect(**DORIS_CONFIG)
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            result = []
            for row in rows:
                processed_row = {}
                for i, value in enumerate(row):
                    # 如果值是数字且超过 15 位，转为字符串
                    if isinstance(value, (int, float)) and len(str(value)) > 15:
                        processed_row[columns[i]] = str(value)
                    else:
                        processed_row[columns[i]] = value
                result.append(processed_row)
            # 返回结果时包含列顺序信息
            return {
                'data': result,
                'column_order': columns  # 添加列顺序信息
            }
    finally:
        conn.close()

@app.route('/query', methods=['POST'])
def query():
    data = request.get_json()
    sql1 = data.get('sql1')
    sql2 = data.get('sql2')
    logging.info(f"收到 /query 请求，sql1: {sql1}, sql2: {sql2}")
    try:
        result1 = query_doris(sql1) if sql1 else {'data': [], 'column_order': []}
        result2 = query_doris(sql2) if sql2 else {'data': [], 'column_order': []}
        return jsonify({
            'result1': result1,
            'result2': result2
        })
    except Exception as e:
        logging.error(f"查询出错: {e}")
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5012, debug=True) 