import pandas as pd
import requests
import json
from datetime import datetime
import pymysql
import os
import glob
import chardet

#Doris 配置
DORIS_HOST = '127.0.0.1'
# DORIS_HOST = '10.0.90.90'

def check_and_create_database(database_name, mysql_host=DORIS_HOST, mysql_port=9030, user='root', password=''):
    """检查数据库是否存在，如果不存在则创建"""
    conn = pymysql.connect(host=mysql_host, port=mysql_port, user=user, password=password)
    try:
        with conn.cursor() as cursor:
            # 检查数据库是否存在
            cursor.execute(f"SHOW DATABASES LIKE '{database_name}'")
            result = cursor.fetchone()
            
            if not result:
                print(f"数据库 {database_name} 不存在，正在创建...")
                cursor.execute(f"CREATE DATABASE {database_name}")
                conn.commit()
                print(f"数据库 {database_name} 创建成功")
            else:
                print(f"数据库 {database_name} 已存在")
    finally:
        conn.close()

def drop_table_if_exists(database_name, table_name, mysql_host=DORIS_HOST, mysql_port=9030, user='root', password=''):
    """删除表（如果存在）"""
    conn = pymysql.connect(host=mysql_host, port=mysql_port, user=user, password=password, database=database_name)
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            conn.commit()
            print(f"表 {table_name} 已删除")
    finally:
        conn.close()

def check_and_create_table(database_name, table_name, mysql_host=DORIS_HOST, mysql_port=9030, user='root', password=''):
    """检查表是否存在，如果不存在则创建"""
    conn = pymysql.connect(host=mysql_host, port=mysql_port, user=user, password=password, database=database_name)
    try:
        with conn.cursor() as cursor:
            # 检查表是否存在
            cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
            result = cursor.fetchone()
            
            if not result:
                print(f"表 {table_name} 不存在，正在创建...")
                
                # 根据event_log.csv的结构创建表
                create_table_sql = f"""
                CREATE TABLE {table_name} (
                    `dt` VARCHAR(30) NOT NULL COMMENT '事件时间',
                    `uid` VARCHAR(100) NOT NULL COMMENT '用户ID',
                    `event` VARCHAR(100) NOT NULL COMMENT '事件名称',
                    `group0` VARCHAR(200) NULL COMMENT '分组字段0',
                    `group1` VARCHAR(200) NULL COMMENT '分组字段1',
                    `batch_id` BIGINT NOT NULL COMMENT '批次ID',
                    `file_name` VARCHAR(255) NOT NULL COMMENT '文件名',
                    INDEX idx_uid (`uid`),
                    INDEX idx_event (`event`),
                    INDEX idx_dt (`dt`),
                    INDEX idx_batch_id (`batch_id`)
                ) ENGINE=OLAP
                DUPLICATE KEY(`dt`, `uid`, `event`, `group0`, `group1`)
                DISTRIBUTED BY HASH(`uid`) BUCKETS 10
                PROPERTIES (
                    "replication_allocation" = "tag.location.default: 1",
                    "storage_format" = "V2"
                )
                """
                
                cursor.execute(create_table_sql)
                conn.commit()
                print(f"表 {table_name} 创建成功")
            else:
                print(f"表 {table_name} 已存在")
    finally:
        conn.close()

def get_next_batch_id(database_name, table_name, mysql_host=DORIS_HOST, mysql_port=9030, user='root', password=''):
    # 查询当前最大batch_id
    conn = pymysql.connect(host=mysql_host, port=mysql_port, user=user, password=password, database=database_name)
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"SELECT MAX(batch_id) FROM {table_name}")
            result = cursor.fetchone()
            max_batch_id = result[0] if result and result[0] is not None else 0
            return max_batch_id + 1
    finally:
        conn.close()

def detect_encoding(file_path):
    with open(file_path, 'rb') as file:
        raw_data = file.read(10000)
        result = chardet.detect(raw_data)
        return result['encoding']

def read_csv_with_encoding(csv_file):
    # 检测文件编码
    detected_encoding = detect_encoding(csv_file)
    print(f"检测到文件 {csv_file} 的编码为: {detected_encoding}")
    
    # 尝试读取文件
    try:
        df = pd.read_csv(csv_file, encoding=detected_encoding)
        # 将数据保存为UTF-8编码
        df.to_csv(csv_file, encoding='utf-8', index=False)
        print(f"已将文件 {csv_file} 转换为UTF-8编码")
        return df
    except Exception as e:
        print(f"使用检测到的编码 {detected_encoding} 读取失败，尝试其他编码...")
        # 如果检测到的编码读取失败，尝试其他常见编码
        encodings = ['utf-8', 'gbk', 'gb2312', 'gb18030', 'utf-8-sig']
        for encoding in encodings:
            try:
                df = pd.read_csv(csv_file, encoding=encoding)
                # 将数据保存为UTF-8编码
                df.to_csv(csv_file, encoding='utf-8', index=False)
                print(f"使用 {encoding} 编码成功读取文件，并已转换为UTF-8编码")
                return df
            except Exception:
                continue
        raise Exception(f"无法读取文件 {csv_file}，请检查文件编码")

def validate_csv_structure(df, csv_file):
    """验证CSV文件结构是否符合预期"""
    expected_columns = ['dt', 'uid', 'event', 'group0', 'group1']
    missing_columns = [col for col in expected_columns if col not in df.columns]
    
    if missing_columns:
        print(f"警告: 文件 {csv_file} 缺少以下列: {missing_columns}")
        print(f"当前列: {list(df.columns)}")
        print("请确保CSV文件包含以下列: dt, uid, event, group0, group1")
        return False
    
    print(f"文件 {csv_file} 结构验证通过")
    return True

def process_csv_to_json(csv_file, database_name, table_name, batch_size=10000):
    # 读取CSV文件，指定编码为utf-8
    df = read_csv_with_encoding(csv_file)
    
    # 验证CSV文件结构
    if not validate_csv_structure(df, csv_file):
        print(f"跳过文件 {csv_file}，结构不符合要求")
        return
    
    # 数据清洗和转换
    # 先去除dt列的前后空格
    df['dt'] = df['dt'].astype(str).str.strip()
    
    # 尝试先用pandas自动识别
    dt_parsed = pd.to_datetime(df['dt'], errors='coerce')
    
    # 对于无法识别的，尝试手动补毫秒再解析
    mask_failed = dt_parsed.isna()
    if mask_failed.any():
        print(f"发现 {mask_failed.sum()} 行dt格式需要特殊处理...")
        # 尝试补.000
        dt_try = df.loc[mask_failed, 'dt'].str.replace(r'^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})$', r'\1.000', regex=True)
        dt_parsed.loc[mask_failed] = pd.to_datetime(dt_try, errors='coerce')
        
        # 再次检查是否还有失败的
        still_failed = dt_parsed.isna()
        if still_failed.any():
            print(f"警告：仍有 {still_failed.sum()} 行dt无法识别，将被过滤")
            print("无法识别的dt示例：")
            print(df.loc[still_failed, 'dt'].head().tolist())
    
    df['dt'] = dt_parsed
    
    # 记录被过滤的行
    original_count = len(df)
    df = df.dropna(subset=['dt'])
    filtered_count = original_count - len(df)
    if filtered_count > 0:
        print(f"过滤掉 {filtered_count} 条dt为空的行")
    
    if len(df) == 0:
        print(f"文件 {csv_file} 处理后没有有效数据，跳过")
        return

    # 格式化为字符串，补齐毫秒
    df['dt'] = df['dt'].dt.strftime('%Y-%m-%d %H:%M:%S.%f').str[:-3]
    
    # 处理空值
    df['group0'] = df['group0'].fillna('')
    df['group1'] = df['group1'].fillna('')

    # 获取本次导入的批次号
    batch_id = get_next_batch_id(database_name, table_name)
    df['batch_id'] = batch_id

    # 新增：添加文件名列
    file_name = os.path.basename(csv_file)
    df['file_name'] = file_name
    
    # 计算总批次数
    total_batches = (len(df) + batch_size - 1) // batch_size
    
    # 分批处理数据
    for batch_num in range(total_batches):
        start_idx = batch_num * batch_size
        end_idx = min((batch_num + 1) * batch_size, len(df))
        
        # 获取当前批次的数据
        batch_df = df.iloc[start_idx:end_idx]
        
        # 将当前批次转换为JSON
        json_data = batch_df.to_json(orient='records', force_ascii=False)
        
        # 打印当前批次信息
        print(f"\n处理第 {batch_num + 1}/{total_batches} 批数据 (batch_id={batch_id})")
        print(f"批次大小: {len(batch_df)} 条记录")
        
        # 打印JSON数据的前几行
        if batch_num == 0:  # 只在第一批时打印示例
            print("\nJSON数据示例（前3行）:")
            json_obj = json.loads(json_data)
            print(json.dumps(json_obj[:3], ensure_ascii=False, indent=2))
        
        # 导入当前批次到Doris
        print(f"\n导入第 {batch_num + 1} 批数据到Doris...")
        import_to_doris(json_data, table_name, database_name)
        
        print(f"第 {batch_num + 1} 批数据导入完成")
    
    print(f"\n所有数据导入完成，共 {len(df)} 条记录 (batch_id={batch_id})")

def import_to_doris(json_data, table_name, database_name):
    # Doris Stream Load API endpoint
    url = f"http://{DORIS_HOST}:8040/api/{database_name}/{table_name}/_stream_load"
    
    # 设置请求头
    headers = {
        "Expect": "100-continue",
        "Content-Type": "application/json; charset=utf-8",
        "format": "json",
        "strip_outer_array": "true"
    }
    
    # 禁用代理设置
    session = requests.Session()
    session.trust_env = False  # 不使用环境变量中的代理设置
    
    # 发送请求，确保数据使用UTF-8编码
    response = session.put(
        url,
        auth=('root', ''),  # 用户名和密码
        headers=headers,
        data=json_data.encode('utf-8')  # 显式指定UTF-8编码
    )
    
    # 打印响应结果
    print("\nResponse Status:", response.status_code)
    print("Response Content:", response.text)
    
    # 检查响应状态
    if response.status_code == 200:
        try:
            response_json = response.json()
            if response_json.get('Status') == 'Success':
                print("✅ 数据导入成功")
            else:
                print(f"❌ 数据导入失败: {response_json.get('Message', '未知错误')}")
        except Exception:
            print("⚠️ Doris返回内容不是标准JSON，可能导入成功也可能失败，请人工检查！")
    else:
        print(f"❌ 请求失败，状态码: {response.status_code}")

def main():
    # 配置参数
    database_name = "doris_udf_test"  # 数据库名
    table_name = "user_event_log"  # 表名
    csv_directory = "test-data-csv"  # CSV文件目录
    
    print("=== Doris数据导入工具 ===")
    print(f"目标数据库: {database_name}")
    print(f"目标表: {table_name}")
    print(f"CSV文件目录: {csv_directory}")
    print(f"Doris主机: {DORIS_HOST}")
    print()
    
    # 检查CSV目录是否存在
    if not os.path.exists(csv_directory):
        print(f"❌ CSV目录 {csv_directory} 不存在！")
        return
    
    # 检查并创建数据库
    try:
        check_and_create_database(database_name)
    except Exception as e:
        print(f"❌ 检查/创建数据库失败: {str(e)}")
        return
    
    # 检查表是否存在
    conn = pymysql.connect(host=DORIS_HOST, port=9030, user='root', password='', database=database_name)
    table_exists = False
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
            result = cursor.fetchone()
            table_exists = result is not None
    finally:
        conn.close()
    
    if table_exists:
        print(f"表 {table_name} 已存在")
        choice = input("是否要删除并重新创建表？(y/N): ").strip().lower()
        if choice == 'y':
            try:
                drop_table_if_exists(database_name, table_name)
                check_and_create_table(database_name, table_name)
            except Exception as e:
                print(f"❌ 重新创建表失败: {str(e)}")
                return
        else:
            print("使用现有表结构")
    else:
        # 检查并创建表
        try:
            check_and_create_table(database_name, table_name)
        except Exception as e:
            print(f"❌ 检查/创建表失败: {str(e)}")
            return
    
    # 读取指定目录下所有csv文件
    csv_pattern = os.path.join(csv_directory, "*.csv")
    csv_files = glob.glob(csv_pattern)
    if not csv_files:
        print(f"目录 {csv_directory} 下未找到csv文件！")
        return
    
    print(f"\n找到 {len(csv_files)} 个CSV文件:")
    for i, csv_file in enumerate(csv_files, 1):
        print(f"  {i}. {os.path.basename(csv_file)}")
    
    for csv_file in csv_files:
        print(f"\n{'='*50}")
        print(f"开始处理文件: {os.path.basename(csv_file)}")
        print(f"完整路径: {csv_file}")
        print(f"{'='*50}")
        try:
            process_csv_to_json(csv_file, database_name, table_name)
        except Exception as e:
            print(f"❌ 处理文件 {csv_file} 时出错: {str(e)}")
            continue
    
    print(f"\n{'='*50}")
    print("所有文件处理完成！")
    print(f"{'='*50}")

if __name__ == "__main__":
    main() 