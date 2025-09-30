# -*- coding: utf-8 -*-
import redis
import time
import sys
# --- 配置源 Redis 和目标 Redis 的连接信息 ---
SOURCE_REDIS_CONFIG = {
    'host': '127.0.0.1',
    'port': 6379,
    'db': 0,
    'password': ""  # 如果没有密码，则为 None
}
filename ="./redis-data.json"

def connect_redis(config):
    """根据配置连接 Redis，并进行 PING 测试"""
    try:
        # decode_responses=False 很重要，确保我们能处理二进制安全的数据
        r = redis.StrictRedis(
            host=config['host'],
            port=config['port'],
            db=config['db'],
            password=config['password'],
            decode_responses=False
        )
        r.ping()
        print(f"成功连接到 Redis: {config['host']}:{config['port']}")
        return r
    except redis.exceptions.RedisError as e:
        print(f"连接 Redis 失败: {config['host']}:{config['port']}, 错误: {e}")
        return None

# 导出到json文件
def output_redis_data():
    source_r = connect_redis(SOURCE_REDIS_CONFIG)
    if not source_r:
        return

    print("开始读取源 Redis 数据...")

    cursor = 0
    total_count = 0
    while True:
        cursor, keys = source_r.scan(cursor, count=500)  # 每次处理500个
        if not keys:
            if cursor == 0:
                break
            continue
        with open(filename, "ab") as f:
            for key in keys:
                try:
                    key_type = source_r.type(key).decode('utf-8')
                    if key_type == 'string':
                        value = source_r.get(key)
                        entry = {
                            "type": "string",
                            "key": key.decode('utf-8', errors='ignore'),
                            "value": value.decode('utf-8', errors='ignore')
                        }
                    elif key_type == 'list':
                        value = source_r.lrange(key, 0, -1)
                        entry = {
                            "type": "list",
                            "key": key.decode('utf-8', errors='ignore'),
                            "value": [v.decode('utf-8', errors='ignore') for v in value]
                        }
                    elif key_type == 'set':
                        value = source_r.smembers(key)
                        entry = {
                            "type": "set",
                            "key": key.decode('utf-8', errors='ignore'),
                            "value": [v.decode('utf-8', errors='ignore') for v in value]
                        }
                    elif key_type == 'zset':
                        value = source_r.zrange(key, 0, -1, withscores=True)
                        entry = {
                            "type": "zset",
                            "key": key.decode('utf-8', errors='ignore'),
                            "value": [{"member": v[0].decode('utf-8', errors='ignore'), "score": v[1]} for v in value]
                        }
                    elif key_type == 'hash':
                        value = source_r.hgetall(key)
                        entry = {
                            "type": "hash",
                            "key": key.decode('utf-8', errors='ignore'),
                            "value": {k.decode('utf-8', errors='ignore'): v.decode('utf-8', errors='ignore') for k, v in value.items()}
                        }
                    else:
                        print(f"跳过不支持的类型: {key_type}，键: {key}")
                        continue
                    print("导出键:", entry["key"], "类型:", entry["type"])
                    f.write((str(entry) + "\n").encode('utf-8'))
                    total_count += 1
                except Exception as e:
                    print(f"处理键时出错: {key}, 错误: {e}")
        if len(keys) < 500:
            break  # 没有更多数据了
    print(f"数据导出完成，共导出 {total_count} 个键。")

def import_redis_data():
    source_r = connect_redis(SOURCE_REDIS_CONFIG)
    if not source_r:
        print("源 Redis 连接失败，无法导入数据。")
        return
    print("开始导入数据到目标 Redis...")
    with open(filename, "rb") as f:
        for line in f:
            try:
                entry = eval(line.decode('utf-8'))
                key_type = entry["type"]
                key = entry["key"].encode('utf-8')
                if key_type == "string":
                    value = entry["value"].encode('utf-8')
                    source_r.set(key, value)
                elif key_type == "list":
                    values = [v.encode('utf-8') for v in entry["value"]]
                    source_r.delete(key)
                    source_r.rpush(key, *values)
                elif key_type == "set":
                    values = [v.encode('utf-8') for v in entry["value"]]
                    source_r.delete(key)
                    source_r.sadd(key, *values)
                elif key_type == "zset":
                    mapping = {v["member"].encode('utf-8'): v["score"] for v in entry["value"]}
                    source_r.delete(key)
                    source_r.zadd(key, mapping)
                elif key_type == "hash":
                    mapping = {k.encode('utf-8'): v.encode('utf-8') for k, v in entry["value"].items()}
                    source_r.delete(key)
                    source_r.hset(key, mapping=mapping)
                else:
                    print(f"跳过不支持的类型: {key_type}，键: {key}")
                    continue
            except Exception as e:
                print(f"导入数据时出错: {line}, 错误: {e}")
    print("数据导入完成。")


if __name__ == "__main__":
    # 接受外部参数 决定是导出还是导入
    if len(sys.argv) != 2 or sys.argv[1] not in ["export", "import"]:
        print("用法: python redis_output.py [export|import]")
        sys.exit(1)
    action = sys.argv[1]
    if action == "export":
        output_redis_data()
    elif action == "import":
        import_redis_data()