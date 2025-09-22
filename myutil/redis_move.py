# -*- coding: utf-8 -*-
import redis
import time

# --- 配置源 Redis 和目标 Redis 的连接信息 ---
SOURCE_REDIS_CONFIG = {
    'host': '127.0.0.1',
    'port': 6379,
    'db': 0,
    'password': "jimmysmile"  # 如果没有密码，则为 None
}

TARGET_REDIS_CONFIG = {
    'host': '192.168.130.53',
    'port': 6379,  # 假设目标 Redis 在不同端口
    'db': 1,
    'password': "123456"
}


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


def migrate_data_by_type(source_r, target_r):
    """
    通过 SCAN 遍历源 Redis，并根据每个 key 的类型进行迁移。
    这种方法兼容不同版本的 Redis。
    """
    migrated_count = 0
    skipped_count = 0
    start_time = time.time()

    print("开始数据迁移（版本兼容模式）...")

    cursor = 0
    while True:
        cursor, keys = source_r.scan(cursor, count=500)  # 每次处理500个

        if not keys:
            if cursor == 0:
                break
            continue

        target_pipe = target_r.pipeline()

        for key in keys:
            try:
                key_type = source_r.type(key).decode('utf-8')
                ttl = source_r.pttl(key)  # 使用 PTTL 获取毫秒级过期时间

                # 默认-1为无过期时间, -2为key不存在 (可能在scan和type之间过期了)
                if ttl == -2:
                    skipped_count += 1
                    continue

                if ttl == -1:
                    ttl = 0  # 在 pipeline 中 0 表示不过期

                # 根据不同类型处理
                if key_type == 'string':
                    value = source_r.get(key)
                    target_pipe.set(key, value, px=ttl if ttl > 0 else None)
                elif key_type == 'list':
                    values = source_r.lrange(key, 0, -1)
                    if values:
                        target_pipe.delete(key)  # 先删除旧的，防止追加
                        target_pipe.rpush(key, *values)
                        if ttl > 0: target_pipe.pexpire(key, ttl)
                elif key_type == 'hash':
                    values = source_r.hgetall(key)
                    if values:
                        target_pipe.delete(key)
                        target_pipe.hset(key, mapping=values)
                        if ttl > 0: target_pipe.pexpire(key, ttl)
                elif key_type == 'set':
                    values = source_r.smembers(key)
                    if values:
                        target_pipe.delete(key)
                        target_pipe.sadd(key, *values)
                        if ttl > 0: target_pipe.pexpire(key, ttl)
                elif key_type == 'zset':
                    values = source_r.zrange(key, 0, -1, withscores=True)
                    if values:
                        target_pipe.delete(key)
                        target_pipe.zadd(key, dict(values))
                        if ttl > 0: target_pipe.pexpire(key, ttl)
                else:
                    print(f"警告: 暂不支持的数据类型 '{key_type}' for key '{key.decode('utf-8', 'ignore')}', 已跳过。")
                    skipped_count += 1
                    continue

                migrated_count += 1

            except Exception as e:
                print(f"处理 key '{key.decode('utf-8', 'ignore')}' 时出错: {e}")
                skipped_count += 1

        # 执行批量写入
        try:
            target_pipe.execute()
        except Exception as e:
            print(f"Pipeline 执行失败: {e}")

        if (migrated_count + skipped_count) % 1000 < 500:  # 避免频繁打印
            print(
                f"已处理 {migrated_count + skipped_count} 个 key... (迁移成功: {migrated_count}, 跳过: {skipped_count})")
        if len(keys) < 500:
            break

    end_time = time.time()
    print("\n----- 数据迁移完成 -----")
    print(f"总共迁移了 {migrated_count} 个 key。")
    print(f"总共跳过了 {skipped_count} 个 key。")
    print(f"总耗时: {end_time - start_time:.2f} 秒。")


if __name__ == '__main__':
    source_redis = connect_redis(SOURCE_REDIS_CONFIG)
    target_redis = connect_redis(TARGET_REDIS_CONFIG)

    if source_redis and target_redis:
        try:
            # 迁移前，可以选择清空目标数据库
            # print("正在清空目标数据库...")
            # target_redis.flushdb()
            # print("目标数据库已清空。")

            # 使用新的按类型迁移的函数
            migrate_data_by_type(source_redis, target_redis)
        except Exception as e:
            print(f"迁移过程中发生未知错误: {e}")