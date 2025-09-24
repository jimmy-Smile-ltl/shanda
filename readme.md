# 工作笔记

## 1. 采集源信息

### 1.1 arxiv 

#### 1.1.1 网站概述

​	html 只有摘要，pdf等文件全文可以下载，缺乏引用数据。文章的url是有规则的。 年月+编号（一个月的，00001 开始 ，看那意思是在十万内），理论上，做全网站采集技术上没有问题。

https://arxiv.org/abs/2409.00001

​	直接点击一个分类，只给看一个月之内的信息。更长时间的需要高级搜索。从高级搜索的意思来看，做全量采集也是可行的。最多在日期上放缩一下范围。精准到AI 小类，有难度。

> 如果出现了预期之外的结果，排查不到问题，记得看看是不是多巧了空格，特别是数字参数

2025.09.23 完成 AI recent 采集，PostgresSQL 调度验证完成



#### 待分析数据源

1. https://papercopilot.com/paper-list/iclr-paper-list/iclr-2024-paper-list/
2. https://openreview.net/group?id=ICLR.cc/2024/Conference#tab-accept-oral

#### 1.1.2  采集技术探索





## 2. 设计规范

### 2.1 采集流程



### 2.2 数据库设计

>  目前由于不好决定一个最终的应该含有的字段，而且，网站之间存在明显的差异，现在不好确定。

>  决定是，一个网站一个表，做的时候尽可能的前后兼容，然后维护一个记录全部表信息和网站信息的表的表。

#### 2.2.1 表之表设计

定位到一张表 db schema table

叠加 信息源相关资料

```sql
 create_sql = f'''
        CREATE TABLE IF NOT EXISTS "{self.schema}"."{self.table_name}" (
            id SERIAL PRIMARY KEY,
            source_name VARCHAR(256) NOT NULL,
            source_url VARCHAR(512),
            category VARCHAR(128) NOT NULL,
            database_name VARCHAR(128),
            mysql_table VARCHAR(128) NOT NULL,
            schema VARCHAR(64) DEFAULT 'spider',
            count INT DEFAULT 0,
            create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(category, mysql_table, source_url)
        );

        CREATE OR REPLACE FUNCTION update_modified_column()
        RETURNS TRIGGER AS $$
        BEGIN
            NEW.update_time = CURRENT_TIMESTAMP;
            RETURN NEW;
        END;
        $$ language 'plpgsql';

        DROP TRIGGER IF EXISTS update_{self.table_name}_modtime ON "{self.schema}"."{self.table_name}";
        CREATE TRIGGER update_{self.table_name}_modtime
            BEFORE UPDATE ON "{self.schema}"."{self.table_name}"
            FOR EACH ROW
            EXECUTE FUNCTION update_modified_column();
        '''
```

#### 2.2.2 一般的表设计

#####  2.2.2.1 设计原则

+ 保留htm 源代码l数据 与url。方便后期排查
+ 



##### 2.2.2.2 一般来说，应该包含字段（都是论文类的数据）

数据源信息

- site
- source
- language

文章基础信息

+ 作者
+ 时间
+ 领域
+ 关键词
+ 摘要



## 3. 评价方法

