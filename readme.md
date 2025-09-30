# 工作笔记

## 1. 采集源信息

### 1.1 arxiv 

#### 1.1.1 网站概述

​	html 只有摘要，pdf等文件全文可以下载，缺乏引用数据。文章的url是有规则的。 年月+编号（一个月的，00001 开始 ，看那意思是在十万内），理论上，做全网站采集技术上没有问题。

https://arxiv.org/abs/2409.00001

​	直接点击一个分类，只给看一个月之内的信息。更长时间的需要高级搜索。从高级搜索的意思来看，做全量采集也是可行的。最多在日期上放缩一下范围。精准到AI 小类，有难度。

> 如果出现了预期之外的结果，排查不到问题，记得看看是不是多巧了空格，特别是数字参数

2025.09.23 完成 AI recent 采集，PostgresSQL 调度验证完成

### 1.2 google scholar

#### 1.2.1 作者信息

---

##### 1. 核心类 `GetAuthorInfoById`

*   **初始化 (`__init__`)**:
    *   接收一个 `scholar_id` 作为输入。
    *   配置了详细的 HTTP 请求头 (`headers`) 和 Cookies，以模拟真实浏览器访问，旨在绕过反爬虫机制。
    *   初始化了自定义的工具类，包括：
        *   `SingleRequestHandler`: 用于发送单个网络请求。
        *   `Cache`: 用于通过 Redis 记录已处理过的 ID，避免重复抓取。
        *   `LogPrint`: 用于日志输出。

*   **主执行方法 (`run`)**:
    *   作为类的入口点，协调整个抓取流程。
    *   首先调用 `is_has_record()` 检查该 ID 是否已被处理，如果是则直接跳过。
    *   依次调用内部方法抓取作者的**主页信息**、**合作者列表**和**文章列表**。
    *   抓取完成后，将该 ID 存入缓存，并返回包含所有信息的字典。

---

##### 2. 信息抓取模块

*   **`get_home_info`**:
    *   **功能**: 抓取作者的个人主页。
    *   **提取内容**: 使用 `BeautifulSoup` 解析 HTML，提取以下信息：
        *   姓名、头像 URL、主页链接。
        *   学术指标（引用总数、h-index 等），并区分“全部”和“2020年后”两个维度。
        *   单位和职称。
        *   研究领域。
        *   年度被引次数图表数据。
        *   开放获取文章数量。

*   **`get_coauthors`**:
    *   **功能**: 抓取作者的合作者列表。
    *   **实现**: 请求一个特定的 `list_colleagues` 接口，该接口一次性返回所有合作者数据，无需分页。
    *   **提取内容**: 合作者的姓名、主页链接和单位信息。

*   **`get_articles`**:
    *   **功能**: 抓取作者发表的所有文章列表。
    *   **实现**:
        *   实现了**分页抓取**逻辑，以每页 100 篇的设置循环请求，直到获取所有文章。
        *   包含**反爬虫策略**：每次翻页后暂停 1 秒 (`time.sleep(1)`)。
        *   包含**优化逻辑**：当抓取到的文章引用数小于 5 时，提前停止翻页，以提高效率。
    *   **提取内容**: 每篇文章的标题、链接、作者列表、发表信息、被引次数和年份。

---

3. ##### 结果示范

   ```json
   {
       "scholar_id": "DTthB48AAAAJ",
       "name": "示例作者",
       "avatar_url": "https://scholar.google.com/citations/images/avatar_scholar_150.jpg",
       "profile_url": "https://scholar.google.com/citations?user=DTthB48AAAAJ&hl=zh-CN&oi=sra",
       "scholar_index": {
           "引用": {
               "all": 15000,
               "after_2020": 9000
           },
           "h-index": {
               "all": 60,
               "after_2020": 45
           },
           "i10-index": {
               "all": 120,
               "after_2020": 95
           }
       },
       "affiliation": "示例大学, 计算机科学与技术系",
       "category": [
           {
               "text": "人工智能",
               "url": "https://scholar.google.com/citations?hl=zh-CN&view_op=search_authors&mauthors=label:artificial_intelligence"
           },
           {
               "text": "机器学习",
               "url": "https://scholar.google.com/citations?hl=zh-CN&view_op=search_authors&mauthors=label:machine_learning"
           }
       ],
       "cite_per_year": {
           "2022": 1800,
           "2023": 2100,
           "2024": 2500
       },
       "open_access_num": 50,
       "non_open_access_num": 80,
       "collaborator_list": [
           {
               "id": "gsc_ucoar_0",
               "name": "合作者A",
               "profile_url": "https://scholar.google.com/citations?user=xxxxxxxxxxxx&hl=zh-CN",
               "affiliation": "另一所示例大学"
           }
       ],
       "article_list": [
           {
               "article_title": "一篇关于人工智能的示例论文",
               "article_url": "https://scholar.google.com/citations?view_op=view_citation&hl=zh-CN&user=DTthB48AAAAJ&citation_for_view=DTthB48AAAAJ:xxxxxxxxxx",
               "authors": "示例作者, 合作者A, 合作者B",
               "publication_info": "顶级期刊, 2023",
               "cited_num": 250,
               "year": 2023
           },
           {
               "article_title": "另一篇关于机器学习的示例论文",
               "article_url": "https://scholar.google.com/citations?view_op=view_citation&hl=zh-CN&user=DTthB48AAAAJ&citation_for_view=DTthB48AAAAJ:yyyyyyyyyy",
               "authors": "示例作者, 合作者C",
               "publication_info": "顶级会议, 2022",
               "cited_num": 180,
               "year": 2022
           }
       ]
   }
   ```

#### 1.2.2 搜索标题

该 Python 脚本定义了一个核心类 `GetArticleByTitle`，其主要功能是根据给定的论文标题，在 Google Scholar 上进行搜索，并抓取搜索结果列表中的相关文章信息。

---

##### 1. 核心类 `GetArticleByTitle`

*   **初始化 (`__init__`)**:
    *   接收一个 `title` (论文标题) 作为输入。
    *   配置了详细的 HTTP 请求头 (`headers`) 和 Cookies，以模拟真实浏览器的行为，旨在绕过 Google Scholar 的反爬虫检测。
    *   初始化了自定义的 `SingleRequestHandler`，用于执行网络请求。
    *   初始化了 `LogPrint` 用于日志输出。

*   **主执行方法 (`run`)**:
    *   作为类的入口点，负责执行整个搜索和抓取流程。
    *   构建 Google Scholar 的搜索 URL，将论文标题作为查询参数。
    *   发送 HTTP 请求获取搜索结果页面的 HTML 内容。
    *   使用 `BeautifulSoup` 解析 HTML，并定位到包含搜索结果的列表。
    *   遍历每一条搜索结果，提取关键信息，并调用 `extract_author_info` 方法来解析作者详情。
    *   如果找不到任何结果，会打印日志并返回 `None`。
    *   最终返回一个包含多篇文章信息的列表 (`article_list`)。

*   **信息提取方法 (`extract_author_info`)**:
    *   这是一个辅助方法，专门用于从单个搜索结果条目中解析作者和出版信息。
    *   它能处理两种不同的 HTML 结构，以确保能抓取到作者行。
    *   **提取内容**:
        *   **作者列表**: 提取所有作者的姓名，并为那些提供了主页链接的作者抓取其 Google Scholar 个人主页 URL。
        *   **作者顺序**: 记录每位作者在作者列表中的顺序。
        *   **出版信息**: 提取文章的出版信息（如期刊、年份等）。
    *   将提取出的作者信息和出版信息存入传入的 `article_info` 字典中。

---

##### 2. 单条文章信息提取逻辑 (在 `run` 方法中)

对于搜索结果中的每一篇文章，脚本会提取以下字段：

*   `article_title`: 文章标题。
*   `article_url`: 文章在 Google Scholar 上的链接。
*   `cited_num`: 被引用次数（通过正则表达式从文本中提取数字）。
*   `html`: 该条搜索结果的原始 HTML 代码片段。
*   `author_dict_list`: 一个包含所有作者信息的列表，由 `extract_author_info` 生成。
*   `publish_info`: 出版信息，由 `extract_author_info` 生成。

---

3. ##### 数据示例

   ```json
   [
       {
           "article_title": "Why and How Auxiliary Tasks Improve JEPA Representations",
           "article_url": "https://scholar.google.com/citations?view_op=view_citation&hl=zh-CN&user=xxxxxxxx&citation_for_view=xxxxxxxx:xxxxxxxx",
           "cited_num": 50,
           "html": "<div class=\"gs_r gs_or gs_scl\">... (此处为该条目的原始HTML) ...</div>",
           "author_dict_list": [
               {
                   "name": "作者A",
                   "order": 1,
                   "url": "https://scholar.google.com/citations?user=author_a_id&hl=zh-CN"
               },
               {
                   "name": "作者B",
                   "order": 2,
                   "url": null
               },
               {
                   "name": "作者C",
                   "order": 3,
                   "url": "https://scholar.google.com/citations?user=author_c_id&hl=zh-CN"
               }
           ],
           "publish_info": "arXiv preprint arXiv:2404.10721, 2024 - arxiv.org"
       },
       {
           "article_title": "相关论文标题示例",
           "article_url": "https://scholar.google.com/citations?view_op=view_citation&hl=zh-CN&user=yyyyyyyy&citation_for_view=yyyyyyyy:yyyyyyyy",
           "cited_num": 15,
           "html": "<div class=\"gs_r gs_or gs_scl\">... (此处为该条目的原始HTML) ...</div>",
           "author_dict_list": [
               {
                   "name": "作者D",
                   "order": 1,
                   "url": null
               }
           ],
           "publish_info": "示例期刊, 2023 - example.com"
       }
   ]
   ```

#### 1.2.3 作者信息(搜索标题)

该脚本是一个爬虫程序，其核心功能是从数据库中读取文章标题，使用这些标题在 Google Scholar 上进行搜索，抓取相关的文章和作者信息，并将结果存入新的数据库表中。

##### 1. 初始化 (`__init__`)
- **设置**: 定义目标网站 (`scholar.google.com`) 和请求头。
- **工具**: 初始化数据库处理器 (`PostgreSQLHandler`)、日志 (`LogPrint`) 和缓存 (`Cache`)。
- **表定义**:
    - `self.table_name_read`: 数据源表 (如 `article_arxiv_org`)。
    - `self.table_name_article`: 存储 Google Scholar 文章搜索结果的表。
    - `self.table_name_author`: 存储 Google Scholar 作者详细信息的表。
- **表创建**: 调用 `create_table_*` 方法，确保目标数据表在数据库中存在。

##### 2. 核心处理逻辑 (`handle_one_title`)
- **输入**: 单个文章标题 (`title`) 和其在源表中的ID (`article_id`)。
- **搜索**: 使用 `GetArticleByTitle` 类根据标题搜索文章。
- **存储文章**: 将搜索到的每篇文章信息存入 `article_search_by_google_scholar` 表。
- **处理作者**:
    - 如果文章是新插入的，则继续处理其作者列表。
    - 对有主页链接的作者，解析出 `author_id`。
    - 使用 `GetAuthorInfoById` 类根据 `author_id` 抓取作者的详细主页信息。
    - 将作者信息存入 `scholar_author` 表。
- **延时**: 方法最后会 `time.sleep(2)` 以降低请求频率。

##### 3. 主执行循环 (`run`)
- **断点续传**: 从缓存中获取上次处理的ID，实现从中断处继续。
- **批处理**: 在 `while` 循环中，分批次（每次20个）从源数据表中获取文章。
- **调用处理**: 对批次中的每一篇文章，调用 `handle_one_title` 方法进行处理。
- **延时**: 每个批次处理完毕后，会 `time.sleep(5)` 以控制爬取速度。
- **结束**: 循环直到处理完所有文章。

---

##### 依赖

- **外部库**:
    - `requests`: 用于网络请求。
    - `beautifulsoup4`: 用于HTML解析。
- **内部模块**:
    - `myutil.*`: 自定义的工具类，用于处理数据库、HTTP请求、日志、缓存等。
    - `GetArticleByTitle`: 封装了按标题搜索 Google Scholar 的逻辑。
    - `GetAuthorInfoById`: 封装了按作者ID抓取其主页信息的逻辑。









#### 待分析数据源

1. https://papercopilot.com/paper-list/iclr-paper-list/iclr-2024-paper-list/   # 会议,这个聚合了一下
2. https://openreview.net/group?id=ICLR.cc/2024/Conference#tab-accept-oral   #  开放评论,的
3. https://scholar.google.es/citations?view_op=top_venues&hl=en&vq=eng_artificialintelligence  #好的AI领域出版物 列表 google scholar 可以通过这里,把文章获取完 点一下h5指数,例如: 未登录 隐私模式下正常访问  除了AI子类 还有 数据分析 与信息系统之类 可以考虑做采集 
   1. https://scholar.google.es/citations?hl=en&vq=eng_artificialintelligence&view_op=list_hcore&venue=AlAHN-bTk3IJ.2025
4. https://www.ccf.org.cn/Academic_Evaluation/AI/   #中国科学院 AI 排名 ,有官网 和评级



##### :heavy_exclamation_mark: 必须要登录 google的 领域作者列表  关键不在strat参数 在after参数 ,值为作者ID  ,修改start参数不生效,after优先级更高

[artificial intelligence](https://scholar.google.com/citations?view_op=search_authors&hl=zh-CN&mauthors=label:artificial_intelligence

#### 1.1.2  采集技术探索

##### 1.1.2.1 代理提供商的选择

1. 快代理 常用 但是 需要登录 实名注册等信息

![image-20250926002331678](/Users/admin/Desktop/spider/assets/image-20250926002331678.png)

海外太贵了



![image-20250926014751256](/Users/admin/Desktop/spider/assets/image-20250926014751256.png)



https://www.ipfoxy.net/pricing/rotating/

![image-20250926014320797](/Users/admin/Desktop/spider/assets/image-20250926014320797.png)



https://www.bright.cn/pricing/proxy-network/residential-proxies

![image-20250926014350717](/Users/admin/Desktop/spider/assets/image-20250926014350717.png)



https://www.smartproxy.cn/buy/static

![image-20250926014435513](/Users/admin/Desktop/spider/assets/image-20250926014435513.png)





https://www.711proxy.com/pricing/regular/residential-proxies-ip

![image-20250926014519990](/Users/admin/Desktop/spider/assets/image-20250926014519990.png)



https://www.kookeey.com/pricing/residential-proxies

![image-20250926014627947](/Users/admin/Desktop/spider/assets/image-20250926014627947.png)



https://stormproxies.com/rotating_reverse_proxies.html

![image-20250926014651450](/Users/admin/Desktop/spider/assets/image-20250926014651450.png)

https://shifter.io/pricing?product=residential-proxies&subproduct=basic-backconnect-proxy







![image-20250926014853236](/Users/admin/Desktop/spider/assets/image-20250926014853236.png)



https://www.shenlongproxy.com/buy?packageType=1

![image-20250928011504799](/Users/admin/Desktop/spider/assets/image-20250928011504799.png)



https://www.711proxy.com/zh-TW/pricing/regular/residential-proxies-gb

![image-20250928011625935](/Users/admin/Desktop/spider/assets/image-20250928011625935.png)



https://www.nextip.cc/pricing/residential-traffic/

![image-20250928012024123](/Users/admin/Desktop/spider/assets/image-20250928012024123.png)





https://instantproxies.com/pricing/

![image-20250928012247404](/Users/admin/Desktop/spider/assets/image-20250928012247404.png)



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

#### 3.1 以google scholar 为中心的话(不往外扩展)

获取不到的是文章的全文, 其他基本都可以