![Datax-logo](https://github.com/userkdg/DataX/blob/master/images/DataX-logo.jpg)


# DataX

DataX 是阿里云 [DataWorks数据集成](https://www.aliyun.com/product/bigdata/ide) 的开源版本，在阿里巴巴集团内被广泛使用的离线数据同步工具/平台。DataX 实现了包括 MySQL、Oracle、OceanBase、SqlServer、Postgre、HDFS、Hive、ADS、HBase、TableStore(OTS)、MaxCompute(ODPS)、Hologres、DRDS 等各种异构数据源之间高效的数据同步功能。

# Features

DataX本身作为数据同步框架，将不同数据源的同步抽象为从源头数据源读取数据的Reader插件，以及向目标端写入数据的Writer插件，理论上DataX框架可以支持任意数据源类型的数据同步工作。同时DataX插件体系作为一套生态系统, 每接入一套新数据源该新加入的数据源即可实现和现有的数据源互通。



# DataX详细介绍

##### 请参考：[DataX-Introduction](https://github.com/userkdg/DataX/blob/master/introduction.md)


# Support Data Channels 

DataX目前已经有了比较全面的插件体系，主流的RDBMS数据库、NOSQL、大数据计算系统都已经接入，目前支持数据如下图，详情请点击：[DataX数据源参考指南](https://github.com/userkdg/DataX/wiki/DataX-all-data-channels)

| 类型           | 数据源        | Reader(读) | Writer(写) |文档|
| ------------ | ---------- | :-------: | :-------: |:-------: |
| RDBMS 关系型数据库 | MySQL      |     √     |     √     |[读](https://github.com/userkdg/DataX/blob/master/mysqlreader/doc/mysqlreader.md) 、[写](https://github.com/userkdg/DataX/blob/master/mysqlwriter/doc/mysqlwriter.md)|
|              | Oracle     |     √     |     √     |[读](https://github.com/userkdg/DataX/blob/master/oraclereader/doc/oraclereader.md) 、[写](https://github.com/userkdg/DataX/blob/master/oraclewriter/doc/oraclewriter.md)|
|              | OceanBase  |     √     |     √     |[读](https://open.oceanbase.com/docs/community/oceanbase-database/V3.1.0/use-datax-to-full-migration-data-to-oceanbase) 、[写](https://open.oceanbase.com/docs/community/oceanbase-database/V3.1.0/use-datax-to-full-migration-data-to-oceanbase)|
|              | SQLServer  |     √     |     √     |[读](https://github.com/userkdg/DataX/blob/master/sqlserverreader/doc/sqlserverreader.md) 、[写](https://github.com/userkdg/DataX/blob/master/sqlserverwriter/doc/sqlserverwriter.md)|
|              | PostgreSQL |     √     |     √     |[读](https://github.com/userkdg/DataX/blob/master/postgresqlreader/doc/postgresqlreader.md) 、[写](https://github.com/userkdg/DataX/blob/master/postgresqlwriter/doc/postgresqlwriter.md)|
|              | DRDS |     √     |     √     |[读](https://github.com/userkdg/DataX/blob/master/drdsreader/doc/drdsreader.md) 、[写](https://github.com/userkdg/DataX/blob/master/drdswriter/doc/drdswriter.md)|
|              | 通用RDBMS(支持所有关系型数据库 hive-jdbc, impala, clickhouse, kylin, hana ...)         |     √     |     √     |[读](https://github.com/userkdg/DataX/blob/master/rdbmsreader/doc/rdbmsreader.md) 、[写](https://github.com/userkdg/DataX/blob/master/rdbmswriter/doc/rdbmswriter.md)|
| 阿里云数仓数据存储    | ODPS       |     √     |     √     |[读](https://github.com/userkdg/DataX/blob/master/odpsreader/doc/odpsreader.md) 、[写](https://github.com/userkdg/DataX/blob/master/odpswriter/doc/odpswriter.md)|
|              | ADS        |           |     √     |[写](https://github.com/userkdg/DataX/blob/master/adswriter/doc/adswriter.md)|
|              | OSS        |     √     |     √     |[读](https://github.com/userkdg/DataX/blob/master/ossreader/doc/ossreader.md) 、[写](https://github.com/userkdg/DataX/blob/master/osswriter/doc/osswriter.md)|
|              | OCS        |           |     √     |[写](https://github.com/userkdg/DataX/blob/master/ocswriter/doc/ocswriter.md)|
| NoSQL数据存储    | OTS        |     √     |     √     |[读](https://github.com/userkdg/DataX/blob/master/otsreader/doc/otsreader.md) 、[写](https://github.com/userkdg/DataX/blob/master/otswriter/doc/otswriter.md)|
|              | Hbase0.94  |     √     |     √     |[读](https://github.com/userkdg/DataX/blob/master/hbase094xreader/doc/hbase094xreader.md) 、[写](https://github.com/userkdg/DataX/blob/master/hbase094xwriter/doc/hbase094xwriter.md)|
|              | Hbase1.1   |     √     |     √     |[读](https://github.com/userkdg/DataX/blob/master/hbase11xreader/doc/hbase11xreader.md) 、[写](https://github.com/userkdg/DataX/blob/master/hbase11xwriter/doc/hbase11xwriter.md)|
|              | Phoenix4.x   |     √     |     √     |[读](https://github.com/userkdg/DataX/blob/master/hbase11xsqlreader/doc/hbase11xsqlreader.md) 、[写](https://github.com/userkdg/DataX/blob/master/hbase11xsqlwriter/doc/hbase11xsqlwriter.md)|
|              | Phoenix5.x   |     √     |     √     |[读](https://github.com/userkdg/DataX/blob/master/hbase20xsqlreader/doc/hbase20xsqlreader.md) 、[写](https://github.com/userkdg/DataX/blob/master/hbase20xsqlwriter/doc/hbase20xsqlwriter.md)|
|              | MongoDB    |     √     |     √     |[读](https://github.com/userkdg/DataX/blob/master/mongodbreader/doc/mongodbreader.md) 、[写](https://github.com/userkdg/DataX/blob/master/mongodbwriter/doc/mongodbwriter.md)|
|              | Hive       |     √     |     √     |[读](https://github.com/userkdg/DataX/blob/master/hdfsreader/doc/hdfsreader.md) 、[写](https://github.com/userkdg/DataX/blob/master/hdfswriter/doc/hdfswriter.md)|
|              | Cassandra       |     √     |     √     |[读](https://github.com/userkdg/DataX/blob/master/cassandrareader/doc/cassandrareader.md) 、[写](https://github.com/userkdg/DataX/blob/master/cassandrawriter/doc/cassandrawriter.md)|
| 无结构化数据存储     | TxtFile    |     √     |     √     |[读](https://github.com/userkdg/DataX/blob/master/txtfilereader/doc/txtfilereader.md) 、[写](https://github.com/userkdg/DataX/blob/master/txtfilewriter/doc/txtfilewriter.md)|
|              | FTP        |     √     |     √     |[读](https://github.com/userkdg/DataX/blob/master/ftpreader/doc/ftpreader.md) 、[写](https://github.com/userkdg/DataX/blob/master/ftpwriter/doc/ftpwriter.md)|
|              | HDFS       |     √     |     √     |[读](https://github.com/userkdg/DataX/blob/master/hdfsreader/doc/hdfsreader.md) 、[写](https://github.com/userkdg/DataX/blob/master/hdfswriter/doc/hdfswriter.md)|
|              | Elasticsearch       |         |     √     |[写](https://github.com/userkdg/DataX/blob/master/elasticsearchwriter/doc/elasticsearchwriter.md)|
| 时间序列数据库 | OpenTSDB | √ |  |[读](https://github.com/userkdg/DataX/blob/master/opentsdbreader/doc/opentsdbreader.md)|
|  | TSDB | √ | √ |[读](https://github.com/userkdg/DataX/blob/master/tsdbreader/doc/tsdbreader.md) 、[写](https://github.com/userkdg/DataX/blob/master/tsdbwriter/doc/tsdbhttpwriter.md)|

# 阿里云DataWorks数据集成

目前DataX的已有能力已经全部融和进阿里云的数据集成，并且比DataX更加高效、安全，同时数据集成具备DataX不具备的其它高级特性和功能。可以理解为数据集成是DataX的全面升级的商业化用版本，为企业可以提供稳定、可靠、安全的数据传输服务。与DataX相比，数据集成主要有以下几大突出特点：

支持实时同步：

- 功能简介：https://help.aliyun.com/document_detail/181912.html
- 支持的数据源：https://help.aliyun.com/document_detail/146778.html
- 支持数据处理：https://help.aliyun.com/document_detail/146777.html

离线同步数据源种类大幅度扩充：

- 新增比如：DB2、Kafka、Hologres、MetaQ、SAPHANA、达梦等等，持续扩充中
- 离线同步支持的数据源：https://help.aliyun.com/document_detail/137670.html
- 具备同步解决方案：
    - 解决方案系统：https://help.aliyun.com/document_detail/171765.html
    - 一键全增量：https://help.aliyun.com/document_detail/175676.html
    - 整库迁移：https://help.aliyun.com/document_detail/137809.html
    - 批量上云：https://help.aliyun.com/document_detail/146671.html
    - 更新更多能力请访问：https://help.aliyun.com/document_detail/137663.html


# 我要开发新的插件

请点击：[DataX插件开发宝典](https://github.com/userkdg/DataX/blob/master/dataxPluginDev.md)


# 项目成员

核心Contributions: 言柏 、枕水、秋奇、青砾、一斅、云时

感谢天烬、光戈、祁然、巴真、静行对DataX做出的贡献。

# License

This software is free to use under the Apache License [Apache license](https://github.com/userkdg/DataX/blob/master/license.txt).
