# hdfs操作工具类

作者：掌少

## 1. 概述

支持上传、下载、删除等操作

## 2. 使用方法

## 2.1 导入依赖包

    compile "com.easysoft:spring-boot-starter-hdfs:xxx"

## 2.2 配置

	spring:
      hadoop:
        path: hdfs://172.16.155.92:8020
        user: hadoop

### 2.3 使用方法

    ......
    @Autowired
    private IHdfsClient hdfsClient;
    ......
