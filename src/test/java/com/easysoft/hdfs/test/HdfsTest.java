package com.easysoft.hdfs.test;

import com.easysoft.hdfs.IHdfsClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileStatus;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.FileInputStream;

@RunWith(SpringRunner.class)
@SpringBootTest
@Slf4j
public class HdfsTest {

    @Autowired
    private IHdfsClient hdfsClient;

    @Test
    public void mkdir() {
        hdfsClient.mkdir("/file");
    }


    @Test()
    public void uploadLocalFile() {
        hdfsClient.uploadLocalFile("README.md", "/file/README.md");
    }

    @Test
    public void testCat() {
        log.info(hdfsClient.cat("/file/README.md"));
    }

    @Test
    public void downloadToLocalFile() {
        hdfsClient.downloadToLocalFile("/Users/zhangyunpeng/README.md", "/file/README.md");
    }

    @Test
    public void create() throws Exception {
        hdfsClient.create(new FileInputStream("checkStyle.xml"), "/file/checkStyle.xml");
    }

    @Test
    public void rename() {
        hdfsClient.rename("/file/README.md", "/file/README.md.bak");
    }

    @Test
    public void ls() {
        FileStatus[] fileStatuses = hdfsClient.ls("/file");
        for (FileStatus fileStatus : fileStatuses) {
            log.info("这是一个：" + (fileStatus.isDirectory() ? "文件夹" : "文件"));
            log.info("副本系数：" + fileStatus.getReplication());
            log.info("大小：" + fileStatus.getLen());
            log.info("路径：" + fileStatus.getPath() + "\n");
        }
    }

    @Test
    public void delete() {
        hdfsClient.delete("/file/README.md.bak");
        hdfsClient.delete("/file/checkStyle.xml");
    }
    @Test
    public void getFileSize() {
        log.info(hdfsClient.getFileSize("/soft/hive-1.1.0-cdh5.7.0.tar.gz").toString());
    }

}
