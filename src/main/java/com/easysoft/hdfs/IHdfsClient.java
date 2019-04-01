package com.easysoft.hdfs;

import org.apache.hadoop.fs.FileStatus;

import java.io.InputStream;
import java.io.OutputStream;

public interface IHdfsClient {
    /**
     * 查看内容
     *
     * @param filePath
     * @return
     */
    String cat(String filePath);

    /**
     * 创建目录
     *
     * @param dirPath
     */
    void mkdir(String dirPath);

    /**
     * 重命名文件
     */

    void rename(String oldPath, String newPath);

    /**
     * 删除文件
     *
     * @throws Exception
     */

    void delete(String filePath);

    /**
     * 上传本地文件到HDFS
     */

    void uploadLocalFile(String localPath, String hdfsPath);


    /**
     * 下载HDFS文件
     */

    void downloadToLocalFile(String localPath, String hdfsPath);

    /**
     * 下载HDFS文件
     *
     * @param outputStream
     * @param hdfsPath
     */
    void download(OutputStream outputStream, String hdfsPath);

    /**
     * 查看某个目录下所有的文件
     *
     * @param dirPath
     * @return
     */
    FileStatus[] ls(String dirPath);

    /**
     * 创建文件
     *
     * @param inputStream
     * @param hdfsPath
     */
    void create(InputStream inputStream, String hdfsPath);

    /**
     * 获取文件大小
     * @param hdfsPath
     * @return
     */
    Long getFileSize(String hdfsPath);
}
