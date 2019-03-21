package com.easysoft.hdfs;

import com.easysoft.hdfs.autoconfigure.HdfsProperties;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.util.FileCopyUtils;

import java.io.*;
import java.net.URI;
@Slf4j
public class HdfsClient implements Closeable, DisposableBean, IHdfsClient {
    public HdfsClient(HdfsProperties hdfsProperties) {
        this.hdfsProperties = hdfsProperties;
        try {
            Configuration configuration = new Configuration();
            fs = FileSystem.get(new URI(hdfsProperties.getPath()), configuration, hdfsProperties.getUser());
        } catch (Exception e) {
            throw new HadoopException("Cannot create operater" + e.getMessage(), e);
        }
    }

    private FileSystem fs;
    private HdfsProperties hdfsProperties;

    @Override
    public void close() throws IOException {
        if (this.fs != null) {
            log.debug("hdfs fileSystem close!");
            this.fs.close();
            this.fs = null;
        }
    }

    @Override
    public void destroy() throws Exception {
        this.close();
    }

    @Override
    public String cat(String filePath) {
        // 读取文件
        FSDataInputStream in = null;
        String content = null;
        try {
            in = fs.open(new Path(filePath));
            content = getContent(in);
        } catch (IOException e) {
            throw new HadoopException("No such file or directory " + filePath, e);
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            return content;
        }

    }

    private String getContent(InputStream in) throws IOException {
        StringWriter writer = new StringWriter(in.available());
        InputStreamReader reader = new InputStreamReader(in, "UTF-8");
        FileCopyUtils.copy(reader, writer);
        return writer.toString();
    }

    @Override
    public void mkdir(String dirPath) {
        try {
            Path p = new Path(dirPath);
            FileStatus fstatus = null;

            try {
                fstatus = fs.getFileStatus(p);
                if (fstatus.isDirectory()) {
                    throw new IllegalStateException("Cannot create directory " + dirPath + ": File exists");
                }

                throw new IllegalStateException(dirPath + " exists but is not a directory");
            } catch (FileNotFoundException e) {
                if (!fs.mkdirs(p)) {
                    throw new HadoopException("Failed to create " + dirPath);
                }
            }
        } catch (IOException e) {
            throw new HadoopException("Cannot create directory " + e.getMessage(), e);
        }
    }

    /**
     * 重命名文件
     */
    @Override
    public void rename(String oldPath, String newPath) {
        try {
            Path oldP = new Path(oldPath);
            Path newP = new Path(newPath);
            if (!fs.rename(oldP, newP)) {
                FileStatus srcFstatus = null;
                FileStatus dstFstatus = null;

                try {
                    srcFstatus = fs.getFileStatus(oldP);
                } catch (FileNotFoundException var23) {
                }

                try {
                    dstFstatus = fs.getFileStatus(newP);
                } catch (IOException var22) {
                }

                if (srcFstatus != null && dstFstatus != null && srcFstatus.isDirectory() && !dstFstatus.isDirectory()) {
                    throw new HadoopException("cannot overwrite non directory " + newPath + " with directory " + oldPath);
                }

                throw new HadoopException("Failed to rename " + oldPath + " to " + newPath);
            }
        } catch (IOException e) {
            throw new HadoopException("Cannot rename resources " + e.getMessage(), e);
        }
    }

    /**
     * 删除文件
     *
     * @throws Exception
     */
    @Override
    public void delete(String filePath) {

        try {
            // 第二个参数指定是否要递归删除，false=否，true=是
            Path p = new Path(filePath);
            if (!fs.delete(p, false)) {
                FileStatus status = fs.getFileStatus(p);
                if (status.isDirectory()) {
                    throw new IllegalStateException("Cannot remove directory \"" + p + "\", if recursive deletion was not specified");
                }
            }
        } catch (IOException e) {
            throw new HadoopException("Cannot delete  resources " + e.getMessage(), e);
        }
    }

    /**
     * 上传本地文件到HDFS
     */
    @Override
    public void uploadLocalFile(String localPath, String hdfsPath) {
        // 第一个参数是本地文件的路径，第二个则是HDFS的路径
        try {
            fs.copyFromLocalFile(new Path(localPath), new Path(hdfsPath));
        } catch (IOException e) {
            throw new HadoopException("Cannot upload resources " + e.getMessage(), e);
        }
    }


    /**
     * 下载HDFS文件
     */
    @Override
    public void downloadToLocalFile(String localPath, String hdfsPath) {
        try {
            FSDataInputStream in = fs.open(new Path(hdfsPath));
            OutputStream outputStream = new FileOutputStream(new File(localPath));
            IOUtils.copyBytes(in, outputStream, 1024);
            in.close();
            outputStream.close();
        } catch (IOException e) {
            throw new HadoopException("Cannot download resources " + e.getMessage(), e);
        }
    }

    @Override
    public FileStatus[] ls(String dirPath) {
        try {
            return fs.listStatus(new Path(dirPath));
        } catch (IOException e) {
            throw new HadoopException("Cannot ls resources " + e.getMessage(), e);
        }
    }

    @Override
    public void create(InputStream inputStream, String hdfsPath) {
        FSDataOutputStream outputStream = null;
        try {
            // 创建文件
            outputStream = fs.create(new Path(hdfsPath));
            byte[] buffer = new byte[1024];
            int byteread = 0;
            while ((byteread = inputStream.read(buffer)) != -1) {
                // 写入内容到文件中
                outputStream.write(buffer, 0, byteread);
            }
        } catch (IOException e) {
            throw new HadoopException("Cannot create resources " + e.getMessage(), e);
        } finally {
            if (outputStream != null) {
                try {
                    outputStream.flush();
                    outputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        }

    }
}
