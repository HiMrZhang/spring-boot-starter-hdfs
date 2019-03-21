package com.easysoft.hdfs.autoconfigure;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "spring.hadoop")
public class HdfsProperties {
    private String path;
    private String user;

}
