package com.easysoft.hdfs.autoconfigure;

import com.easysoft.hdfs.HdfsClient;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnMissingBean({HdfsClient.class})
@EnableConfigurationProperties(HdfsProperties.class)
public class HdfsClientAutoConfiguration {
    @Bean
    @ConditionalOnMissingBean(HdfsClient.class)
    public HdfsClient hdfsClient(HdfsProperties hdfsProperties) {
        return new HdfsClient(hdfsProperties);
    }
}
