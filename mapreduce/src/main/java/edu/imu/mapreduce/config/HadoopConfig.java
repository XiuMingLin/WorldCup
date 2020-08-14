package edu.imu.mapreduce.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.URI;

@Configuration
@ConditionalOnProperty(name="hadoop.name-node")
@Slf4j
public class HadoopConfig {

    @Value("${hadoop.name-node}")
    private String nameNode;

    @Bean("fileSystem")
    public FileSystem createFs(){
        //读取配置文件
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        conf.set("dfs.replication", "1");
        // 文件系统
        FileSystem fs = null;
        // 返回指定的文件系统
        try {
            URI uri = new URI(nameNode.trim());
            fs = FileSystem.get(uri,conf);
            log.info("Success");
        } catch (Exception e) {
            log.error("", e);
        }
        return  fs;
    }
}