package com.example.demo.config;

import cn.hutool.core.util.StrUtil;
import org.apache.commons.lang3.StringUtils;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.Config;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

/**
 * @description Redisson配置
 * @author 谢诗宏
 * @date 2024年11月20日
 */
@Configuration
public class RedissonConfig {

    @Value("${spring.data.redis.host}")
    public String host;

    @Value("${spring.data.redis.port}")
    public Integer port;

    @Value("${spring.data.redis.password}")
    public String password;

    @Value("${spring.data.redis.database}")
    public Integer database;

    @Value("${redis.database.businessUser}")
    public Integer businessUser;
    /**
     * 创建默认的 Redisson 实例
     */
    @Bean
    @Primary
    public RedissonClient defaultRedissonClient() throws IOException {
        Config config = createRedissonConfig(database);
        return Redisson.create(config);
    }

    /**
     * 创建业务A的 Redisson 实例，连接数据库 1
     */
    @Bean
    public RedissonClient businessUserRedissonClient() throws IOException {
        Config config = createRedissonConfig(businessUser);  // 业务A数据库 1
        return Redisson.create(config);
    }

    /**
     * 默认redisson实例
     */
    public Config createRedissonConfig(int database) throws IOException {
        Config config = Config.fromYAML(RedissonConfig.class.getClassLoader().getResource("redisson-config.yml"));
        config.useSingleServer()
                .setAddress("redis://" + host + ":" + port)
                .setPassword(StringUtils.isNotBlank(password) ? password : null)
                .setDatabase(database);
        config.setCodec(new StringCodec());
        return config;
    }

}