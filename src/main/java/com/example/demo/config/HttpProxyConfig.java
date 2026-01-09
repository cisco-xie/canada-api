package com.example.demo.config;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.http.HttpRequest;
import com.example.demo.core.model.UserConfig;
import lombok.extern.slf4j.Slf4j;
import okhttp3.ConnectionPool;
import okhttp3.Credentials;
import okhttp3.OkHttpClient;
import org.apache.commons.lang3.StringUtils;

import java.net.*;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * 代理封装配置
 */
@Slf4j
public class HttpProxyConfig {

    /**
     * 封装代理
     * @param request
     * @param userConfig
     */
    public static void configureProxy(HttpRequest request, UserConfig userConfig) {
        if (BeanUtil.isNotEmpty(userConfig) && null != userConfig.getProxyType() && 0 != userConfig.getProxyType()) {
            // 统一设置代理认证
            if (StringUtils.isNotBlank(userConfig.getProxyUsername()) && StringUtils.isNotBlank(userConfig.getProxyPassword())) {
                Authenticator.setDefault(new Authenticator() {
                    @Override
                    protected PasswordAuthentication getPasswordAuthentication() {
                        return new PasswordAuthentication(
                                userConfig.getProxyUsername(),
                                userConfig.getProxyPassword().toCharArray()
                        );
                    }
                });
            }

            // 设置代理主机和端口
            if (userConfig.getProxyType() == 1) {
                setHttpProxy(userConfig);
            } else if (userConfig.getProxyType() == 2) {
                setSocksProxy(userConfig);
            }

            // 禁用默认的 HTTP 认证隧道设置（针对 SOCKS 代理）
            System.setProperty("jdk.http.auth.tunneling.disabledSchemes", "");

            // 设置代理类型
            Proxy proxy = new Proxy(
                    userConfig.getProxyType() == 1 ? Proxy.Type.HTTP : Proxy.Type.SOCKS,
                    new InetSocketAddress(userConfig.getProxyHost(), userConfig.getProxyPort())
            );
            request.setProxy(proxy);
        }
    }

    private static void setHttpProxy(UserConfig userConfig) {
        System.setProperty("http.proxyHost", userConfig.getProxyHost());
        System.setProperty("http.proxyPort", String.valueOf(userConfig.getProxyPort()));
        System.setProperty("https.proxyHost", userConfig.getProxyHost());
        System.setProperty("https.proxyPort", String.valueOf(userConfig.getProxyPort()));
    }

    private static void setSocksProxy(UserConfig userConfig) {
        System.setProperty("socksProxyHost", userConfig.getProxyHost());
        System.setProperty("socksProxyPort", String.valueOf(userConfig.getProxyPort()));
    }

    private final Map<String, OkHttpClient> clientMap = new ConcurrentHashMap<>();

    /**
     * okHttp 模式 代理请求
     * @param config
     * @return
     */
    public OkHttpClient getClient(UserConfig config) {
        return clientMap.computeIfAbsent(config.getProxyKey(), k -> {
            Proxy proxy = new Proxy(
                    config.getProxyType() == 1 ? Proxy.Type.HTTP : Proxy.Type.SOCKS,
                    new InetSocketAddress(config.getProxyHost(), config.getProxyPort())
            );

            OkHttpClient.Builder builder = new OkHttpClient.Builder()
                    .proxy(proxy)
                    .connectTimeout(10, TimeUnit.SECONDS)
                    .readTimeout(10, TimeUnit.SECONDS)
                    .retryOnConnectionFailure(false) // 🚫 禁用自动重试
                    .connectionPool(new ConnectionPool(20, 5, TimeUnit.MINUTES)); // 🚀 高并发支持

            if (config.hasAuth()) {
                builder.proxyAuthenticator((route, response) -> {
                    String credential = Credentials.basic(config.getProxyUsername(), config.getProxyPassword());
                    return response.request().newBuilder()
                            .header("Proxy-Authorization", credential)
                            .build();
                });
            }

            return builder.build();
        });
    }

}
