package com.example.demo.config;

import com.example.demo.core.model.UserConfig;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import okhttp3.*;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
public class OkHttpProxyDispatcher {

    private static final int MAX_FAIL = 3;
    private static final long COOLDOWN_MS = 10 * 1000;  // 10秒冷却
    private static final int MAX_RETRY = 0;                 // 最多重试次数

    private final ConcurrentHashMap<String, ProxyState> proxyStateMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, OkHttpClient> clientMap = new ConcurrentHashMap<>();

    // 全局默认无代理的OkHttpClient，单例即可
    private final OkHttpClient defaultClient = new OkHttpClient.Builder()
            .cookieJar(new MemoryCookieJar())
            .followRedirects(false) // 禁用自动重定向，方便拿location
            .connectTimeout(10, TimeUnit.SECONDS)
            .readTimeout(10, TimeUnit.SECONDS)
            .callTimeout(10, TimeUnit.SECONDS)   // 整个调用最大超时
            .retryOnConnectionFailure(false)            // 🚫 禁用自动重试--开启（true）	网络不稳定、代理环境频繁断连、希望提升请求成功率///关闭（false）	业务请求非幂等、严格控制重试次数、希望错误直接抛出给业务层处理
//            .connectionPool(new ConnectionPool(20, 5, TimeUnit.MINUTES)) // 🚀 高并发支持
            .connectionPool(new ConnectionPool(0, 1, TimeUnit.SECONDS)) // 连接池大小为 0，避免复用
            .build();

    private OkHttpClient defaultClient() {
        return defaultClient;
    }

    // 根据账户唯一key获取或创建OkHttpClient
    public OkHttpClient getClient(UserConfig config) {
        String key = config.getProxyKey();

        // ✅ 无代理：直接返回默认客户端
        if ("no-proxy".equals(key)) {
            return defaultClient();
        }

        // ✅ SOCKS5 且带用户名密码认证：使用自定义 SocketFactory 生成 OkHttpClient
        if (config.getProxyType() == 2 && config.hasAuth()) {
            return clientMap.computeIfAbsent(key, k ->
                    Socks5ProxyClientBuilder.createSocks5Client(
                            config.getProxyHost(),
                            config.getProxyPort(),
                            config.getProxyUsername(),
                            config.getProxyPassword()
                    )
            );
        }

        // ✅ 其他（HTTP代理、SOCKS5无认证）
        return clientMap.computeIfAbsent(key, k -> {
            Proxy proxy = new Proxy(
                    config.getProxyType() == 1 ? Proxy.Type.HTTP : Proxy.Type.SOCKS,
                    new InetSocketAddress(config.getProxyHost(), config.getProxyPort())
            );

            OkHttpClient.Builder builder = new OkHttpClient.Builder()
                    .proxy(proxy)
                    .cookieJar(new MemoryCookieJar())
                    .followRedirects(false) // 禁用自动重定向，方便拿location
                    .connectTimeout(10, TimeUnit.SECONDS)
                    .readTimeout(10, TimeUnit.SECONDS)
                    .callTimeout(10, TimeUnit.SECONDS)   // 整个调用最大超时
                    .retryOnConnectionFailure(false)            // 🚫 禁用自动重试
                    //.connectionPool(new ConnectionPool(20, 5, TimeUnit.MINUTES)); // 🚀 高并发支持
                    .connectionPool(new ConnectionPool(0, 1, TimeUnit.SECONDS)); // 连接池大小为 0，避免复用


            // 仅 HTTP 代理认证支持
            if (config.getProxyType() == 1 && config.hasAuth()) {
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

    /**
     * 执行请求,并返回结果
     * @param method
     * @param url
     * @param body
     * @param headers
     * @param config
     * @param checkOnlyConnection   仅校验连通性
     * @return
     * @throws IOException
     */
    public HttpResult execute(String method,
                          String url,
                          String body,
                          Map<String, String> headers,
                          UserConfig config,
                          boolean checkOnlyConnection) throws IOException {

        String key = config.getProxyKey();
        ProxyState state = proxyStateMap.computeIfAbsent(key, k -> new ProxyState());

        for (int attempt = 0; attempt <= MAX_RETRY; attempt++) {
            // TODO 暂时不开启代理冷却功能
            /*if (!state.isAvailable()) {
                String msg = String.format("[OkHttpProxyDispatcher] 代理 %s 冷却中，拒绝请求", key);
                log.info(msg);
                throw new IOException(msg);
            }*/
            try {
                String proxyTypeStr = (config.getProxyType() == null || config.getProxyType() == 0) ? "无代理" : (config.getProxyType() == 1 ? "HTTP" : "SOCKS");
                log.info("[OkHttpProxyDispatcher] 尝试请求，方法={}，URL={}，代理={}[{}]，第{}次尝试",
                        method, url, key, proxyTypeStr, attempt + 1);

                OkHttpClient client = getClient(config);

                Request.Builder requestBuilder = new Request.Builder().url(url);
                if ("POST".equalsIgnoreCase(method)) {
                    String contentType = headers.getOrDefault("content-type", "application/json");
                    // 确保请求体 MIME 和 Header 一致
                    RequestBody requestBody = RequestBody.create(
                            StringUtils.isBlank(body) ? "" : body,
                            MediaType.parse(contentType)
                    );
                    // OkHttp 不自动设置 Content-Type 头，所以手动设置
                    requestBuilder.addHeader("Content-Type", contentType);
                    requestBuilder.post(requestBody);
                } else if ("GET".equalsIgnoreCase(method)) {
                    requestBuilder.get();
                } else {
                    throw new UnsupportedOperationException("仅支持 GET 和 POST 方法");
                }

                if (headers != null) {
                    headers.forEach(requestBuilder::addHeader);
                }

                // 添加默认伪装头（若 headers 中未指定）
                if (!requestBuilder.build().headers().names().contains("User-Agent")) {
                    requestBuilder.header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36");
                }
                if (!requestBuilder.build().headers().names().contains("Accept")) {
                    requestBuilder.header("Accept", "*/*");
                }
                // 默认 OkHttp 会自动添加并解压 gzip
                /*if (!requestBuilder.build().headers().names().contains("Accept-Encoding")) {
                    requestBuilder.header("Accept-Encoding", "gzip, deflate, br, zstd");
                }*/
                if (!requestBuilder.build().headers().names().contains("Accept-Language")) {
                    requestBuilder.header("Accept-Language", "zh-CN,zh;q=0.9");
                }

                long start = System.currentTimeMillis(); // ✅ 请求开始时间
                try (Response response = client.newCall(requestBuilder.build()).execute()) {
                    long cost = System.currentTimeMillis() - start; // ✅ 请求耗时
                    int code = response.code();
                    String respBody = response.body() != null ? response.body().string() : null;
                    Map<String, List<String>> respHeaders = response.headers().toMultimap();

                    // 提取特定 Cookie（示例，如果需要）
                    String cookieToken = null;
                    List<String> setCookies = respHeaders.get("Set-Cookie");
                    if (setCookies != null) {
                        for (String cookie : setCookies) {
                            if (cookie.startsWith("token=")) {
                                cookieToken = cookie;
                                break;
                            }
                        }
                    }

                    // ✅ 若是“仅校验连通性”，不做任何状态码校验
                    if (checkOnlyConnection) {
                        // 成功，重置失败计数
                        state.reset();
                        return new HttpResult(respBody, respHeaders, code, cookieToken, cost);
                    }
                    //if (code >= 200 && code < 300) {
                        // 成功，重置失败计数
                        state.reset();
                        log.info("[OkHttpProxyDispatcher] 请求成功，方法={}，URL={}，账户={}，代理={}[{}]，耗时={}m", method, url, config.getAccount(), key, config.getProxyType() == 1 ? "HTTP" : "SOCKS", cost);
                        return new HttpResult(respBody, respHeaders, code, cookieToken, cost);
                    /*} else {
                        throw new IOException("响应失败，状态码：" + code + "，响应体：" + respBody);
                    }*/
                }
            } catch (Exception e) {
                state.fail();
                log.warn("[OkHttpProxyDispatcher] 请求失败，方法={}，URL={}，账户={}，代理={}[{}]，失败次数={}/{}, 错误：{}",
                        method, url, config.getAccount(), key, config.getProxyType() == 1 ? "HTTP" : "SOCKS", state.getFailCount(), MAX_FAIL, e.getMessage());
                if (attempt == MAX_RETRY) {
                    throw new IOException("请求全部重试失败：" + e.getMessage(), e);
                }
                // 简单等待一下再重试，也可以根据需要调整策略
                /*try {
                    Thread.sleep(200);
                } catch (InterruptedException ignored) {}*/
            }
        }
        throw new IOException("请求执行失败，未命中任何有效结果");
    }

    /**
     * 发起请求，返回完整响应信息（包括状态码、响应头、body、cookie等）
     * @param method
     * @param url
     * @param body
     * @param headers
     * @param config
     * @return
     * @throws IOException
     */
    public HttpResult executeFull(String method,
                                  String url,
                                  String body,
                                  Map<String, String> headers,
                                  UserConfig config) throws IOException {

        String key = config.getProxyKey();
        ProxyState state = proxyStateMap.computeIfAbsent(key, k -> new ProxyState());

        for (int attempt = 0; attempt <= MAX_RETRY; attempt++) {
            /*if (!state.isAvailable()) {
                String msg = String.format("[OkHttpProxyDispatcher] 代理 %s 冷却中，拒绝请求", key);
                log.info(msg);
                throw new IOException(msg);
            }*/
            try {
                String proxyTypeStr = (config.getProxyType() == null || config.getProxyType() == 0) ? "无代理" : (config.getProxyType() == 1 ? "HTTP" : "SOCKS");
                log.info("[OkHttpProxyDispatcher] 尝试请求，方法={}，URL={}，代理={}[{}]，第{}次尝试",
                        method, url, key, proxyTypeStr, attempt + 1);

                OkHttpClient client = getClient(config);

                Request.Builder requestBuilder = new Request.Builder().url(url);
                if ("POST".equalsIgnoreCase(method)) {
                    String contentType = headers.getOrDefault("content-type", "application/json");
                    RequestBody requestBody = RequestBody.create(
                            StringUtils.isBlank(body) ? "" : body,
                            MediaType.parse(contentType)
                    );
                    requestBuilder.addHeader("Content-Type", contentType);
                    requestBuilder.post(requestBody);
                } else if ("GET".equalsIgnoreCase(method)) {
                    requestBuilder.get();
                } else {
                    throw new UnsupportedOperationException("仅支持 GET 和 POST 方法");
                }

                if (headers != null) {
                    headers.forEach(requestBuilder::addHeader);
                }

                // 添加默认伪装头（若 headers 中未指定）
                if (!requestBuilder.build().headers().names().contains("User-Agent")) {
                    requestBuilder.header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36");
                }
                if (!requestBuilder.build().headers().names().contains("Accept")) {
                    requestBuilder.header("Accept", "*/*");
                }
                // 默认 OkHttp 会自动添加并解压 gzip
                /*if (!requestBuilder.build().headers().names().contains("Accept-Encoding")) {
                    requestBuilder.header("Accept-Encoding", "gzip, deflate, br, zstd");
                }*/
                if (!requestBuilder.build().headers().names().contains("Accept-Language")) {
                    requestBuilder.header("Accept-Language", "zh-CN,zh;q=0.9");
                }

                long start = System.currentTimeMillis(); // ✅ 请求开始时间
                try (Response response = client.newCall(requestBuilder.build()).execute()) {
                    long cost = System.currentTimeMillis() - start; // ✅ 请求耗时
                    int code = response.code();
                    String respBody = response.body() != null ? response.body().string() : "";
                    Headers respHeaders = response.headers();

                    //if (code >= 200 && code < 400) {  // 允许200~399范围，因为302要拿location
                        state.reset();

                        // 解析 Set-Cookie 里的 token（示例只找名为 "token" 的cookie）
                        String cookieToken = null;
                        List<String> cookies = respHeaders.values("Set-Cookie");
                        if (cookies != null) {
                            for (String cookie : cookies) {
                                if (cookie.startsWith("token=")) {
                                    cookieToken = cookie.split(";")[0].substring("token=".length());
                                    break;
                                }
                            }
                        }

                        log.info("[OkHttpProxyDispatcher] 请求成功，方法={}，URL={}，代理={}[{}]", method, url, key, config.getProxyType() == 1 ? "HTTP" : "SOCKS");
                        return new HttpResult(respBody, respHeaders.toMultimap(), code, cookieToken, cost);
                    /*} else {
                        throw new IOException("响应失败，状态码：" + code + "，响应体：" + respBody);
                    }*/
                }
            } catch (Exception e) {
                state.fail();
                log.warn("[OkHttpProxyDispatcher] 请求失败，方法={}，URL={}，代理={}[{}]，失败次数={}/{}, 错误：{}",
                        method, url, key, config.getProxyType() == 1 ? "HTTP" : "SOCKS", state.getFailCount(), MAX_FAIL, e.getMessage());
                if (attempt == MAX_RETRY) {
                    throw new IOException("请求全部重试失败：" + e.getMessage(), e);
                }
                try {
                    Thread.sleep(200);
                } catch (InterruptedException ignored) {}
            }
        }
        throw new IOException("请求执行失败，未命中任何有效结果");
    }
    /**
     * 执行请求，返回图片字节数组
     * @param method GET/POST，一般请求验证码用GET
     * @param url 请求地址
     * @param body 请求体，图片请求一般不带，传null或空字符串
     * @param headers 自定义请求头
     * @param config 代理配置等
     * @return 图片二进制
     * @throws IOException 请求失败时抛出
     */
    public ImageResult executeImage(String method,
                                    String url,
                                    String body,
                                    Map<String, String> headers,
                                    UserConfig config) throws IOException {

        String key = config.getProxyKey();
        ProxyState state = proxyStateMap.computeIfAbsent(key, k -> new ProxyState());

        for (int attempt = 0; attempt <= MAX_RETRY; attempt++) {
            /*if (!state.isAvailable()) {
                String msg = String.format("[OkHttpProxyDispatcher] 代理 %s 冷却中，拒绝请求", key);
                log.info(msg);
                throw new IOException(msg);
            }*/
            try {
                String proxyTypeStr = (config.getProxyType() == null || config.getProxyType() == 0) ? "无代理" : (config.getProxyType() == 1 ? "HTTP" : "SOCKS");
                log.info("[OkHttpProxyDispatcher] 尝试请求，方法={}，URL={}，代理={}[{}]，第{}次尝试",
                        method, url, key, proxyTypeStr, attempt + 1);

                OkHttpClient client = getClient(config);

                Request.Builder requestBuilder = new Request.Builder().url(url);
                if ("POST".equalsIgnoreCase(method)) {
                    String contentType = headers.getOrDefault("content-type", "application/json");
                    RequestBody requestBody = RequestBody.create(
                            StringUtils.isBlank(body) ? "" : body,
                            MediaType.parse(contentType)
                    );
                    requestBuilder.addHeader("Content-Type", contentType);
                    requestBuilder.post(requestBody);
                } else if ("GET".equalsIgnoreCase(method)) {
                    requestBuilder.get();
                } else {
                    throw new UnsupportedOperationException("仅支持 GET 和 POST 方法");
                }

                if (headers != null) {
                    headers.forEach(requestBuilder::addHeader);
                }

                // 添加默认伪装头（若 headers 中未指定）
                if (!requestBuilder.build().headers().names().contains("User-Agent")) {
                    requestBuilder.header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36");
                }
                if (!requestBuilder.build().headers().names().contains("Accept")) {
                    requestBuilder.header("Accept", "*/*");
                }
                if (!requestBuilder.build().headers().names().contains("Accept-Encoding")) {
                    requestBuilder.header("Accept-Encoding", "identity"); // 禁用 GZIP 解码，保证图片完整
                }
                if (!requestBuilder.build().headers().names().contains("Accept-Language")) {
                    requestBuilder.header("Accept-Language", "zh-CN,zh;q=0.9");
                }

                try (Response response = client.newCall(requestBuilder.build()).execute()) {
                    int code = response.code();
                    ResponseBody responseBody = response.body();

                    if (responseBody != null) {
                        byte[] bytes = responseBody.bytes();
                        if (bytes.length == 0) {
                            throw new IOException("响应体为空");
                        }
                        state.reset();
                        log.info("[OkHttpProxyDispatcher] 请求成功，方法={}，URL={}，代理={}[{}]", method, url, key, config.getProxyType() == 1 ? "HTTP" : "SOCKS");

                        ImageResult result = new ImageResult();
                        result.setStatus(code);
                        result.setBody(bytes);
                        result.setHeaders(response.headers().toMultimap());
                        return result;
                    } else {
                        String respStr = responseBody != null ? responseBody.string() : "";
                        throw new IOException("响应失败，状态码：" + code + "，响应体：" + respStr);
                    }
                }
            } catch (Exception e) {
                state.fail();
                log.warn("[OkHttpProxyDispatcher] 请求失败，方法={}，URL={}，代理={}[{}]，失败次数={}/{}, 错误：{}",
                        method, url, key, config.getProxyType() == 1 ? "HTTP" : "SOCKS", state.getFailCount(), MAX_FAIL, e.getMessage());
                if (attempt == MAX_RETRY) {
                    throw new IOException("请求全部重试失败：" + e.getMessage(), e);
                }
                try {
                    Thread.sleep(200);
                } catch (InterruptedException ignored) {}
            }
        }
        throw new IOException("请求执行失败，未命中任何有效结果");
    }

    @Data
    @AllArgsConstructor
    public static class HttpResult {
        private String body;
        private Map<String, List<String>> headers;
        private int status;
        private String cookieToken;     // 如果需要提取特定 Cookie
        private final long durationMs;  // 耗时
    }
    @Data
    public class ImageResult {
        private int status;
        private byte[] body;
        private Map<String, List<String>> headers;
    }
    private static class ProxyState {
        private int failCount = 0;
        private long lastFailTime = 0;

        synchronized void fail() {
            failCount++;
            lastFailTime = System.currentTimeMillis();
        }

        synchronized void reset() {
            failCount = 0;
            lastFailTime = 0;
        }

        synchronized boolean isAvailable() {
            if (failCount < MAX_FAIL) {
                return true;
            }
            long elapsed = System.currentTimeMillis() - lastFailTime;
            return elapsed > COOLDOWN_MS;
        }

        synchronized int getFailCount() {
            return failCount;
        }
    }

}


