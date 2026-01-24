package com.example.demo.api;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.date.LocalDateTimeUtil;
import cn.hutool.core.date.TimeInterval;
import cn.hutool.core.thread.ThreadUtil;
import cn.hutool.core.util.IdUtil;
import cn.hutool.core.util.NumberUtil;
import cn.hutool.core.util.RandomUtil;
import cn.hutool.core.util.XmlUtil;
import cn.hutool.http.HttpRequest;
import cn.hutool.http.HttpResponse;
import cn.hutool.json.JSONArray;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import cn.hutool.jwt.JWTUtil;
import com.example.demo.common.constants.RedisConstants;
import com.example.demo.common.enmu.GameType;
import com.example.demo.common.enmu.SystemError;
import com.example.demo.common.utils.CaptchaSolver;
import com.example.demo.common.utils.KeyUtil;
import com.example.demo.common.utils.ToDayRangeUtil;
import com.example.demo.config.OkHttpProxyDispatcher;
import com.example.demo.config.PriorityTaskExecutor;
import com.example.demo.core.exception.BusinessException;
import com.example.demo.core.model.UserConfig;
import com.example.demo.model.dto.AdminLoginDTO;
import com.example.demo.model.dto.LoginDTO;
import com.example.demo.model.vo.*;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.redisson.api.*;
import org.springframework.stereotype.Component;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Slf4j
@Component
public class HandicapApi {

    @Resource(name = "defaultRedissonClient")
    private RedissonClient redisson;

    @Resource(name = "businessUserRedissonClient")
    private RedissonClient businessUserRedissonClient;

    @Resource
    private ConfigService configService;

    @Resource
    private OkHttpProxyDispatcher dispatcher;

    // 初始化本地缓存赔率数据，设置为写入后 10 分钟过期
    private final Cache<String, String> localOddsCache = Caffeine.newBuilder()
            .expireAfterWrite(10, TimeUnit.MINUTES)
            .maximumSize(5000) // 限制缓存最多存储 5000 项
            .build();


    /**
     * 设置账号代理
     * @param userProxy
     */
    public void config(ConfigUserVO userProxy) {
        UserConfig proxy = BeanUtil.copyProperties(userProxy, UserConfig.class);
        if (BeanUtil.isNotEmpty(proxy)) {
            businessUserRedissonClient.getBucket(KeyUtil.genKey(RedisConstants.USER_PROXY_PREFIX, userProxy.getAccount())).set(JSONUtil.toJsonStr(proxy));
        }
    }

    /**
     * 验证码二值化 + 去噪 提供识别率
     * @param src
     * @return
     */
    private BufferedImage preprocess(BufferedImage src) {

        int w = src.getWidth();
        int h = src.getHeight();

        BufferedImage result = new BufferedImage(w, h, BufferedImage.TYPE_INT_RGB);

        for (int x = 0; x < w; x++) {
            for (int y = 0; y < h; y++) {
                Color c = new Color(src.getRGB(x, y));
                int gray = (c.getRed() + c.getGreen() + c.getBlue()) / 3;

                // 拉开对比度（核心）
                int v;
                if (gray < 100) {
                    v = 0;
                } else if (gray > 200) {
                    v = 255;
                } else {
                    v = gray;
                }

                result.setRGB(x, y, new Color(v, v, v).getRGB());
            }
        }
        return result;
    }

    /**
     * OCR 结果清洗
     * @param s
     * @return
     */
    private String clean(String s) {
        if (s == null) return "";
        return s.replaceAll("\\s+", "").replaceAll("[^0-9]", "");
    }

    /**
     * 选择最合理结果
     * @param a
     * @param b
     * @return
     */
    private String pickBest(String a, String b) {
        if (a.length() == 4 && NumberUtil.isNumber(a)) return a;
        if (b.length() == 4 && NumberUtil.isNumber(b)) return b;
        return null;
    }

    /**
     * 获取登录验证码信息
     *
     * @return 验证码id
     */
    public Map<String, String> code(UserConfig userConfig) {
        Map<String, String> resultMap = new HashMap<>();
        Map<String, String> headers = new HashMap<>();
        headers.put("sec-fetch-user", "?1");
        headers.put("upgrade-insecure-requests", "1");

        int maxRetries = 1;

        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                String uid = getUid(userConfig);
                if (StringUtils.isBlank(uid)) {
                    throw new BusinessException(SystemError.USER_1016);
                }
                String url = userConfig.getBaseUrl() + "seccode.php?uid=" + uid;
                log.info("尝试请求验证码: 第{}次尝试", attempt);

                OkHttpProxyDispatcher.ImageResult result =
                        dispatcher.executeImage("GET", url, null, headers, userConfig);

                int status = result.getStatus();
                Map<String, List<String>> respHeaders = result.getHeaders();
                byte[] imageBytes = result.getBody();

                if (status != 200 || imageBytes == null || imageBytes.length == 0) {
                    log.error("验证码请求失败，状态码={}, bodySize={}", status,
                            imageBytes == null ? 0 : imageBytes.length);
                    continue;
                }

                // Content-Type 校验
                String contentType = "";
                List<String> ctList = respHeaders.get("Content-Type");
                if (ctList != null && !ctList.isEmpty()) {
                    contentType = ctList.get(0);
                }

                if (contentType.contains("text")) {
                    String text = new String(imageBytes, StandardCharsets.UTF_8);
                    log.error("验证码返回文本内容: {}", text);
                    if (text.contains("416 Range Not Satisfiable")) {
                        resultMap.put("code", "0000");
                        return resultMap;
                    }
                    continue;
                }

                // 🔥 使用纯 Java 模板匹配
                String code = CaptchaSolver.solveCaptcha(imageBytes);

                resultMap.put("uid", uid);
                resultMap.put("code", code);
                return resultMap;

            } catch (Exception e) {
                Throwable cause = e.getCause();
                if (attempt == maxRetries) {
                    log.error("验证码达到最大重试次数，抛出异常", e);
                    throw new BusinessException(SystemError.USER_1009,
                            userConfig.getAccount(), e);
                }

                if (cause instanceof UnknownHostException) {
                    throw new BusinessException(SystemError.SYS_419,
                            userConfig.getAccount(), "代理主机未知", e);
                } else if (cause instanceof ConnectException) {
                    throw new BusinessException(SystemError.SYS_419,
                            userConfig.getAccount(), "代理连接异常", e);
                } else if (cause instanceof SocketTimeoutException) {
                    throw new BusinessException(SystemError.SYS_419,
                            userConfig.getAccount(), "代理连接超时", e);
                } else if (cause instanceof IOException) {
                    throw new BusinessException(SystemError.SYS_419,
                            userConfig.getAccount(), "代理IO异常", e);
                } else {
                    throw new BusinessException(SystemError.SYS_419,
                            userConfig.getAccount(), "代理未知异常", e);
                }
            }
        }

        resultMap.put("code", "0000");
        return resultMap;
    }


    /**
     * 登录
     *
     * @return token
     */
    public String login(String params, String uuid) {
        String url = "https://3575978705.tcrax4d8j.com/login";
        Map<String, String> headers = new HashMap<>();
        headers.put("accept", "*/*");
        headers.put("accept-language", "zh-CN,zh;q=0.9");
        headers.put("content-type", "application/x-www-form-urlencoded");
        HttpResponse resultRes = HttpRequest.post(url)
                .addHeaders(headers)
                .cookie("2a29530a2306=" + uuid)
                .body(params)
                .execute();
        return resultRes.getCookieValue("token");
    }

    /**
     * 批量登录
     *
     * @return token
     */
//    public List<LoginDTO> batchLogin(List<LoginVO> logins) {
//        List<LoginDTO> tokens = new ArrayList<>();
//        final LoginDTO[] loginDTO = {null};
//        logins.forEach(login -> {
//            String token = null;
//            loginDTO[0] = new LoginDTO();
//            loginDTO[0].setAccount(login.getAccount());
//            int retryCount = 0; // 重试次数计数
//            final int maxRetries = 3; // 最大重试次数
//
//            String url = "https://3575978705.tcrax4d8j.com/login";
//            while (retryCount < maxRetries) {
//                try {
//                    Map<String, String> headers = new HashMap<>();
//                    headers.put("accept", "*/*");
//                    headers.put("accept-language", "zh-CN,zh;q=0.9");
//                    headers.put("content-type", "application/x-www-form-urlencoded");
//                    String uuid = IdUtil.randomUUID();
//                    String params = "type=1&account=" + login.getAccount() + "&password=" + login.getPassword() + "&code=" + code(uuid);
//                    HttpResponse resultRes = HttpRequest.post(url)
//                            .addHeaders(headers)
//                            .cookie("2a29530a2306=" + uuid)
//                            .body(params)
//                            .execute();
//                    token = resultRes.getCookieValue("token");
//                    // 如果成功获取到 token，跳出重试循环
//                    if (!token.isBlank()) {
//                        break;
//                    }
//                } catch (Exception e) {
//                    // 捕获异常并打印日志
//                    log.error("账号 {} 获取 token 时出现异常: {}", login.getAccount(), e.getMessage());
//                }
//                retryCount++; // 增加重试次数
//
//                // 在重试前添加短暂延迟，避免过快重复调用
//                try {
//                    Thread.sleep(300); // 延迟 300 毫秒
//                } catch (InterruptedException ex) {
//                    Thread.currentThread().interrupt();
//                }
//            }
//
//            // 如果达到最大重试次数仍未获取到 token，添加占位信息或错误标记
//            if (token == null || token.isEmpty()) {
//                log.warn("账号 {} 获取 token 失败，已达到最大重试次数", login.getAccount());
//                // token = "FAILED";
//            }
//            loginDTO[0].setToken(token);
//            tokens.add(loginDTO[0]);
//        });
//
//        return tokens;
//    }

    public AdminLoginDTO adminLogin(AdminLoginVO login) {

        // Redis 键值
        String redisKey = KeyUtil.genKey(RedisConstants.USER_ADMIN_PREFIX, login.getUsername());

        // 判断 Redis 中是否存在该用户数据
        boolean exists = businessUserRedissonClient.getBucket(redisKey).isExists();
        if (exists) {
            AdminLoginDTO adminLogin = JSONUtil.toBean(JSONUtil.parseObj(businessUserRedissonClient.getBucket(redisKey).get()), AdminLoginDTO.class);
            if (StringUtils.equals(login.getPassword(), adminLogin.getPassword())) {
                String token = JWTUtil.createToken(BeanUtil.beanToMap(adminLogin), "admin".getBytes());
                adminLogin.setAccessToken(token);
                adminLogin.setRefreshToken(token);
                return adminLogin;
            } else {
                throw new BusinessException(SystemError.USER_1008);
            }
        } else {
            throw new BusinessException(SystemError.USER_1008);
        }
    }

    /**
     * 单账号下线
     * @param username
     * @param id
     */
    public void singleLoginOut(String username, String id) {
        UserConfig userConfig = JSONUtil.toBean(JSONUtil.parseObj(businessUserRedissonClient.getBucket(KeyUtil.genKey(RedisConstants.USER_PROXY_PREFIX, username, id)).get()), UserConfig.class);
        if (BeanUtil.isNotEmpty(userConfig)) {
            userConfig.setToken("");
            userConfig.setIsAutoLogin(0);
            userConfig.setIsTokenValid(0);
            // 下线清空相关金额
            userConfig.setBalance(null);
            userConfig.setBetting(null);
            userConfig.setAmount(null);
            userConfig.setResult(null);
            userConfig.setUpdateTime(LocalDateTimeUtil.format(LocalDateTime.now(), DatePattern.NORM_DATETIME_PATTERN));
            businessUserRedissonClient.getBucket(KeyUtil.genKey(RedisConstants.USER_PROXY_PREFIX, username, id)).set(JSONUtil.toJsonStr(userConfig));
        }
    }

    /**
     * 批量账号下线
     * @param username
     */
    public void batchLoginOut(String username) {
        List<UserConfig> userConfigs = configService.accounts(username, null);
        userConfigs.forEach(userConfig -> {
            userConfig.setToken(null);
            userConfig.setIsAutoLogin(0);
            userConfig.setIsTokenValid(0);
            // 下线清空相关金额
            userConfig.setBalance(null);
            userConfig.setBetting(null);
            userConfig.setAmount(null);
            userConfig.setResult(null);
            userConfig.setUpdateTime(LocalDateTimeUtil.format(LocalDateTime.now(), DatePattern.NORM_DATETIME_PATTERN));
            businessUserRedissonClient.getBucket(KeyUtil.genKey(RedisConstants.USER_PROXY_PREFIX, username, userConfig.getId())).set(JSONUtil.toJsonStr(userConfig));
        });
    }


    /**
     * 修改初始密码 密码要求：6-20个字符必须由大小写字母和数字组成
     * @param username
     * @param id
     * @param token
     * @param oldPassword
     * @param password
     * @return
     */
    public Boolean changePassword(String username, String id, String token, String oldPassword, String password) {
        List<UserConfig> userConfigs = configService.accounts(username, id);
        if (CollUtil.isEmpty(userConfigs)) {
            throw new BusinessException(SystemError.USER_1007);
        }

        UserConfig userConfig = userConfigs.get(0);
        log.info("修改初始密码-获取盘口账号redis信息:{}", userConfig);

        String baseUrl = userConfig.getBaseUrl();
        String url = baseUrl + "app/control/chg_passwd.php?uid=" + token;
        String params = "action=1&uid=" + token + "&OLDpassword=" + oldPassword + "&password=" + password + "&REpassword=" + password;

        Map<String, String> headers = new HashMap<>();
        headers.put("accept", "*/*");
        headers.put("accept-language", "zh-CN,zh;q=0.9");
        headers.put("priority", "u=1, i");
        headers.put("sec-ch-ua-mobile", "?0");
        headers.put("sec-ch-ua-platform", "\"Windows\"");
        headers.put("sec-fetch-dest", "empty");
        headers.put("sec-fetch-mode", "cors");
        headers.put("sec-fetch-site", "same-origin");
        headers.put("x-requested-with", "XMLHttpRequest");
        headers.put("content-type", "application/x-www-form-urlencoded");

        try {
            OkHttpProxyDispatcher.HttpResult resultHttp = dispatcher.execute("POST", url, params, headers, userConfig, false);

            // ✅ 成功判断
            JSONObject result = JSONUtil.parseObj(resultHttp.getBody());
            if (result.getInt("title", -1) == 1) {
                log.info("修改初始密码成功 - 账号:{} 旧密码:{} 新密码:{}",
                        userConfig.getAccount(), oldPassword, password);
                return true;
            }

            // ❌ 失败（有返回但不符合成功特征）
            log.error("修改初始密码失败 - 账号:{} 旧密码:{} 新密码:{}，返回内容:{}",
                    userConfig.getAccount(), oldPassword, password, resultHttp.getBody());
            return false;

        } catch (Exception e) {
            Throwable cause = e.getCause();
            if (cause instanceof UnknownHostException) {
                log.error("代理请求失败：主机未知。可能是域名解析失败或代理服务器地址有误。异常信息：{}", e.getMessage(), e);
            } else if (cause instanceof ConnectException) {
                log.error("代理请求失败：连接异常。可能是代理服务器未开启或网络不可达。异常信息：{}", e.getMessage(), e);
            } else if (cause instanceof SocketTimeoutException) {
                log.error("代理请求失败：连接超时。可能是网络延迟过高或代理服务器响应缓慢。异常信息：{}", e.getMessage(), e);
            } else if (cause instanceof IOException) {
                log.error("代理请求失败：IO异常。可能是数据传输错误或代理配置不正确。异常信息：{}", e.getMessage(), e);
            } else {
                log.error("代理请求失败：未知异常。请检查代理配置和网络状态。异常信息：{}", e.getMessage(), e);
            }
        }
        log.error("修改初始密码失败 - 账号:{} 旧密码:{} 新密码:{}", userConfig.getAccount(), oldPassword, password);
        return false;
    }

    /**
     * 登录IP验证
     * @return
     */
    public Boolean loginchecknotification(String username, String id, String token) {
        // 获取用户配置
        List<UserConfig> userConfigs = configService.accounts(username, id);
        if (CollectionUtil.isEmpty(userConfigs)) {
            throw new BusinessException(SystemError.USER_1007);
        }
        String baseUrl = userConfigs.get(0).getBaseUrl();
        if (StringUtils.isBlank(token)) {
            token = userConfigs.get(0).getToken();
        }
        if (StringUtils.isBlank(token)) {
            log.error("登录IP验证-盘口账号:{} token为空不进行验证", userConfigs.get(0).getAccount());
            return false;
        }

        // 请求参数和 URL
        String params = "trustLogin=true";
        String url = baseUrl + "logincheck/acknowledge?" + params;

        // 设置请求头
        Map<String, String> headers = new HashMap<>();
        headers.put("content-type", "application/x-www-form-urlencoded");
        headers.put("accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7");
        headers.put("accept-language", "zh-CN,zh;q=0.9");
        headers.put("accept-encoding", "gzip, deflate, br, zstd");
        headers.put("referer", baseUrl + "agent/loginchecknotification");
        headers.put("sec-ch-ua", "\"Google Chrome\";v=\"131\", \"Chromium\";v=\"131\", \"Not_A Brand\";v=\"24\"");
        headers.put("sec-ch-ua-mobile", "?0");
        headers.put("sec-ch-ua-platform", "\"Windows\"");
        headers.put("sec-fetch-dest", "iframe");
        headers.put("sec-fetch-mode", "navigate");
        headers.put("sec-fetch-site", "same-origin");
        headers.put("sec-fetch-user", "?1");
        headers.put("priority", "u=0, i");
        headers.put("upgrade-insecure-requests", "1");
        headers.put("cookie", "b-user-id=26524651-2f28-ea47-ba0e-60d8403cf359; ssid1=33de784aa526ae7242e60a34ae9c4bcf; random=2947; __nxquid=2VpeLcL6SZeI0gRHiGka7eIv7z2qcA==0013; token=" + token);
        headers.put("user-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36");

        try {
            // 使用 OkHttpProxyDispatcher 发起请求
            OkHttpProxyDispatcher.HttpResult result = dispatcher.executeFull("GET", url, null, headers, userConfigs.get(0));

            // 响应为 302 并跳转到 agent/index 即视为登录成功
            if (result.getStatus() == 302) {
                List<String> locationHeaders = result.getHeaders().get("location");
                String location = (locationHeaders != null && !locationHeaders.isEmpty()) ? locationHeaders.get(0) : null;

                if (StringUtils.isNotBlank(location) && location.contains("agent/index")) {
                    log.info("登录IP验证-盘口账号:{} 验证成功", userConfigs.get(0).getAccount());
                    return true;
                } else {
                    log.error("登录IP验证-盘口账号:{} 验证失败", userConfigs.get(0).getAccount());
                    return false;
                }
            }
        } catch (Exception e) {
            Throwable cause = e.getCause();
            if (cause instanceof UnknownHostException) {
                log.error("代理请求失败：主机未知。可能是域名解析失败或代理地址有误。异常信息：{}", e.getMessage(), e);
            } else if (cause instanceof ConnectException) {
                log.error("代理请求失败：连接异常。可能是代理服务器未开启或网络不可达。异常信息：{}", e.getMessage(), e);
            } else if (cause instanceof SocketTimeoutException) {
                log.error("代理请求失败：连接超时。可能是网络延迟过高或代理服务器响应缓慢。异常信息：{}", e.getMessage(), e);
            } else if (cause instanceof IOException) {
                log.error("代理请求失败：IO异常。可能是数据传输错误或代理配置不正确。异常信息：{}", e.getMessage(), e);
            } else {
                log.error("代理请求失败：未知异常。异常信息：{}", e.getMessage(), e);
            }
        }

        return false;
    }

    public void getHome(String securityCode, UserConfig userConfig) {
        String url = userConfig.getBaseUrl() + "?code=" + securityCode;

        Map<String, String> headers = new HashMap<>();
        headers.put("accept", "*/*");
        headers.put("accept-language", "zh-CN,zh;q=0.9");
        headers.put("x-requested-with", "XMLHttpRequest");

        String result;
        try {
            OkHttpProxyDispatcher.HttpResult resultHttp =
                    dispatcher.execute("GET", url, null, headers, userConfig, false);
            result = resultHttp.getBody();
        } catch (Exception e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            log.error("账号 {} 获取 main 页面异常", userConfig.getAccount(), cause);
            throw new BusinessException(SystemError.SYS_419, userConfig.getAccount(), "代理请求失败");
        }
    }

    /**
     * 访问首页
     *
     * @return 0:错误 1:成功 2:首次登录需修改密码
     */
    public Integer getMain(UserConfig userConfig, String uid) {
        String url = userConfig.getBaseUrl() + "app/control/main.php?active=1&uid=" + uid;

        Map<String, String> headers = new HashMap<>();
        headers.put("accept", "*/*");
        headers.put("accept-language", "zh-CN,zh;q=0.9");
        headers.put("x-requested-with", "XMLHttpRequest");

        String result;
        try {
            OkHttpProxyDispatcher.HttpResult resultHttp =
                    dispatcher.execute("GET", url, null, headers, userConfig, false);
            result = resultHttp.getBody();
        } catch (Exception e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            log.error("账号 {} 获取 main 页面异常", userConfig.getAccount(), cause);
            throw new BusinessException(SystemError.SYS_419, userConfig.getAccount(), "代理请求失败");
        }

        if (StringUtils.isBlank(result)) {
            log.warn("账号 {} main 页面返回空内容", userConfig.getAccount());
            return 0;
        }

        String html = result
                .replaceAll("[\\r\\n\\t]", "")
                .replace("帳", "帐")
                .replace("號", "号");


        // ③ 登录成功（最高优先级）
        if (html.contains("<title>欢迎光临")
                || (html.contains("会员帐户") && html.contains("getUserInfo"))) {
            log.info("账号 {} 登录成功", userConfig.getAccount());
            return 1;
        }

        // ② 账号停用 / 异常
        if (html.contains("停用")) {
            log.warn("账号 {} 已被停用", userConfig.getAccount());
            return 0;
        }

        // ① 强制修改密码
        if (html.contains("chg_passwd.php")) {
            log.warn("账号 {} 首次登录需修改密码", userConfig.getAccount());
            return 2;
        }

        // ④ 未知页面
        log.error("账号 {} main 页面返回未知内容: {}",
                userConfig.getAccount(),
                html.length() > 200 ? html.substring(0, 200) : html);

        return 0;
    }

    /**
     * 获取uid
     *
     * @return 结果
     */
    public String getUid(UserConfig userConfig) {
        String url = userConfig.getBaseUrl() + "app/control/index.php?active=getUid";

        Map<String, String> headers = new HashMap<>();
        headers.put("accept", "*/*");
        headers.put("accept-language", "zh-CN,zh;q=0.9");
        headers.put("sec-ch-ua-mobile", "?0");
        headers.put("sec-ch-ua-platform", "\"Windows\"");
        headers.put("sec-fetch-dest", "empty");
        headers.put("sec-fetch-mode", "cors");
        headers.put("sec-fetch-site", "same-origin");
        headers.put("x-requested-with", "XMLHttpRequest");

        String result = null;
        try {
            // ✅ 使用 OkHttpProxyDispatcher 发起 GET 请求
            OkHttpProxyDispatcher.HttpResult resultHttp = dispatcher.execute("GET", url, null, headers, userConfig, false);
            result = StringUtils.isNotBlank(resultHttp.getBody()) ? resultHttp.getBody() : null;
        } catch (Exception e) {
            Throwable cause = e.getCause();
            if (cause instanceof UnknownHostException) {
                log.error("代理请求失败：主机未知。可能是域名解析失败或代理服务器地址有误。异常信息：{}", e.getMessage(), e);
                throw new BusinessException(SystemError.SYS_419, userConfig.getAccount(), "连接超时");
            } else if (cause instanceof ConnectException) {
                log.error("代理请求失败：连接异常。可能是代理服务器未开启或网络不可达。异常信息：{}", e.getMessage(), e);
                throw new BusinessException(SystemError.SYS_419, userConfig.getAccount(), "连接异常");
            } else if (cause instanceof SocketTimeoutException) {
                log.error("代理请求失败：连接超时。可能是网络延迟过高或代理服务器响应缓慢。异常信息：{}", e.getMessage(), e);
                throw new BusinessException(SystemError.SYS_419, userConfig.getAccount(), "连接超时");
            } else if (cause instanceof IOException) {
                log.error("代理请求失败：IO异常。可能是数据传输错误或代理配置不正确。异常信息：{}", e.getMessage(), e);
                throw new BusinessException(SystemError.SYS_419, userConfig.getAccount(), "IO异常");
            } else {
                log.error("代理请求失败：未知异常。请检查代理配置和网络状态。异常信息：{}", e.getMessage(), e);
                throw new BusinessException(SystemError.SYS_419, userConfig.getAccount(), "未知异常");
            }
        }

        log.info("获取uid接口返回信息:{}", result);
        if (StringUtils.isNotBlank(result) && JSONUtil.isTypeJSONObject(result)) {
            JSONObject jsonObject = JSONUtil.parseObj(result);
            return jsonObject.getStr("uid");
        }

        return null;
    }

    /**
     * 单个账号登录
     * @param id
     * @return
     */
    public LoginDTO singleLogin(String username, String id) {
        List<UserConfig> userConfigs = configService.accounts(username, id);
        if (CollUtil.isEmpty(userConfigs)) {
            throw new BusinessException(SystemError.USER_1007);
        }
        String baseUrl = userConfigs.get(0).getBaseUrl();
        LoginDTO loginDTO = new LoginDTO();
        loginDTO.setAccount(userConfigs.get(0).getAccount());

        // 获取代理配置
        UserConfig userConfig = userConfigs.get(0);

        if (StringUtils.isNotBlank(userConfig.getToken())) {
            JSONObject jsonObject = account(username, id);
            if (!jsonObject.isEmpty()) {
                // 说明当前账号token还有效，不再登录
                loginDTO.setToken(userConfigs.get(0).getToken());
                userConfig.setToken(userConfigs.get(0).getToken());
                userConfig.setIsTokenValid(1);
                userConfig.setUpdateTime(LocalDateTimeUtil.format(LocalDateTime.now(), DatePattern.NORM_DATETIME_PATTERN));
                businessUserRedissonClient.getBucket(KeyUtil.genKey(RedisConstants.USER_PROXY_PREFIX, username, userConfigs.get(0).getId())).set(JSONUtil.toJsonStr(userConfig));
                return loginDTO;
            }
        }

        int retryCount = 0;
        int maxRetries = 3;
        String token = null;

        AdminLoginDTO adminLoginDTO = configService.getUser(username);
        getHome(adminLoginDTO.getSecurityCode(), userConfig);
        while (retryCount < maxRetries) {
            Map<String, String> code = code(userConfig);
            if ("0000".equals(code.get("code")) || code.get("code").isBlank()) {
                retryCount++;
                continue;
            }
            String uid = code.get("uid");
            String url = baseUrl + "app/control/index.php?type=";
            Map<String, String> headers = new HashMap<>();
            headers.put("sec-fetch-user", "?1");
            headers.put("upgrade-insecure-requests", "1");
            headers.put("content-type", "application/x-www-form-urlencoded");

            String params = "active=1&uid=" + uid + "&logintype=super_admin&langx=zh-cn&username=" + userConfig.getAccount() + "&passwd=" + userConfig.getPassword() + "&seccodeverify=" + code.get("code");

            log.info("登录请求 账号 {} uuid 为: {} 完整url {} 参数{}", userConfig.getAccount(), uid, url, params);

            try {
                // 用 OkHttpProxyDispatcher 发请求，POST 请求，获取完整响应
                OkHttpProxyDispatcher.HttpResult httpResult = dispatcher.executeFull("POST", url, params, headers, userConfig);

                int status = httpResult.getStatus();
                String respBody = httpResult.getBody();
                Map<String, List<String>> respHeaders = httpResult.getHeaders();

                if (respBody.contains("安全码")) {
                    throw new BusinessException(SystemError.USER_1018);
                }
                if (respBody.contains("账号或密码错误")) {
                    throw new BusinessException(SystemError.USER_1008);
                }
                if (respBody.contains("驗證碼")) {
                    // 验证码错误
                    log.info("登录请求 账号{} 验证码{} 验证码错误", userConfigs.get(0).getAccount(), code.get("code"));
                    retryCount++;
                    continue;
                }
                if (respBody.contains("密码错误次数超过限制")) {
                    throw new BusinessException(SystemError.USER_1011, userConfig.getAccount());
                }
                if (respBody.contains("IP已拉黑")) {
                    throw new BusinessException(SystemError.USER_1019);
                }
                if (respBody.contains("帐号已停用！")) {
                    throw new BusinessException(SystemError.USER_1017);
                }
                // 登录成功
                Pattern successPattern = Pattern.compile("Confirm\\.php\\?uid=([a-zA-Z0-9]+)");
                Matcher successMatcher = successPattern.matcher(respBody);
                if (successMatcher.find()) {
                    token = successMatcher.group(1);
                }
                // 访问main首页确认是否登录成功
                Integer main = getMain(userConfig, token);

                if (main == 1) {
                    userConfig.setIsAutoLogin(1);
                    if (StringUtils.isNotBlank(token)) {
                        loginDTO.setToken(token);
                        userConfig.setToken(token);
                        userConfig.setIsTokenValid(1);
                        userConfig.setUpdateTime(LocalDateTimeUtil.format(LocalDateTime.now(), DatePattern.NORM_DATETIME_PATTERN));
                        businessUserRedissonClient.getBucket(KeyUtil.genKey(RedisConstants.USER_PROXY_PREFIX, username, userConfigs.get(0).getId())).set(JSONUtil.toJsonStr(userConfig));
                        log.info("登录请求 账号 {} 参数{} 获取token成功 token:{}", userConfigs.get(0).getAccount(), params, token);
                        // 更新余额
                        account(username, id);
                        return loginDTO;
                    }
                } else if (main == 2) {
                    // 初次登录需要修改密码
                    String password = RandomUtil.randomStringLower(8);
                    boolean change = changePassword(username, id, token, userConfig.getPassword(), password);
                    if (!change) {
                        throw new BusinessException(SystemError.USER_1010, userConfig.getAccount());
                    }
                    userConfig.setPassword(password);
                    userConfig.setToken(null);
                    userConfig.setIsTokenValid(0);
                    businessUserRedissonClient.getBucket(KeyUtil.genKey(RedisConstants.USER_PROXY_PREFIX, username, userConfig.getId())).set(JSONUtil.toJsonStr(userConfig));
                    continue; // 重新登录
                }
                log.info("登录请求 账号 {} 参数{} 获取token失败", userConfigs.get(0).getAccount(), params);

                retryCount++;
            } catch (Exception e) {
                log.warn("账号 {} 获取 token 异常", userConfigs.get(0).getAccount());
                if (e instanceof BusinessException) {
                    throw (BusinessException) e;
                }
                Throwable cause = e.getCause();
                if (cause instanceof UnknownHostException) {
                    log.error("代理请求失败：主机未知。可能是域名解析失败或代理地址有误。异常信息：{}", e.getMessage(), e);
                    throw new BusinessException(SystemError.SYS_419, userConfig.getAccount(), "代理请求失败");
                } else if (cause instanceof ConnectException) {
                    log.error("代理请求失败：连接异常。可能是代理服务器未开启或网络不可达。异常信息：{}", e.getMessage(), e);
                    throw new BusinessException(SystemError.SYS_419, userConfig.getAccount(), "连接异常");
                } else if (cause instanceof SocketTimeoutException) {
                    log.error("代理请求失败：连接超时。可能是网络延迟过高或代理服务器响应缓慢。异常信息：{}", e.getMessage(), e);
                    throw new BusinessException(SystemError.SYS_419, userConfig.getAccount(), "连接超时");
                } else if (cause instanceof IOException) {
                    log.error("代理请求失败：IO异常。可能是数据传输错误或代理配置不正确。异常信息：{}", e.getMessage(), e);
                    throw new BusinessException(SystemError.SYS_419, userConfig.getAccount(), "IO异常");
                } else {
                    log.error("代理请求失败：未知异常。异常信息：{}", e.getMessage(), e);
                }
            }
        }

        if (StringUtils.isBlank(token) && retryCount == maxRetries) {
            userConfig.setToken(null);
            userConfig.setIsTokenValid(0);
            userConfig.setBalance(null);
            userConfig.setBetting(null);
            userConfig.setAmount(null);
            userConfig.setResult(null);
            userConfig.setUpdateTime(LocalDateTimeUtil.format(LocalDateTime.now(), DatePattern.NORM_DATETIME_PATTERN));
            businessUserRedissonClient.getBucket(KeyUtil.genKey(RedisConstants.USER_PROXY_PREFIX, username, userConfigs.get(0).getId())).set(JSONUtil.toJsonStr(userConfig));
            log.warn("账号 {} 获取 token 失败，已达到最大重试次数", userConfigs.get(0).getAccount());
        }
        return loginDTO;
    }

    /**
     * 批量登录
     *
     * @return token
     */
    public JSONObject batchLogin(String username) {
        List<UserConfig> userConfigs = configService.accounts(username, null);
        List<LoginDTO> results = Collections.synchronizedList(new ArrayList<>());
        Set<String> tokenSet = ConcurrentHashMap.newKeySet();

        // 记录成功和失败的数量
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failureCount = new AtomicInteger(0);

        // 线程池大小可以根据任务量调整
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        userConfigs.forEach(login -> {
            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                LoginDTO loginDTO;
                int retryCount = 0;
                final int maxRetries = 3;
                while (retryCount < maxRetries) {
                    loginDTO = singleLogin(username, login.getId()); // 调用单个登录逻辑
                    String token = loginDTO.getToken();
                    log.error("账号{}==登录{}次 login {}", login.getAccount(), retryCount, token);

                    if (StringUtils.isNotBlank(token) && tokenSet.add(token)) {
                        // token 不为空且未重复，成功
                        results.add(loginDTO);
                        successCount.incrementAndGet(); // 成功数量+1
                        return;
                    }

                    retryCount++;
                    log.warn("账号 {} 的 token {} 已重复，重新尝试获取 (重试次数: {})", login.getAccount(), token, retryCount);
                }

                // 登录失败
                log.error("账号 {} 最终获取 token 失败，重试次数已达上限", login.getAccount());
                results.add(new LoginDTO(login.getAccount(), null)); // 失败标记
                failureCount.incrementAndGet(); // 失败数量+1
            }, executorService);
            futures.add(future);
        });

        // 等待所有任务完成
        futures.forEach(CompletableFuture::join);
        executorService.shutdown();

        // 打印成功和失败数量
        log.info("批量登录完成：成功 {} 个，失败 {} 个", successCount.get(), failureCount.get());

        JSONObject result = new JSONObject();
        result.putOpt("successCount", successCount.get());
        result.putOpt("failureCount", failureCount.get());
        return result;
    }

    /**
     * 获取账号余额信息
     *
     * @return 结果
     */
    public JSONObject account(String username, String id) {
        List<UserConfig> userConfigs = configService.accounts(username, id);
        if (CollUtil.isEmpty(userConfigs)) {
            throw new BusinessException(SystemError.USER_1007);
        }
        log.info("获取盘口账号redis信息:{}", userConfigs.get(0));
        UserConfig userConfig = userConfigs.get(0);
        String url = userConfig.getBaseUrl() + "app/control/main.php?searchType=getUserInfo&user_id=" + userConfig.getUserId() + "&uid=" + userConfig.getToken() + "&reload=1&gtype=9";

        Map<String, String> headers = new HashMap<>();
        headers.put("accept", "*/*");
        headers.put("accept-language", "zh-CN,zh;q=0.9");
        headers.put("priority", "u=1, i");
        headers.put("sec-ch-ua-mobile", "?0");
        headers.put("sec-ch-ua-platform", "\"Windows\"");
        headers.put("sec-fetch-dest", "empty");
        headers.put("sec-fetch-mode", "cors");
        headers.put("sec-fetch-site", "same-origin");
        headers.put("x-requested-with", "XMLHttpRequest");

        String result = null;
        try {
            // ✅ 使用 OkHttpProxyDispatcher 发起 GET 请求
            OkHttpProxyDispatcher.HttpResult resultHttp = dispatcher.execute("GET", url, null, headers, userConfig, false);
            result = StringUtils.isNotBlank(resultHttp.getBody()) ? resultHttp.getBody() : null;
        } catch (Exception e) {
            Throwable cause = e.getCause();
            if (cause instanceof UnknownHostException) {
                log.error("代理请求失败：主机未知。可能是域名解析失败或代理服务器地址有误。异常信息：{}", e.getMessage(), e);
                throw new BusinessException(SystemError.SYS_419, userConfig.getAccount(), "连接超时");
            } else if (cause instanceof ConnectException) {
                log.error("代理请求失败：连接异常。可能是代理服务器未开启或网络不可达。异常信息：{}", e.getMessage(), e);
                throw new BusinessException(SystemError.SYS_419, userConfig.getAccount(), "连接异常");
            } else if (cause instanceof SocketTimeoutException) {
                log.error("代理请求失败：连接超时。可能是网络延迟过高或代理服务器响应缓慢。异常信息：{}", e.getMessage(), e);
                throw new BusinessException(SystemError.SYS_419, userConfig.getAccount(), "连接超时");
            } else if (cause instanceof IOException) {
                log.error("代理请求失败：IO异常。可能是数据传输错误或代理配置不正确。异常信息：{}", e.getMessage(), e);
                throw new BusinessException(SystemError.SYS_419, userConfig.getAccount(), "IO异常");
            } else {
                log.error("代理请求失败：未知异常。请检查代理配置和网络状态。异常信息：{}", e.getMessage(), e);
                throw new BusinessException(SystemError.SYS_419, userConfig.getAccount(), "未知异常");
            }
        }

        log.info("获取盘口账号{},接口返回信息:{}", userConfig.getAccount(), result);
        JSONObject json = JSONUtil.parseObj(result);
        // ✅ 同步余额（YuWo）
        BigDecimal balance = json.getBigDecimal("YuWo");
        if (balance != null) {
            userConfig.setBalance(balance);
            userConfig.setUpdateTime(LocalDateTimeUtil.format(
                    LocalDateTime.now(),
                    DatePattern.NORM_DATETIME_PATTERN
            ));
            businessUserRedissonClient.getBucket(KeyUtil.genKey(RedisConstants.USER_PROXY_PREFIX, username, id)).set(JSONUtil.toJsonStr(userConfig));
        }

        return json;
    }

    /**
     * 获取token
     * @param id
     * @return
     */
    public String token(String username, String id) {
        UserConfig userConfig = JSONUtil.toBean(JSONUtil.parseObj(businessUserRedissonClient.getBucket(KeyUtil.genKey(RedisConstants.USER_PROXY_PREFIX, username, id)).get()), UserConfig.class);
        log.info("获取到账户id:{} Redis数据:{}", id, userConfig);
        if (BeanUtil.isNotEmpty(userConfig)) {
            String token = userConfig.getToken();
            log.info("获取到账户id:{} Redis token数据:{}", id, token);
            JSONObject jsonObject = account(username, id);
            log.info("获取到账户id:{} 校验盘口账号有效性:{}", id, jsonObject);
            if (jsonObject.isEmpty()) {
                // 当前账号登录失效，进行自动登录操作
                log.info("校验盘口账号失效:{}", userConfig.getAccount());
                if (StringUtils.isBlank(userConfig.getToken())) {
                    log.info("redis中账号:{} token不存在才进行登录操作", userConfig.getAccount());
                    if (userConfig.getIsAutoLogin() == 1) {
                        log.info("盘口账号自动登录:{}", userConfig.getAccount());
                        LoginDTO loginDTO = singleLogin(username, userConfig.getId());
                        if (BeanUtil.isNotEmpty(loginDTO)) {
                            token = loginDTO.getToken();
                            log.info("盘口账号自动登录成功:{}", userConfig.getAccount());
                            userConfig.setIsTokenValid(1);
                            userConfig.setToken(token);
                            userConfig.setUpdateTime(LocalDateTimeUtil.format(LocalDateTime.now(), DatePattern.NORM_DATETIME_PATTERN));
                            businessUserRedissonClient.getBucket(KeyUtil.genKey(RedisConstants.USER_PROXY_PREFIX, username, id)).set(JSONUtil.toJsonStr(userConfig));
                            return token;
                        } else {
                            log.info("盘口账号自动登录失败:{}", userConfig.getAccount());
                            userConfig.setIsTokenValid(0);
                            userConfig.setToken(null);
                            userConfig.setUpdateTime(LocalDateTimeUtil.format(LocalDateTime.now(), DatePattern.NORM_DATETIME_PATTERN));
                            businessUserRedissonClient.getBucket(KeyUtil.genKey(RedisConstants.USER_PROXY_PREFIX, username, id)).set(JSONUtil.toJsonStr(userConfig));
                            return null;
                        }
                    } else {
                        userConfig.setIsTokenValid(0);
                        userConfig.setToken(null);
                        userConfig.setUpdateTime(LocalDateTimeUtil.format(LocalDateTime.now(), DatePattern.NORM_DATETIME_PATTERN));
                        businessUserRedissonClient.getBucket(KeyUtil.genKey(RedisConstants.USER_PROXY_PREFIX, username, id)).set(JSONUtil.toJsonStr(userConfig));
                        return null;
                    }
                }
            }
            return token;
        }
        return null;
    }

    /**
     * 获取期数
     *
     * @return 期数
     */
    public String period(String username, String id, String lottery) {
        List<UserConfig> userConfigs = configService.accounts(username, id);
        if (CollectionUtil.isEmpty(userConfigs)) {
            throw new BusinessException(SystemError.USER_1007);
        }

        UserConfig userConfig = userConfigs.get(0);
        String baseUrl = userConfig.getBaseUrl();
        String token = userConfig.getToken();

        String url = baseUrl + "app/control/LotteryResults.php?searchType=recently10&gtype=9&uid=" + token;

        Map<String, String> headers = new HashMap<>();
        headers.put("accept", "*/*");
        headers.put("accept-language", "zh-CN,zh;q=0.9");
        headers.put("priority", "u=1, i");
        headers.put("sec-ch-ua-mobile", "?0");
        headers.put("sec-ch-ua-platform", "\"Windows\"");
        headers.put("sec-fetch-dest", "empty");
        headers.put("sec-fetch-mode", "cors");
        headers.put("sec-fetch-site", "same-origin");
        headers.put("x-requested-with", "XMLHttpRequest");

        String result = null;
        try {
            // ✅ 替换请求发送逻辑为 dispatcher
            OkHttpProxyDispatcher.HttpResult resultHttp = dispatcher.execute("GET", url, null, headers, userConfig, false);
            result = StringUtils.isNotBlank(resultHttp.getBody()) ? resultHttp.getBody() : null;
        } catch (Exception e) {
            Throwable cause = e.getCause(); // 获取原始异常原因
            if (cause instanceof UnknownHostException) {
                log.error("获取期数 代理请求失败：主机未知。可能是域名解析失败或代理地址有误。异常信息：{}", e.getMessage(), e);
                throw new BusinessException(SystemError.SYS_419, userConfig.getAccount(), "代理请求失败");
            } else if (cause instanceof ConnectException) {
                log.error("获取期数 代理请求失败：连接异常。可能是代理服务器未开启或网络不可达。异常信息：{}", e.getMessage(), e);
                throw new BusinessException(SystemError.SYS_419, userConfig.getAccount(), "连接异常");
            } else if (cause instanceof SocketTimeoutException) {
                log.error("获取期数 代理请求失败：连接超时。可能是网络延迟过高或代理服务器响应缓慢。异常信息：{}", e.getMessage(), e);
                throw new BusinessException(SystemError.SYS_419, userConfig.getAccount(), "连接超时");
            } else if (cause instanceof IOException) {
                log.error("获取期数 代理请求失败：IO异常。可能是数据传输错误或代理配置不正确。异常信息：{}", e.getMessage(), e);
                throw new BusinessException(SystemError.SYS_419, userConfig.getAccount(), "IO异常");
            } else {
                log.error("获取期数 代理请求失败：未知异常。异常信息：{}", e.getMessage(), e);
                throw new BusinessException(SystemError.SYS_419, userConfig.getAccount(), "未知异常");
            }
        }

        if (StringUtils.isBlank(result)) {
            throw new BusinessException(SystemError.USER_1001);
        }

        log.info("平台用户:{},账号:{},种类:{},最新期数信息:{}", username, userConfig.getAccount(), lottery, result);

        if (result.contains("416 Range Not Satisfiable")) {
            throw new BusinessException(SystemError.SYS_419, userConfig.getAccount(), "代理请求失败");
        }

        return result;
    }

    /**
     * 获取最新开奖
     *
     * @return 期数
     */
    public String lastResult(String username, String id, String lottery) {
        List<UserConfig> userConfigs = configService.accounts(username, id);
        if (CollectionUtil.isEmpty(userConfigs)) {
            throw new BusinessException(SystemError.USER_1007);
        }

        UserConfig userConfig = userConfigs.get(0);
        String baseUrl = userConfig.getBaseUrl();
        String token = userConfig.getToken();

        String url = baseUrl + "member/lastResult?lottery=" + lottery + "&_=" + System.currentTimeMillis();
        Map<String, String> headers = new HashMap<>();
        headers.put("accept", "*/*");
        headers.put("accept-language", "zh-CN,zh;q=0.9");
        headers.put("cookie", "defaultSetting=5%2C10%2C20%2C50%2C100%2C200%2C500%2C1000; settingChecked=0; index=; index2=; page=lm; oid=3a9cad1bfcc69f05fd202bb3ddbc9df05b3bc062; defaultLT=" + lottery + "; random=4671; token=" + token + "; __nxquid=QebLfcL6SZckLHDCiGltWK0vmD+qcA==0013;");
        headers.put("priority", "u=1, i");
        headers.put("sec-ch-ua", "\"Chromium\";v=\"130\", \"Google Chrome\";v=\"130\", \"Not?A_Brand\";v=\"99\"");
        headers.put("sec-ch-ua-mobile", "?0");
        headers.put("sec-ch-ua-platform", "\"Windows\"");
        headers.put("sec-fetch-dest", "empty");
        headers.put("sec-fetch-mode", "cors");
        headers.put("sec-fetch-site", "same-origin");
        headers.put("user-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36");
        headers.put("x-requested-with", "XMLHttpRequest");

        String result = null;
        try {
            // ✅ 使用代理调度器替换原请求方式
            OkHttpProxyDispatcher.HttpResult resultHttp = dispatcher.execute("GET", url, null, headers, userConfig, false);
            result = StringUtils.isNotBlank(resultHttp.getBody()) ? resultHttp.getBody() : null;
        } catch (Exception e) {
            Throwable cause = e.getCause();
            if (cause instanceof UnknownHostException) {
                log.error("获取最新开奖 代理请求失败：主机未知。可能是域名解析失败或代理地址有误。异常信息：{}", e.getMessage(), e);
                throw new BusinessException(SystemError.SYS_419, userConfig.getAccount(), "代理请求失败");
            } else if (cause instanceof ConnectException) {
                log.error("获取最新开奖 代理请求失败：连接异常。可能是代理服务器未开启或网络不可达。异常信息：{}", e.getMessage(), e);
                throw new BusinessException(SystemError.SYS_419, userConfig.getAccount(), "连接异常");
            } else if (cause instanceof SocketTimeoutException) {
                log.error("获取最新开奖 代理请求失败：连接超时。可能是网络延迟过高或代理服务器响应缓慢。异常信息：{}", e.getMessage(), e);
                throw new BusinessException(SystemError.SYS_419, userConfig.getAccount(), "连接超时");
            } else if (cause instanceof IOException) {
                log.error("获取最新开奖 代理请求失败：IO异常。可能是数据传输错误或代理配置不正确。异常信息：{}", e.getMessage(), e);
                throw new BusinessException(SystemError.SYS_419, userConfig.getAccount(), "IO异常");
            } else {
                log.error("获取最新开奖 代理请求失败：未知异常。异常信息：{}", e.getMessage(), e);
                throw new BusinessException(SystemError.SYS_419, userConfig.getAccount(), "未知异常");
            }
        }

        if (StringUtils.isBlank(result)) {
            throw new BusinessException(SystemError.USER_1001);
        }

        log.info("平台用户:{},账号:{},种类:{},最新开奖信息:{}", username, userConfig.getAccount(), lottery, result);
        if (result.contains("416 Range Not Satisfiable")) {
            throw new BusinessException(SystemError.SYS_419, userConfig.getAccount(), "代理请求失败");
        }

        return result;
    }

    /**
     * 获取赔率
     *
     * @return 结果
     */
    public String odds(String username, String id, String wtype) {
        List<UserConfig> userConfigs = configService.accounts(username, id);
        if (CollUtil.isEmpty(userConfigs)) {
            throw new BusinessException(SystemError.USER_1007);
        }

        UserConfig userConfig = userConfigs.get(0);
        String baseUrl = userConfig.getBaseUrl();
        String token = userConfig.getToken();

        // GameType gameType = GameType.getByWtype(lottery);
        String url = baseUrl + "app/control/getRate.php?uid=" + token + "&gtype=9&wtype=" + wtype + "&ltype=1";

        Map<String, String> headers = new HashMap<>();
        headers.put("accept", "*/*");
        headers.put("accept-language", "zh-CN,zh;q=0.9");
        headers.put("priority", "u=1, i");
        headers.put("sec-ch-ua-mobile", "?0");
        headers.put("sec-ch-ua-platform", "\"Windows\"");
        headers.put("sec-fetch-dest", "empty");
        headers.put("sec-fetch-mode", "cors");
        headers.put("sec-fetch-site", "same-origin");
        headers.put("x-requested-with", "XMLHttpRequest");

        String result;
        try {
            // ✅ 替换核心请求逻辑
            OkHttpProxyDispatcher.HttpResult resultHttp = dispatcher.execute("GET", url, null, headers, userConfig, false);
            result = StringUtils.isNotBlank(resultHttp.getBody()) ? resultHttp.getBody() : null;
        } catch (Exception e) {
            Throwable cause = e.getCause(); // 获取原始异常原因
            if (cause instanceof UnknownHostException) {
                log.error("获取赔率 代理请求失败：主机未知。可能是域名解析失败或代理服务器地址有误。异常信息：{}", e.getMessage(), e);
                throw new BusinessException(SystemError.SYS_419, userConfig.getAccount(), "连接超时");
            } else if (cause instanceof ConnectException) {
                log.error("获取赔率 代理请求失败：连接异常。可能是代理服务器未开启或网络不可达。异常信息：{}", e.getMessage(), e);
                throw new BusinessException(SystemError.SYS_419, userConfig.getAccount(), "连接异常");
            } else if (cause instanceof SocketTimeoutException) {
                log.error("获取赔率 代理请求失败：连接超时。可能是网络延迟过高或代理服务器响应缓慢。异常信息：{}", e.getMessage(), e);
                throw new BusinessException(SystemError.SYS_419, userConfig.getAccount(), "连接超时");
            } else if (cause instanceof IOException) {
                log.error("获取赔率 代理请求失败：IO异常。可能是数据传输错误或代理配置不正确。异常信息：{}", e.getMessage(), e);
                throw new BusinessException(SystemError.SYS_419, userConfig.getAccount(), "IO异常");
            } else {
                log.error("获取赔率 代理请求失败：未知异常。请检查代理配置和网络状态。异常信息：{}", e.getMessage(), e);
                throw new BusinessException(SystemError.SYS_419, userConfig.getAccount(), "未知异常");
            }
        }

        JSONObject jsonObject = JSONUtil.parseObj(result);
        if (jsonObject.getInt("title") != 1) {
            throw new BusinessException(SystemError.USER_1015);
        }
        log.info("获取赔率信息, 平台用户:{}, 账号：{}, 游戏:{}, 赔率信息:{}", username, userConfig.getAccount(), wtype, result);
        return result;
    }


    /**
     * 下单api
     *
     * @return 结果
     */
    public JSONObject bet(String username, String id, OrderVO order) {
        List<UserConfig> userConfigs = configService.accounts(username, id);
        if (CollectionUtil.isEmpty(userConfigs)) {
            throw new BusinessException(SystemError.USER_1007);
        }

        UserConfig userConfig = userConfigs.get(0);
        String baseUrl = userConfig.getBaseUrl();
        String token = userConfig.getToken();
        String url = baseUrl + "member/bet";

        Map<String, String> headers = new HashMap<>();
        headers.put("accept", "*/*");
        headers.put("accept-language", "zh-CN,zh;q=0.9");
        headers.put("cookie", "defaultSetting=5%2C10%2C20%2C50%2C100%2C200%2C500%2C1000; settingChecked=0; index=; index2=; oid=3a9cad1bfcc69f05fd202bb3ddbc9df05b3bc062; defaultLT=" + order.getLottery() + "; page=lm; token=" + token);
        headers.put("priority", "u=1, i");
        headers.put("sec-ch-ua-mobile", "?0");
        headers.put("sec-ch-ua-platform", "\"Windows\"");
        headers.put("sec-fetch-dest", "empty");
        headers.put("sec-fetch-mode", "cors");
        headers.put("sec-fetch-site", "same-origin");
        headers.put("user-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36");
        headers.put("x-requested-with", "XMLHttpRequest");

        // ✅ 使用统一请求调度器
        String result;
        try {
            OkHttpProxyDispatcher.HttpResult resultHttp = dispatcher.execute("POST", url, JSONUtil.toJsonStr(order), headers, userConfig, false);
            result = StringUtils.isNotBlank(resultHttp.getBody()) ? resultHttp.getBody() : null;
        } catch (Exception e) {
            log.error("下注请求异常，用户:{}, 账号:{}, 参数:{}, 错误:{}", username, userConfig.getAccount(), JSONUtil.toJsonStr(order), e.getMessage(), e);
            throw new BusinessException(SystemError.ORDER_1101, e.getMessage());
        }

        JSONObject resultJson = new JSONObject();
        if (StringUtils.isNotBlank(result)) {
            resultJson = JSONUtil.parseObj(result);
            int status = resultJson.getInt("status", -1);
            if (status != 0) {
                throw new BusinessException(SystemError.ORDER_1100);
            }
        }
        return resultJson;
    }

    /**
     * 加拿大28 下单api
     *
     * @return 结果
     */
    public JSONObject betNew(String username, String id) {
        List<UserConfig> userConfigs = configService.accounts(username, id);
        if (CollectionUtil.isEmpty(userConfigs)) {
            throw new BusinessException(SystemError.USER_1007);
        }

        UserConfig userConfig = userConfigs.get(0);
        String baseUrl = userConfig.getBaseUrl();
        String token = userConfig.getToken();
        String url = baseUrl + "app/control/bet.php";

        Map<String, String> headers = new HashMap<>();
        headers.put("accept", "*/*");
        headers.put("accept-language", "zh-CN,zh;q=0.9");
        headers.put("priority", "u=1, i");
        headers.put("sec-ch-ua-mobile", "?0");
        headers.put("sec-ch-ua-platform", "\"Windows\"");
        headers.put("sec-fetch-dest", "empty");
        headers.put("sec-fetch-mode", "cors");
        headers.put("sec-fetch-site", "same-origin");
        headers.put("x-requested-with", "XMLHttpRequest");

        List<BetNew> order = new ArrayList<>();
        BetNew betNew1 = new BetNew();
        betNew1.setIdIndex("1");
        betNew1.setAmount("1");
        betNew1.setRate("950");
        BetNew betNew2 = new BetNew();
        betNew2.setIdIndex("2");
        betNew2.setAmount("1");
        betNew2.setRate("329");
        BetNew betNew3 = new BetNew();
        betNew3.setIdIndex("3");
        betNew3.setAmount("1");
        betNew3.setRate("164.7");
        order.add(betNew1);
        order.add(betNew2);
        order.add(betNew3);

        String params = "uid=" + token + "&gid=440328&gtype=9&wtype=all&ltype=1&ratechg=1&orders=" + JSONUtil.toJsonStr(order);

        // ✅ 使用统一请求调度器
        String result;
        try {
            OkHttpProxyDispatcher.HttpResult resultHttp = dispatcher.execute("POST", url, params, headers, userConfig, false);
            result = StringUtils.isNotBlank(resultHttp.getBody()) ? resultHttp.getBody() : null;
        } catch (Exception e) {
            log.error("下注请求异常，用户:{}, 账号:{}, 参数:{}, 错误:{}", username, userConfig.getAccount(), params, e.getMessage(), e);
            throw new BusinessException(SystemError.ORDER_1101, e.getMessage());
        }

        JSONObject resultJson = new JSONObject();
        if (StringUtils.isNotBlank(result)) {
            resultJson = JSONUtil.parseObj(result);
            int status = resultJson.getInt("title", -1);
            if (status != 1) {
                throw new BusinessException(SystemError.ORDER_1100);
            }
        }
        return resultJson;
    }

    /**
     * 计算延迟时间
     * @param closeTime
     * @param userConfigCloseTime
     * @return
     */
    private long calculateRemainingTime(long closeTime, Integer userConfigCloseTime) {
        if (userConfigCloseTime == null || userConfigCloseTime == 0) {
            return 0;
        }
        long currentTime = System.currentTimeMillis() / 1000;
        return closeTime - userConfigCloseTime - currentTime;
    }

    /**
     * 获取所有开启下注的平台用户
     * @return
     */
    private List<AdminLoginDTO> getAutoBetAdminUsers() {
        // 匹配所有平台用户的 Redis Key
        String pattern = KeyUtil.genKey(RedisConstants.USER_ADMIN_PREFIX, "*");

        // 使用 Redisson 执行扫描所有平台用户操作
        RKeys keys = businessUserRedissonClient.getKeys();
        Iterator<String> iterableKeys = keys.getKeysByPattern(pattern).iterator();

        // 提取出已开启方案的平台用户和方案 使用线程安全的列表
        List<AdminLoginDTO> adminUsers = new ArrayList<>();

        // 创建批量操作实例
        RBatch batch = businessUserRedissonClient.createBatch();

        // 存储 RBucket 实例
        List<RBucket<String>> buckets = new ArrayList<>();

        // 将每个操作添加到批处理队列
        while (iterableKeys.hasNext()) {
            String key = iterableKeys.next();
            RBucket<String> bucket = businessUserRedissonClient.getBucket(key); // 获取 RBucket 对象
            bucket.getAsync(); // 为每个键添加获取操作
            buckets.add(bucket);
        }
        // 执行所有批量操作
        batch.execute();

        // 处理结果
        buckets.forEach(bucket -> {
            String json = bucket.get();  // 获取每个键的值
            if (json != null) {
                AdminLoginDTO admin = JSONUtil.toBean(json, AdminLoginDTO.class);
                if (null != admin.getAutoBet() && admin.getAutoBet() != 0) {
                    adminUsers.add(admin);
                }
            }
        });
        return adminUsers;
    }

    private void logThreadPoolStatus(ThreadPoolExecutor executor, String poolName) {
        log.info("{} 状态 - 核心线程数: {}, 最大线程数: {}, 当前线程数: {}, 活跃线程数: {}, 队列大小: {}, 已完成任务数: {}",
                poolName,
                executor.getCorePoolSize(),
                executor.getMaximumPoolSize(),
                executor.getPoolSize(),
                executor.getActiveCount(),
                executor.getQueue().size(),
                executor.getCompletedTaskCount());
    }

    /**
     * 自动下注
     */
    public void autoBetCompletableFuture() {
        TimeInterval timerTotal = DateUtil.timer();
        TimeInterval timerPre = DateUtil.timer();
        log.info("前置操作，获取真正有效的平台用户和方案...");
        TimeInterval timerRedis = DateUtil.timer();
        // 提取出已开启下注的平台用户和方案 使用线程安全的列表
        // 第一阶段：过滤出有效平台用户
        List<AdminLoginDTO> adminUsers = getAutoBetAdminUsers();
        log.info("过滤出开启下注的平台用户花费:{}毫秒", timerRedis.interval());
        // 如果没有有效用户，直接退出
        if (CollUtil.isEmpty(adminUsers)) {
            log.info("所有平台用户都未开启自动下注，直接退出当前任务");
            return;
        }

        // 计数器，用于统计成功/失败数量
        AtomicInteger successCount = new AtomicInteger();
        AtomicInteger failureCount = new AtomicInteger();
        AtomicInteger reverseCount = new AtomicInteger();
        // 计数器，用于统计总的有效方案数量
        AtomicInteger totalTasks = new AtomicInteger(0);
        // 计数器，用于统计总的有效账号数量
        AtomicInteger totalAccounts = new AtomicInteger(0);
        // 计数器，用于统计总的下注数量
        AtomicInteger totalBets = new AtomicInteger(0);
        // 使用一个列表保存需要删除的用户
        List<AdminLoginDTO> toRemoveUsers = new ArrayList<>();
        Map<String, List<ConfigPlanVO>> enabledUserConfigsMap = new HashMap<>();
        Map<String, List<UserConfig>> tokenValidConfigsMap = new HashMap<>();

        // 第二阶段：并发处理每个有效用户的方案和登录账号
        TimeInterval timerThear = DateUtil.timer();
        int cpuCoreCount = Runtime.getRuntime().availableProcessors();
        ExecutorService executorAdminUserService = Executors.newFixedThreadPool(Math.min(adminUsers.size(), cpuCoreCount * 2));
//        ExecutorService executorAdminUserService = Executors.newCachedThreadPool();

        // 使用 ForkJoinPool 进行更高效的任务执行
//        ForkJoinPool forkJoinPool = new ForkJoinPool(Math.min(adminUsers.size(), cpuCoreCount * 2));
        List<CompletableFuture<Void>> adminFutures = new ArrayList<>();

        // 先请求所有用户的数据
        adminUsers.forEach(admin -> {
            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                // 批量请求启用方案和有效配置
                CompletableFuture<List<ConfigPlanVO>> enabledUserConfigsFuture = getEnabledConfigPlansAsync(admin.getUsername());
                CompletableFuture<List<UserConfig>> tokenValidConfigsFuture = getTokenValidConfigsAsync(admin.getUsername());

                CompletableFuture<Void> combinedFuture = CompletableFuture.allOf(
                        enabledUserConfigsFuture,
                        tokenValidConfigsFuture
                ).thenRun(() -> {
                    try {
                        // 校验当期是否需要跳过
                        JSONObject skipPlan = configService.getMaxDatetimeNotice(admin.getUsername());
                        if (skipPlan != null) {
                            LocalDateTime now = LocalDateTime.now();
                            LocalDateTime localDateTime = skipPlan.getLocalDateTime("datetime", now.plusSeconds(-60));
                            // -60是为了防止datetime时间不存在
                            Duration between = LocalDateTimeUtil.between(localDateTime, now);
                            if (between.toSeconds() < 60) {
                                // 如果距离上次跳过通知时间小于60秒，则跳过当期投注
                                log.info("平台用户:{}发生投注跳过事件,剩余跳过时间:{}秒", admin.getUsername(), 60 - between.toSeconds());
                                toRemoveUsers.add(admin);
                                return;
                            }
                        }

                        // 获取启用的配置方案和有效配置
                        List<ConfigPlanVO> enabledUserConfigs = enabledUserConfigsFuture.join();
                        List<UserConfig> tokenValidConfigs = tokenValidConfigsFuture.join();

                        // 校验结果并更新
                        if (CollUtil.isEmpty(enabledUserConfigs) || CollUtil.isEmpty(tokenValidConfigs)) {
                            toRemoveUsers.add(admin);
                            return;
                        }
                        // 校验正反投账号
                        List<UserConfig> validUserConfigs = validUserBetTypeAsync(admin, tokenValidConfigs).join();
                        if (CollUtil.isEmpty(validUserConfigs)) {
                            toRemoveUsers.add(admin);
                            return;
                        }

                        // 将有效方案添加到 map
                        enabledUserConfigsMap.put(admin.getUsername(), enabledUserConfigs);
                        tokenValidConfigsMap.put(admin.getUsername(), tokenValidConfigs);

                        // 更新总有效方案数量
                        totalTasks.addAndGet(enabledUserConfigs.size());
                        totalAccounts.addAndGet(tokenValidConfigs.size());
                    } catch (Exception e) {
                        log.error("处理用户 {} 时出错", admin.getUsername(), e);
                    }
                });
                // 使用 ForkJoinPool 提高并发性能
                combinedFuture.join();
            }, executorAdminUserService);

            adminFutures.add(future);
        });

        // 等待所有任务完成
        CompletableFuture<Void> allOf = CompletableFuture.allOf(adminFutures.toArray(new CompletableFuture[0]));
        allOf.join(); // 等待所有任务完成
        shutdownExecutor(executorAdminUserService);
        log.info("过滤出所有开启方案和已登录账号花费:{}毫秒", timerThear.interval());
        // 过滤掉需要删除的用户
        adminUsers.removeAll(toRemoveUsers);
        // 如果没有有效用户，直接退出
        if (CollUtil.isEmpty(adminUsers)) {
            log.info("所有平台用户都未开启方案或者不存在有效登录账号，直接退出当前任务");
            return;
        }
        log.info("前置操作结束,当前任务有:{}位平台用户开启方案并且存在有效登录账号,总有效方案数量:{},花费:{}毫秒", adminUsers.size(), totalTasks.get(), timerPre.interval());

        int threadPoolUserSize = Math.min(adminUsers.size(), cpuCoreCount * 2);
        int threadPoolPlanSize = Math.min(totalTasks.get(), Math.max(cpuCoreCount * 2, 100));
        // 初始化线程池（动态线程池，避免任务阻塞）
        // ExecutorService executorUserService = Executors.newFixedThreadPool(threadPoolUserSize);
        // ExecutorService executorPlanService = Executors.newFixedThreadPool(threadPoolPlanSize);
        // 平台用户线程池
        ExecutorService executorUserService = new ThreadPoolExecutor(
                threadPoolUserSize, // 核心线程数
                Math.max(adminUsers.size(), threadPoolUserSize), // 最大线程数
                0L, TimeUnit.MILLISECONDS, // 无空闲线程超时
                new ArrayBlockingQueue<>(300), // 有界队列，避免任务积压
                new ThreadFactoryBuilder().setNameFormat("user-pool-%d").build(), // 自定义线程工厂，便于调试
                new ThreadPoolExecutor.CallerRunsPolicy() // 拒绝策略：由调用者线程执行任务
        );

        // 用户方案线程池
        ExecutorService executorPlanService = new ThreadPoolExecutor(
                threadPoolPlanSize, // 核心线程数
                Math.max(totalTasks.get(), threadPoolPlanSize), // 最大线程数
                0L, TimeUnit.SECONDS, // 空闲线程超时时间
                new LinkedBlockingQueue<>(1000), // 有界队列，防止任务积压
                new ThreadFactoryBuilder().setNameFormat("plan-pool-%d").build(),
                new ThreadPoolExecutor.AbortPolicy() // 拒绝策略：抛出异常，便于发现问题
        );

        List<CompletableFuture<Void>> futures = new ArrayList<>();
        // 对每个平台用户的操作并行化
        for (AdminLoginDTO admin : adminUsers) {
            String username = admin.getUsername();
            CompletableFuture<Void> future = CompletableFuture.supplyAsync(() -> {
                TimeInterval timerFutures = DateUtil.timer();
                AtomicInteger successUserCount = new AtomicInteger();
                AtomicInteger failureUserCount = new AtomicInteger();
                AtomicInteger reverseUserCount = new AtomicInteger();
                // 获取该用户的启用方案
                List<ConfigPlanVO> enabledUserConfigs = enabledUserConfigsMap.get(admin.getUsername());
                if (CollUtil.isEmpty(enabledUserConfigs)) {
                    log.info("当前平台用户:{}不存在启用方案，直接跳过", username);
                    return null;
                }
                // 过滤掉未登录的盘口账号
                List<UserConfig> tokenValidConfigs = tokenValidConfigsMap.get(admin.getUsername());
                if (CollUtil.isEmpty(tokenValidConfigs)) {
                    log.info("当前平台用户:{}不存在有效盘口账号，直接跳过", username);
                    return null;
                }
                // 初始化记录盘口账户的下单校验缓存
                Map<String, Boolean> validOrderCache = new ConcurrentHashMap<>();

                // 记录成功账号
                List<UserConfig> successAccounts = new CopyOnWriteArrayList<>();
                // 记录失败账号
                List<UserConfig> failedAccounts = new CopyOnWriteArrayList<>();
                // 对每个配置计划进行异步处理
                List<CompletableFuture<Void>> planFutures = new ArrayList<>();
                AtomicReference<String> drawNumber = new AtomicReference<>();
                Map<String, String> accountOdds = new ConcurrentHashMap<>();
                // 获取方案列表中第一个所属游戏（前提是所有方案都是同一种游戏）
                String lottery = enabledUserConfigs.get(0).getLottery();

                // 校验当期是否需要跳过
                JSONObject skip = configService.getMaxDatetimeNotice(username);
                if (skip != null) {
                    LocalDateTime now = LocalDateTime.now();
                    LocalDateTime localDateTime = skip.getLocalDateTime("datetime", now.plusSeconds(-60));
                    // -60是为了防止datetime时间不存在
                    Duration between = LocalDateTimeUtil.between(localDateTime, now);
                    if (between.toSeconds() < 60) {
                        // 如果距离上次跳过通知时间小于60秒，则跳过当期投注
                        log.info("平台用户:{}发生投注跳过事件,剩余跳过时间:{}秒", username, 60 - between.toSeconds());
                        return null;
                    }
                }
                Map<String, String> periodMap = validatePeriod(username, "合并型方案", lottery, tokenValidConfigs);
                // 下注前检验所有账号token是否有效，或者盘口网络是否正常，所有账号都获取期数成功说明正常，只要有一个账号获取失败则跳出下注
                if (StringUtils.isBlank(periodMap.get("period"))) {
                    // 如果开启了跳过参数，则跳过,这里如果设置了跳过通知，如果token或网络无效不会投注的情况,然后又因为没有通知导致前端一下不知道什么情况为什么不投注
                    if (1 == admin.getSkip()) {
                        handlePeriodError(username, periodMap.get("account"), "合并型方案", lottery);
                    }
                    return null;
                }

                /*JSONObject validateOrder = validateOrder(username, plan.getName(), plan.getLottery(), tokenValidConfigs, validOrderCache);
                // 下注前检验所有账号token是否有效，或者盘口网络是否正常，所有账号都获取期数成功说明正常，只要有一个账号获取失败则跳出下注
                if (!validateOrder.getBool("valid")) {
                    // 如果开启了跳过参数，则跳过,这里如果设置了跳过通知，如果token或网络无效不会投注的情况,然后又因为没有通知导致前端一下不知道什么情况为什么不投注
                    if (1 == admin.getSkip()) {
                        handlePeriodError(username, validateOrder.getStr("account"), plan.getName(), plan.getLottery());
                    }
                    return;
                }*/

                JSONObject periodJson = JSONUtil.parseObj(periodMap.get("period"));
                long closeTime = periodJson.getLong("closeTime");
                drawNumber.set(periodJson.getStr("gid"));

                // 校验封盘时间
                if (!validateCloseTime(closeTime, username, lottery, drawNumber.get())) {
                    return null;
                }

                // 获取期数锁
                RLock lock = acquireBetLock(closeTime, username, lottery, drawNumber.get());
                if (lock == null) {
                    log.info("平台用户:{},游戏:{},期数:{},方案:{},当前期数正在执行", username, lottery, drawNumber.get(), "合并型方案");
                    return null;
                }

                ExecutorService oddsThreadPool = Executors.newFixedThreadPool(5); // 并发数按实际资源调整
                log.info("获取所有账号赔率 平台用户:{},有效盘口账号数:{},游戏:{},期数:{},方案:{}", username, tokenValidConfigs.size(), lottery, drawNumber.get(), "合并型方案");
                TimeInterval timerOdds = DateUtil.timer();
                List<CompletableFuture<Void>> futuresOdds = new ArrayList<>();
                for (UserConfig userConfig : tokenValidConfigs) {
                    CompletableFuture<Void> futureOdds = CompletableFuture.runAsync(() -> {
                        try {
                            String odds = getOddsFromCache(username, drawNumber.get(), userConfig.getId(), lottery, "合并型方案");
                            accountOdds.put(userConfig.getAccount(), odds);
                        } catch (Exception e) {
                            log.info("获取赔率异常 平台用户:{},盘口账号:{},游戏:{},期数:{},方案:{}, 异常信息:", username, userConfig.getAccount(), lottery, drawNumber.get(), "合并型方案", e);
                        }
                    }, oddsThreadPool);
                    futuresOdds.add(futureOdds);
                }
                // 等待所有赔率任务完成
                CompletableFuture.allOf(futuresOdds.toArray(new CompletableFuture[0])).join();
                logThreadPoolStatus((ThreadPoolExecutor) oddsThreadPool, "批量获取账号赔率线程池");
                // 执行完下注任务后关闭线程池
                shutdownExecutor(oddsThreadPool);
                log.info("获取所有账号赔率 平台用户:{},有效盘口账号数:{},游戏:{},期数:{},封盘时间:{},方案:{},花费:{}ms", username, tokenValidConfigs.size(), lottery, drawNumber.get(), LocalDateTimeUtil.of(closeTime), "合并型方案", timerOdds.interval());

                if (accountOdds.isEmpty()) {
                    log.info("获取整个方案的用户赔率为空 平台用户:{},游戏:{},期数:{},方案:{},释放当前期数锁并跳过", username, lottery, drawNumber.get(), "合并型方案");
                    // 确保锁释放
                    if (lock.isHeldByCurrentThread()) {
                        lock.unlock();
                    }
                    if (admin.getSkip() == 1) {
                        handlePeriodError(username, "所有账号", "合并型方案", lottery);
                        return null;
                    }
                }
                // 校验赔率账号数量是否一致
                if (accountOdds.size() != tokenValidConfigs.size()) {
                    List<String> missingAccounts = tokenValidConfigs.stream()
                            .map(UserConfig::getAccount)
                            .filter(acc -> !accountOdds.containsKey(acc))
                            .collect(Collectors.toList());

                    log.warn("翻倍计划-部分账号赔率缺失 平台用户:{},缺失账号:{},游戏:{},期数:{},方案:{}",
                            username, missingAccounts, lottery, drawNumber, "合并型方案");

                    if (lock.isHeldByCurrentThread()) {
                        lock.unlock();
                    }
                    if (admin.getSkip() == 1) {
                        handlePeriodError(username, String.join(",", missingAccounts), "合并型方案", lottery);
                    }
                    return null;
                }
                // 获取一个赔率
                String odds = null;
                // 遍历有效账号，找到第一个有赔率的账号
                for (UserConfig config : tokenValidConfigs) {
                    String account = config.getAccount();
                    odds = accountOdds.get(account);
                    if (StringUtils.isNotBlank(odds)) {
                        break;
                    }
                }

                if (StringUtils.isBlank(odds)) {
                    log.info("获取赔率失败 平台用户:{},游戏:{},期数:{},方案:{},释放当前期数锁并跳过", lottery, drawNumber.get(), username, "合并型方案");
                    // 确保锁释放
                    if (lock.isHeldByCurrentThread()) {
                        lock.unlock();
                    }
                    // 如果设置了跳过通知
                    if (admin.getSkip() == 1) {
                        handlePeriodError(username, "所有账号", "合并型方案", lottery);
                        return null;
                    }
                }

                for (ConfigPlanVO plan : enabledUserConfigs) {
                    String finalOdds = odds;
                    planFutures.add(CompletableFuture.runAsync(() -> {
                        // 获取正投账号数
                        List<UserConfig> positiveAccounts = getRandomAccount(tokenValidConfigs, plan.getPositiveAccountNum(), 1);
                        // 获取反投账号数
                        List<UserConfig> reverseAccounts = getRandomAccount(tokenValidConfigs, plan.getReverseAccountNum(), 2);
                        // 把正反投账号集合
                        List<UserConfig> allAccounts = new ArrayList<>();
                        allAccounts.addAll(positiveAccounts);
                        allAccounts.addAll(reverseAccounts);
                        totalBets.addAndGet(allAccounts.size());

                        JSONObject oddsJson = JSONUtil.parseObj(finalOdds);

                        // 获取正反投的位置 key
                        Map<Integer, Map<String, List<String>>> oddsMap = new HashMap<>();
                        // 获取单号
                        List<Integer> positions;
                        // 获取大小
                        List<Integer> twoSidedDxPositions;
                        // 获取单双
                        List<Integer> twoSidedDsPositions;
                        // 获取龙虎
                        List<Integer> twoSidedLhPositions;

                        int positiveNum = plan.getPositiveNum();
                        // 随机方案 -- 随机获取正投数量和反投数量 随机选择单面还是双面进行下注
                        if (null != plan.getPlanType() && plan.getPlanType() == 2) {
                            // 完全随机方案
                            positiveNum = RandomUtil.randomInt(1, 9);
                            if (RandomUtil.randomBoolean()) {
                                // 获取单号
                                positions = RandomUtil.randomEleList(plan.getPositions(), RandomUtil.randomInt(1,10));
                                // 获取大小
                                twoSidedDxPositions = null;
                                // 获取单双
                                twoSidedDsPositions = null;
                                // 获取龙虎
                                twoSidedLhPositions = null;
                            } else {
                                // 获取单号
                                positions = null;
                                // 随机获取大小的位置
                                twoSidedDxPositions = RandomUtil.randomEleList(plan.getTwoSidedDxPositions(), RandomUtil.randomInt(1,10));
                                // 随机获取单双的位置
                                twoSidedDsPositions = RandomUtil.randomEleList(plan.getTwoSidedDsPositions(), RandomUtil.randomInt(1,10));
                                // 随机获取龙虎的位置
                                twoSidedLhPositions = RandomUtil.randomEleList(plan.getTwoSidedLhPositions(), RandomUtil.randomInt(1,5));
                            }
                        } else if (null != plan.getPlanType() && plan.getPlanType() == 3) {
                            // 指定位置数量随机方案
                            positiveNum = RandomUtil.randomInt(1, 9);
                            // 获取单号
                            positions = RandomUtil.randomEleList(plan.getPositions(), plan.getPositionsNum());
                            // 随机获取大小的位置
                            twoSidedDxPositions = RandomUtil.randomEleList(plan.getTwoSidedDxPositions(), plan.getTwoSidedDxNum());
                            // 随机获取单双的位置
                            twoSidedDsPositions = RandomUtil.randomEleList(plan.getTwoSidedDsPositions(), plan.getTwoSidedDsNum());
                            // 随机获取龙虎的位置
                            twoSidedLhPositions = RandomUtil.randomEleList(plan.getTwoSidedLhPositions(), plan.getTwoSidedLhNum());
                        } else {
                            // 自定义方案
                            // 获取单号
                            positions = plan.getPositions();
                            // 获取大小
                            twoSidedDxPositions = plan.getTwoSidedDxPositions();
                            // 获取单双
                            twoSidedDsPositions = plan.getTwoSidedDsPositions();
                            // 获取龙虎
                            twoSidedLhPositions = plan.getTwoSidedLhPositions();
                        }

                        if (null != positions) {
                            for (int pos : positions) {
                                String jsonkey = "B" + pos;
                                List<String> matchedKeys = oddsJson.keySet().stream()
                                        .filter(matchedKey -> matchedKey.contains(jsonkey + "_"))
                                        .collect(Collectors.toList());
                                Map<String, List<String>> oddKeys = getOdds(matchedKeys, positiveNum);
                                oddsMap.put(pos, oddKeys);
                            }
                        }

                        // 使用 CountDownLatch 等待所有延迟任务完成
                        // CountDownLatch latch = new CountDownLatch(allAccounts.size()); // 初始化为需要执行的任务数
                        JSONObject twoSidedAmountJson = twoSidedAmountJson(oddsJson, twoSidedDxPositions, twoSidedDsPositions, twoSidedLhPositions, positiveAccounts, reverseAccounts, plan);
                        JSONObject amountJson = distributeAmount(oddsMap, positiveAccounts, reverseAccounts, plan);
                        // 下注线程池
                        ExecutorService executorBetService = new ThreadPoolExecutor(
                                allAccounts.size(), // 核心线程数
                                allAccounts.size(), // 最大线程数
                                0L, TimeUnit.SECONDS, // 空闲线程超时时间
                                new LinkedBlockingQueue<>(10), // 队列大小
                                new ThreadFactoryBuilder().setNameFormat("bet-pool-%d").build(),
                                new ThreadPoolExecutor.AbortPolicy() // 拒绝策略：抛出异常，便于发现问题
                        );

                        log.info("开始下注前,平台用户:{},游戏:{},期数:{},方案:{}, 正反投账号总数:{}, 正反投账号列表:{}, 投注金额拆分结果amountJson:{}", username, plan.getLottery(), drawNumber, plan.getName(), allAccounts.size(), allAccounts, amountJson);
                        // 在遍历下注操作时，使用线程池异步执行每个下注操作
                        List<CompletableFuture<Void>> betFutures = new ArrayList<>();
                        for (UserConfig userConfig : allAccounts) {
                            betFutures.add(CompletableFuture.runAsync(() -> {
                                // 遍历下注操作
                                retryBet(username, plan, drawNumber.get(), userConfig,
                                        oddsJson, successAccounts, failedAccounts, positions, amountJson, twoSidedAmountJson,
                                        successCount, failureCount, successUserCount, failureUserCount);
                            }, executorBetService));
                        }

                        // 等待所有下注操作完成
                        CompletableFuture<Void> allBets = CompletableFuture.allOf(betFutures.toArray(new CompletableFuture[0]));
                        try {
                            allBets.get();  // 等待所有下注操作完成
                        } catch (Exception e) {
                            log.error("执行下注操作时发生异常:", e);
                        }

                        logThreadPoolStatus((ThreadPoolExecutor) executorBetService, "创建订单线程池");
                        // 执行完下注任务后关闭线程池
                        shutdownExecutor(executorBetService);

                    }, executorPlanService));
                }

                // 等待所有配置计划任务完成
                CompletableFuture<Void> allPlans = CompletableFuture.allOf(planFutures.toArray(new CompletableFuture[0]));
                try {
                    allPlans.get();
                } catch (Exception e) {
                    log.error("执行配置计划任务时发生异常 平台用户:{}", username, e);
                }

                // 下注线程池
                ExecutorService executorBetService = new ThreadPoolExecutor(
                        5, // 核心线程数
                        10, // 最大线程数
                        0L, TimeUnit.SECONDS, // 空闲线程超时时间
                        new LinkedBlockingQueue<>(10), // 队列大小
                        new ThreadFactoryBuilder().setNameFormat("bet-pool-%d").build(),
                        new ThreadPoolExecutor.AbortPolicy() // 拒绝策略：抛出异常，便于发现问题
                );

                // 在遍历下注操作时，使用线程池异步执行每个下注操作
                List<CompletableFuture<Void>> betFutures = new ArrayList<>();
                for (UserConfig userConfig : tokenValidConfigs) {
                    betFutures.add(CompletableFuture.runAsync(() -> {
                        try {

                            String isBetRedisKey = KeyUtil.genKey(
                                    RedisConstants.USER_BET_PERIOD_RES_PREFIX,
                                    lottery,
                                    ToDayRangeUtil.getToDayRange(),
                                    drawNumber.get(),
                                    "success",
                                    username,
                                    userConfig.getAccount(),
                                    "merge"
                            );
                            // 判断是否已下注
                            JSONObject exists = JSONUtil.parseObj(redisson.getBucket(isBetRedisKey).get());
                            if (!exists.isEmpty() && exists.getInt("type") == 1) {
                                log.info("平台用户:{},游戏:{},期数:{},账号:{},方案:{},已下过注,不再重复下注", username, lottery, drawNumber, userConfig.getAccount()
                                        , "合并式方案");
                                return;
                            }

                            // 遍历下注操作
                            log.info("合并下单,期数:{}", drawNumber.get());
                            String result = mergeSubmitOrder(username, userConfig, enabledUserConfigs, drawNumber.get(), closeTime);
                            // 结果解析
                            handleOrderResult(username, result, userConfig, drawNumber.get(), enabledUserConfigs, successAccounts, failedAccounts, successCount, failureCount, successUserCount, failureUserCount);
                        } catch (Exception e) {
                            log.error("处理账户时发生异常,平台用户:{},账号:{},异常如下:", username, userConfig.getAccount(), e);
                            failedAccounts.add(userConfig);
                            failureCount.incrementAndGet();
                            failureUserCount.incrementAndGet();
                        }
                    }, executorBetService));
                }

                // 等待所有下注操作完成
                CompletableFuture<Void> allBets = CompletableFuture.allOf(betFutures.toArray(new CompletableFuture[0]));
                try {
                    allBets.get();  // 等待所有下注操作完成
                } catch (Exception e) {
                    log.error("执行批量下注操作时发生异常:", e);
                }

                logThreadPoolStatus((ThreadPoolExecutor) executorBetService, "批量下注线程池");
                // 执行完下注任务后关闭线程池
                shutdownExecutor(executorBetService);

                // 等待所有延迟任务完成
                TimeInterval timerProcess = DateUtil.timer();
                try {
                    if (CollUtil.isNotEmpty(failedAccounts)) {
                        // 补单处理
                        processReverseBet(admin, failedAccounts, successAccounts, enabledUserConfigs, drawNumber.get(), closeTime, reverseCount, reverseUserCount);
                    }
                } catch (Exception e) {
                    log.error("补单等待延迟任务完成时出现中断异常 平台用户:{}, 游戏:{},期数:{},方案:{}", username, lottery, drawNumber, "合并方案", e);
                } finally {
                    log.info("平台用户:{}, 游戏:{},方案:{},期数:{},补单花费:{}毫秒", username, lottery, "合并方案", drawNumber, timerProcess.interval());
                }

                if (successCount.get() > 0 || reverseCount.get() > 0) {
                    // 如果发生过投注，那么异步刷新一下流水
                     ThreadUtil.execAsync(() -> {
                         summaryToday(username, null, null);
                     });
                }
                log.info("批量下注完成,平台用户:{}, 成功次数: {}, 失败次数: {}, 反补次数: {}, 花费:{}毫秒", username, successUserCount.get(), failureUserCount.get(), reverseUserCount.get(), timerFutures.interval());
                return null;
            }, executorUserService);
            futures.add(future);
        }
        // 等待所有平台用户的任务完成
        CompletableFuture<Void> allTasks = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
        try {
            allTasks.get(60, TimeUnit.SECONDS); // 等待所有任务完成，设置 60 秒超时
        } catch (Exception e) {
            log.error("执行任务时出现异常", e);
        }

        logThreadPoolStatus((ThreadPoolExecutor) executorUserService, "用户线程池");
        logThreadPoolStatus((ThreadPoolExecutor) executorPlanService, "方案线程池");
        // 执行完所有任务后关闭线程池
        shutdownExecutor(executorUserService);
        shutdownExecutor(executorPlanService);
        // 打印批量登录的成功和失败次数
        log.info("当前任务批量下注完成, 开启下注用户数:{}, 总方案数:{}, 总登录账号数:{}, 应下注总数:{}, 成功次数: {}, 失败次数: {}, 反补次数: {}, 总花费:{}毫秒",
                adminUsers.size(),
                totalTasks.get(),
                totalAccounts.get(),
                totalBets.get(),
                successCount.get(),
                failureCount.get(),
                reverseCount.get(),
                timerTotal.interval());
    }

    /**
     * 优化线程池关闭逻辑
     * @param executorService
     */
    private void shutdownExecutor(ExecutorService executorService) {
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                log.error("任务超时，强制关闭线程池");
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /**
     * 执行下单
     *
     * @param username
     * @param plan
     * @param drawNumber
     * @param userConfig
     * @param oddsJson
     * @param successAccounts
     * @param failedAccounts
     * @param positions
     * @param amountJson
     * @param twoSidedAmountJson
     * @param successCount
     * @param failureCount
     * @param successUserCount
     * @param failureUserCount
     */
    private void retryBet(String username, ConfigPlanVO plan, String drawNumber, UserConfig userConfig,
                          JSONObject oddsJson, List<UserConfig> successAccounts, List<UserConfig> failedAccounts, List<Integer> positions, JSONObject amountJson, JSONObject twoSidedAmountJson,
                          AtomicInteger successCount, AtomicInteger failureCount, AtomicInteger successUserCount, AtomicInteger failureUserCount) {
        int retryCount = 3;
        int attempt = 0;
        boolean successBet = false;

        // 投注前获取当前盘口账号的赔率信息, 这样做的目的是防止同一批下注的账号中会有不同盘口的，不同盘口赔率不一致就会导致投注失败
        String odds = null;
        int maxRetry = 3;
        int retry = 0;
        while (retry < maxRetry) {
            try {
                odds = getOddsFromCache(username, drawNumber, userConfig.getId(), plan.getLottery(), plan.getName());
                if (StringUtils.isNotBlank(odds)) {
                    break;
                }
            } catch (Exception e) {
                log.warn("获取赔率异常 第{}次尝试 平台用户:{},游戏:{},期数:{},方案:{}", retry + 1, username, plan.getLottery(), drawNumber, plan.getName());
            }
            retry++;
            try {
                Thread.sleep(200); // 可选，稍微等一下再重试
            } catch (InterruptedException ignored) {}
        }

        JSONObject oddsJsonNew;
        if (StringUtils.isBlank(odds)) {
            log.info("重试{}次后仍无法获取最新赔率，使用传入的赔率 平台用户:{},游戏:{},期数:{},方案:{}", maxRetry, username, plan.getLottery(), drawNumber, plan.getName());
            oddsJsonNew = oddsJson; // 使用传入的赔率
        } else {
            oddsJsonNew = JSONUtil.parseObj(odds);
        }
        if (oddsJsonNew == null || oddsJsonNew.isEmpty()) {
            log.info("最终无法获取最新赔率 平台用户:{},游戏:{},期数:{},方案:{}", username, plan.getLottery(), drawNumber, plan.getName());
            failedAccounts.add(userConfig);
            failureCount.incrementAndGet();
            failureUserCount.incrementAndGet();
            return;
        }
        // while (attempt < retryCount && !successBet) {
            try {
                String isBetRedisKey = KeyUtil.genKey(
                        RedisConstants.USER_BET_PERIOD_RES_PREFIX,
                        plan.getLottery(),
                        ToDayRangeUtil.getToDayRange(),
                        drawNumber,
                        "success",
                        username,
                        userConfig.getAccount(),
                        "merge"
                );
                // 判断是否已下注
                JSONObject exists = JSONUtil.parseObj(redisson.getBucket(isBetRedisKey).get());
                if (!exists.isEmpty() && exists.getInt("type") == 1) {
                    log.info("平台用户:{},游戏:{},期数:{},账号:{},方案:{},已下过注,不再重复下注", username, plan.getLottery(), drawNumber, userConfig.getAccount()
                            , plan.getName());
                    return;
                }

                String betTypeStr = userConfig.getBetType() == 1 ? "positive" : "reverse";

                // 计算延迟时间
                // long delay = calculateRemainingTime(closeTime, userConfig.getCloseTime());
                // delay = Math.max(delay, 0); // 确保延迟时间非负数

                // log.info("平台用户:{},游戏:{},期数:{},封盘时间:{},账号:{},方案:{}, 及时延迟下注", username, plan.getLottery(), drawNumber, closeTime, userConfig.getAccount(), plan.getName());
                // 延迟下注任务
                // scheduledExecutorService.schedule(() -> {

                // 创建下注请求参数
                OrderVO order = createOrder(plan, oddsJsonNew, drawNumber, positions, betTypeStr, amountJson, twoSidedAmountJson, userConfig);
                if (order.getBets().isEmpty()) {
                    log.info("平台用户:{},游戏:{},期数:{},账号:{},方案:{},没有可下注的号码", username, plan.getLottery(), drawNumber, userConfig.getAccount(), plan.getName());
                    failedAccounts.add(userConfig);
                    failureCount.incrementAndGet();
                    failureUserCount.incrementAndGet();
                    return;
                }
                // 提交下注
                submitOrder(username, order, userConfig, plan, drawNumber, successAccounts, failedAccounts, successCount, failureCount);
                // 结果解析
                // handleOrderResult(username, result, userConfig, drawNumber, plan, successAccounts, failedAccounts, successCount, failureCount, successUserCount, failureUserCount);
                successBet = true; // 正常执行成功，设置为 true，退出循环
                // latch.countDown();
                // }, delay, TimeUnit.SECONDS);
            } catch (Exception e) {
                attempt++;  // 重试计数器递增
                log.error("处理账户时发生异常,平台用户:{},账号:{},异常如下:", username, userConfig.getAccount(), e);
                failedAccounts.add(userConfig);
                failureCount.incrementAndGet();
                failureUserCount.incrementAndGet();
                /*if (attempt >= retryCount) {
                    Thread.currentThread().interrupt();
                    log.error("下单重试次数已达到上限，平台用户:{}, 游戏:{},期数:{},方案:{}", username, plan.getLottery(), drawNumber, plan.getName());
                }*/
            }
        // }
    }

    /**
     * 检查所有账号是否获取到有效期数
     * @param username
     * @param planName
     * @param lottery
     * @param tokenValidConfigs
     * @return
     */
    public Map<String, String> validatePeriod(String username, String planName, String lottery, List<UserConfig> tokenValidConfigs) {
        Map<String, String> result = new ConcurrentHashMap<>();
        ExecutorService executor = Executors.newFixedThreadPool(Math.min(tokenValidConfigs.size(), 10));
        CompletionService<Map<String, String>> completionService = new ExecutorCompletionService<>(executor);

        for (UserConfig config : tokenValidConfigs) {
            completionService.submit(() -> {
                Map<String, String> res = new HashMap<>();
                try {
                    String period = odds(username, config.getId(), lottery);
                    if (StringUtils.isBlank(period)) {
                        log.info("平台用户:{}, 方案:{}, 游戏:{}, 账号:{} 获取期数为空", username, planName, lottery, config.getAccount());
                        res.put("account", config.getAccount());
                        res.put("period", null);
                    } else {
                        res.put("period", period);
                    }
                } catch (Exception e) {
                    log.info("平台用户:{}, 方案:{}, 游戏:{}, 账号:{} 获取期数异常", username, planName, lottery, config.getAccount(), e);
                    res.put("account", config.getAccount());
                    res.put("period", null);
                }
                return res;
            });
        }

        int total = tokenValidConfigs.size();
        String finalPeriod = null;

        try {
            for (int i = 0; i < total; i++) {
                Future<Map<String, String>> future = completionService.take();
                Map<String, String> res = future.get();

                if (res.containsKey("period") && res.get("period") == null) {
                    PriorityTaskExecutor.shutdownExecutor(executor);
                    return res;
                }
                if (res.containsKey("period") && finalPeriod == null) {
                    finalPeriod = res.get("period");
                }
            }
        } catch (Exception e) {
            log.error("校验期数线程异常", e);
            result.put("period", null);
            return result;
        } finally {
            PriorityTaskExecutor.shutdownExecutor(executor);
        }

        result.put("period", finalPeriod);
        return result;
    }

    private static final String[] RANKS = {
            "冠军", "亚军", "第三名", "第四名", "第五名",
            "第六名", "第七名", "第八名", "第九名", "第十名"
    };

    /**
     * 根据投注时的bet转换成注单结果的play字段参数格式 如:第九名 4
     * 主要用于投注超时的时候获取未结注单进行对比查看是否真正已投注
     * @param bet
     * @return
     */
    public String getPlayOrderBets(Bet bet) {
        String game = bet.getGame();
        if (game != null && game.matches("B\\d{1,2}")) {
            int index = Integer.parseInt(game.substring(1)) - 1;
            if (index >= 0 && index < RANKS.length) {
                return RANKS[index] + " " + bet.getContents();
            }
        }
        return bet.getContents(); // fallback 或 return null，根据需要处理
    }


    /**
     * 检查所有账号是否能正常请求投注api
     * @param username
     * @param planName
     * @param lottery
     * @param tokenValidConfigs
     * @param validOrderCache
     * @return
     */
    public JSONObject validateOrder(String username, String planName, String lottery, List<UserConfig> tokenValidConfigs, Map<String, Boolean> validOrderCache) {
        JSONObject periodMap = new JSONObject();

        ExecutorService executor = Executors.newFixedThreadPool(Math.min(tokenValidConfigs.size(), 10));
        CompletionService<JSONObject> completionService = new ExecutorCompletionService<>(executor);

        // 提交任务
        for (UserConfig config : tokenValidConfigs) {
            String account = config.getAccount();

            // ✅ 已校验过的账号，直接构造校验结果，不再发请求
            if (validOrderCache.containsKey(account)) {
                boolean cachedValid = validOrderCache.get(account);
                if (!cachedValid) {
                    periodMap.putOpt("account", account);
                    periodMap.putOpt("valid", false);
                    return periodMap; // 有一个失败，立即返回
                }
                continue; // 有效则跳过，避免重复
            }

            // 否则提交异步校验任务
            completionService.submit(() -> {
                JSONObject result = new JSONObject();
                boolean valid = false;
                try {
                    valid = validOrder(username, config, lottery);
                } catch (Exception e) {
                    log.info("平台用户:{}, 方案:{}, 游戏:{}, 账号:{} 访问投注api异常", username, planName, lottery, config.getAccount());
                }

                validOrderCache.put(config.getAccount(), valid);
                result.putOpt("account", config.getAccount());
                result.putOpt("valid", valid);
                return result;
            });
        }

        // 收集结果
        int total = tokenValidConfigs.size();
        try {
            for (int i = 0; i < total; i++) {
                Future<JSONObject> future = completionService.take(); // 最快完成的任务先处理
                JSONObject result = future.get();
                if (!result.getBool("valid", true)) {
                    executor.shutdownNow(); // 有一个失败，立即终止其他未完成的校验
                    return result;
                }
            }
        } catch (Exception e) {
            log.error("校验投注API线程异常", e);
            periodMap.putOpt("valid", false);
            return periodMap;
        } finally {
            PriorityTaskExecutor.shutdownExecutor(executor);
        }

        periodMap.putOpt("valid", true);
        return periodMap;
    }

    /**
     * 存储最新期数数据
     * @param lottery
     * @param period
     */
    private void savePeriodLatest(String lottery, String period) {
        JSONObject periodJson = JSONUtil.parseObj(period);
        long closeTime = periodJson.getLong("closeTime");
        long currentTime = System.currentTimeMillis();
        long betTime = closeTime - currentTime;
        businessUserRedissonClient.getBucket(KeyUtil.genKey(RedisConstants.USER_BET_SKIP_PREFIX,lottery)).set(periodJson, Duration.ofMillis(betTime));
    }

    /**
     * 校验封盘时间
     * @param closeTime
     * @param username
     * @param lottery
     * @param drawNumber
     * @return
     */
    private boolean validateCloseTime(long closeTime, String username, String lottery, String drawNumber) {
        if (closeTime < 15) {
            log.info("平台用户:{}, 游戏:{}, 期数:{}, 剩余封盘时间:{}, 即将封盘, 不进行下注", username, lottery, drawNumber, closeTime);
            return false;
        }
        return true;
    }

    /**
     * 尝试获取期数锁
     * @param username
     * @param drawNumber
     * @return
     */
    private RLock acquireBetLock(long closeTime, String username, String lottery, String drawNumber) {
        RLock lock = businessUserRedissonClient.getLock(KeyUtil.genKey(RedisConstants.USER_BET_PERIOD_PREFIX, lottery, drawNumber, username, "merge"));
        try {
            // 5秒内尝试获取锁，持锁时间到封盘时间+10秒
            if (lock.tryLock(5, closeTime + 10, TimeUnit.SECONDS)) {
                return lock;
            }
        } catch (InterruptedException e) {
            log.info("平台用户:{}, 游戏:{}, 期数:{}, 方案:{}, 获取锁失败", username, lottery, drawNumber, "合并型方案");
        }
        return null;
    }

    /**
     * 遍历账号获取获取期数失败 缓存记录
     * @param username
     */
    public void handlePeriodError(String username, String account, String planName, String lottery) {
        log.info("平台用户:{}, 方案:{}, 游戏:{}, 账号尝试获取期数失败, 跳过当期下注", username, planName, lottery);

        // 创建异常数据的 JSON 对象
        JSONObject jsonObject = new JSONObject();
        jsonObject.putOpt("lottery", Objects.requireNonNull(GameType.getByWtype(lottery)).getDescription());
        jsonObject.putOpt("datetime", LocalDateTimeUtil.formatNormal(LocalDateTime.now()));
        jsonObject.putOpt("title", "自动投注");
        jsonObject.putOpt("description", "检测到账号["+account+"]状态异常或盘口网络异常，请检查账号以及网络状态。跳过当前期数，下期开始自动投注。");
        jsonObject.putOpt("extra", "网络异常");
        jsonObject.putOpt("status", "danger");
        jsonObject.putOpt("type", "1");

        // 生成 Redis 键
        String key = KeyUtil.genKey(RedisConstants.USER_BET_SKIP_PREFIX, ToDayRangeUtil.getToDayRange(), username);

        // 使用分布式锁确保线程安全
        RLock lock = businessUserRedissonClient.getLock(KeyUtil.genKey(RedisConstants.USER_BET_SKIP_PREFIX, username));

        try {
            // 尝试获取锁，超时5秒，持锁10秒
            if (lock.tryLock(5, 10, TimeUnit.SECONDS)) {
                try {
                    // 获取现有的异常数据
                    RBucket<String> bucket = businessUserRedissonClient.getBucket(key);

                    String existingErrorsStr = bucket.get(); // 获取现有异常数据
                    JSONArray existingErrors = (existingErrorsStr == null) ? new JSONArray() : JSONUtil.parseArray(existingErrorsStr);
                    if (!existingErrors.isEmpty()) {
                        JSONObject latest = existingErrors.stream()
                                .map(obj -> (JSONObject) obj)  // 转为 JSONObject
                                .max((o1, o2) -> {
                                    // 获取 datetime 字段并进行比较，确保是倒序
                                    String datetime1 = o1.getStr("datetime");
                                    String datetime2 = o2.getStr("datetime");

                                    // 解析成 LocalDateTime 后进行比较
                                    LocalDateTime time1 = LocalDateTime.parse(datetime1, DateTimeFormatter.ofPattern(DatePattern.NORM_DATETIME_PATTERN));
                                    LocalDateTime time2 = LocalDateTime.parse(datetime2, DateTimeFormatter.ofPattern(DatePattern.NORM_DATETIME_PATTERN));

                                    return time1.compareTo(time2);  // 正序排列：最大值为最新的时间
                                })
                                .orElse(null);
                        LocalDateTime now = LocalDateTime.now();
                        LocalDateTime localDateTime = latest.getLocalDateTime("datetime", now.plusSeconds(-60));
                        // -60是为了防止datetime时间不存在
                        Duration between = LocalDateTimeUtil.between(localDateTime, now);
                        if (between.toSeconds() < 60) {
                            // 如果距离上次跳过通知时间小于60秒，则跳过当期投注
                            log.info("平台用户:{}, 60秒内发生投注跳过事件,不进行重复记录", username);
                            lock.unlock(); // 解锁
                            return;
                        }
                    }
                    // 添加新的异常记录到现有记录中
                    existingErrors.add(jsonObject);

                    // 使用 getAndSet() 保证原子操作
                    String previousValue = bucket.getAndSet(existingErrors.toString());

                    if (previousValue != null) {
                        // 如果值发生变化，说明是并发修改，需回滚或做进一步处理
                        log.info("Redis 键的数据已被其他线程修改，当前操作可能存在并发冲突: {}", key);
                    } else {
                        // 成功更新数据
                        bucket.set(existingErrors.toString(), Duration.ofHours(24));
                    }
                } finally {
                    lock.unlock(); // 解锁
                }
            } else {
                log.warn("无法获取期数跳过锁，跳过当前操作：{}", username);
            }
        } catch (InterruptedException e) {
            log.error("获取期数跳过分布式锁失败: {}", e.getMessage(), e);
            Thread.currentThread().interrupt(); // 恢复中断状态
        } catch (Exception e) {
            log.error("处理期数异常失败: {}", e.getMessage(), e);
        }
    }


    /**
     * 获取已启用的方案
     * @param username
     * @return
     */
    private List<ConfigPlanVO> getEnabledConfigPlans(String username) {
        List<ConfigPlanVO> configs = configService.getAllPlansRBatch(username, null);
        if (CollUtil.isEmpty(configs)) {
            return Collections.emptyList();
        }
        return configs.stream()
                .filter(config -> config.getEnable() != 0)
                .collect(Collectors.toList());
    }

    /**
     * 获取token有效的账号
     * @param username
     * @return
     */
    private List<UserConfig> getTokenValidConfigs(String username) {
        List<UserConfig> userConfigs = configService.accountsRBatch(username, null);

        if (CollUtil.isEmpty(userConfigs)) {
            return Collections.emptyList();
        }

        // 过滤掉未登录的盘口账号，直接返回有效的账户列表
        return userConfigs.stream()
                .filter(userConfig -> 1 == userConfig.getIsTokenValid() && StringUtils.isNotBlank(userConfig.getToken()))
                .collect(Collectors.toList());
    }

    /**
     * 校验list中正反投账号是否都存在
     * @param userConfigs
     * @return
     */
    private List<UserConfig> validUserBetType(AdminLoginDTO admin, List<UserConfig> userConfigs) {
        // 校验正投和反投账户数量是否都大于1
        long positiveCount = userConfigs.stream().filter(userConfig -> userConfig.getBetType() == 1).count();
        long reverseCount = userConfigs.stream().filter(userConfig -> userConfig.getBetType() == 2).count();
        if (positiveCount < 1 || reverseCount < 1) {
            // 如果正投或反投账户数量不满足条件，返回空列表
            // 关闭自动下注
            admin.setAutoBet(0);
            businessUserRedissonClient.getBucket(KeyUtil.genKey(
                    RedisConstants.USER_ADMIN_PREFIX,
                    admin.getUsername()
            )).set(JSONUtil.toJsonStr(admin));
            return Collections.emptyList();
        }
        return userConfigs;
    }

    private CompletableFuture<List<ConfigPlanVO>> getEnabledConfigPlansAsync(String username) {
        return CompletableFuture.supplyAsync(() -> {
            List<ConfigPlanVO> configs = configService.getAllPlansRBatch(username, null);
            if (CollUtil.isEmpty(configs)) {
                return Collections.emptyList();
            }
            return configs.stream()
                    .filter(config -> config.getEnable() != 0)
                    .collect(Collectors.toList());
        });
    }

    private CompletableFuture<List<UserConfig>> getTokenValidConfigsAsync(String username) {
        return CompletableFuture.supplyAsync(() -> {
            List<UserConfig> userConfigs = configService.accountsRBatch(username, null);
            if (CollUtil.isEmpty(userConfigs)) {
                return Collections.emptyList();
            }
            return userConfigs.stream()
                    .filter(userConfig -> 1 == userConfig.getIsTokenValid() && StringUtils.isNotBlank(userConfig.getToken()))
                    .collect(Collectors.toList());
        });
    }

    private CompletableFuture<List<UserConfig>> validUserBetTypeAsync(AdminLoginDTO admin, List<UserConfig> userConfigs) {
        return CompletableFuture.supplyAsync(() -> {
            long positiveCount = userConfigs.stream().filter(userConfig -> userConfig.getBetType() == 1).count();
            long reverseCount = userConfigs.stream().filter(userConfig -> userConfig.getBetType() == 2).count();
            if (positiveCount < 1 || reverseCount < 1) {
                admin.setAutoBet(0);
                businessUserRedissonClient.getBucket(KeyUtil.genKey(
                        RedisConstants.USER_ADMIN_PREFIX,
                        admin.getUsername()
                )).set(JSONUtil.toJsonStr(admin));
                return Collections.emptyList();
            }
            return userConfigs;
        });
    }

    /**
     * 缓存赔率： 可以先尝试从本地缓存中获取赔率数据，只有在缓存没有时再访问 Redis 和外部 API。
     * @param drawNumber
     * @param lottery
     * @return
     */
    public String getOddsFromCache(String username, String drawNumber, String accountId, String lottery, String planName) {
        String key = KeyUtil.genKey(RedisConstants.USER_BET_ODDS_PREFIX, DateUtil.format(DateUtil.date(), "yyyyMMdd"), lottery, drawNumber, accountId);
        // 使用 get 确保只有一个线程获取和设置缓存
        return localOddsCache.get(key, k -> {
            try {
                // 从 盘口 API获取
                log.info("平台用户:{},游戏:{},期数:{},账号:{},方案:{},期数赔率不存在缓存数据,api获取", username, lottery, drawNumber, accountId, planName);
                String odds = odds(username, accountId, lottery);
                if (StringUtils.isBlank(odds)) {
                    log.warn("平台用户:{},游戏:{},期数:{},账号:{},方案:{},获取赔率为空，key:{}", username, lottery, drawNumber, accountId, planName, k);
                    return null; // 不缓存空值
                }
                return odds;
            } catch (Exception e) {
                log.error("平台用户:{},游戏:{},期数:{},账号:{},方案:{},获取赔率失败：{}", username, lottery, drawNumber, accountId, planName, e.getMessage(), e);
                return null; // 不缓存异常情况
            }
        });
    }

    /**
     * 获取双面分配金额
     * @param oddsJson
     * @param twoSidedDxPositions
     * @param twoSidedDsPositions
     * @param twoSidedLhPositions
     * @param positiveAccounts
     * @param reverseAccounts
     * @param plan
     * @return
     */
    private JSONObject twoSidedAmountJson(JSONObject oddsJson, List<Integer> twoSidedDxPositions, List<Integer> twoSidedDsPositions, List<Integer> twoSidedLhPositions, List<UserConfig> positiveAccounts, List<UserConfig> reverseAccounts, ConfigPlanVO plan) {
        JSONObject twoSidedAmountJson = null;
        if (CollUtil.isNotEmpty(twoSidedDxPositions) || CollUtil.isNotEmpty(twoSidedDsPositions) || CollUtil.isNotEmpty(twoSidedLhPositions)) {
            // 获取双面-大小正反投的位置 key
            List<String> twoSidedDxOdds = new ArrayList<>();
            // 获取双面-单双正反投的位置 key
            List<String> twoSidedDsOdds = new ArrayList<>();
            // 获取双面-龙虎正反投的位置 key
            List<String> twoSidedLhOdds = new ArrayList<>();

            if (null != twoSidedDxPositions) {
                for (int pos : twoSidedDxPositions) {
                    String jsonkey = "DX" + pos;
                    List<String> matchedKeys = oddsJson.keySet().stream()
                            .filter(matchedKey -> matchedKey.startsWith(jsonkey + "_"))
                            .toList();
                    twoSidedDxOdds.addAll(matchedKeys);
                }
            }
            if (null != twoSidedDsPositions) {
                for (int pos : twoSidedDsPositions) {
                    String jsonkey = "DS" + pos;
                    List<String> matchedKeys = oddsJson.keySet().stream()
                            .filter(matchedKey -> matchedKey.startsWith(jsonkey + "_"))
                            .toList();
                    twoSidedDsOdds.addAll(matchedKeys);
                }
            }
            if (null != twoSidedLhPositions) {
                for (int pos : twoSidedLhPositions) {
                    String jsonkey = "LH" + pos;
                    List<String> matchedKeys = oddsJson.keySet().stream()
                            .filter(matchedKey -> matchedKey.startsWith(jsonkey + "_"))
                            .toList();
                    twoSidedLhOdds.addAll(matchedKeys);
                }
            }

            // 分配大小单双龙虎对应正反投
            Map<String, Map<String, List<String>>> twoSidedMap = assignPositions(positiveAccounts, reverseAccounts, twoSidedDxOdds, twoSidedDsOdds, twoSidedLhOdds);
            // 获取双面金额分配
            twoSidedAmountJson = twoSidedDistributeAmount(twoSidedMap, positiveAccounts, reverseAccounts, plan);
        }
        return twoSidedAmountJson;
    }

    /**
     * 封装订单
     * @param plan
     * @param oddsJson
     * @param drawNumber
     * @param positions
     * @param oddsType
     * @return
     */
    private OrderVO createOrder(ConfigPlanVO plan, JSONObject oddsJson, String drawNumber, List<Integer> positions, String oddsType, JSONObject amountJson, JSONObject twoSidedAmountJson, UserConfig userConfig) {
        OrderVO order = new OrderVO();
        List<Bet> bets = new ArrayList<>();
        order.setLottery(plan.getLottery());
        order.setDrawNumber(drawNumber);
        order.setFastBets(false);
        order.setIgnore(false);

        if (null != positions && !positions.isEmpty()) {
            for (int pos : positions) {
                JSONObject posJsonKey = amountJson.getJSONObject(String.valueOf(pos));
                if (StringUtils.equals("positive", oddsType)) {
                    JSONObject posJson = posJsonKey.getJSONObject("positive");
                    posJson.forEach((k, v) -> {
                        List<String> oddsKey = oddsKyeSplitter(k);
                        JSONObject amountValue = JSONUtil.parseObj(v);
                        Bet bet = new Bet();
                        bet.setGame(oddsKey.get(0));
                        bet.setAmount(amountValue.getLong(userConfig.getAccount()));
                        bet.setContents(oddsKey.get(1));
                        bet.setOdds(oddsJson.containsKey(k) ? oddsJson.getDouble(k) : 9.966);   // 设置默认赔率
                        bets.add(bet);
                    });
                } else {
                    JSONObject posJson = posJsonKey.getJSONObject("reverse");
                    posJson.forEach((k, v) -> {
                        List<String> oddsKey = oddsKyeSplitter(k);
                        JSONObject amountValue = JSONUtil.parseObj(v);
                        Bet bet = new Bet();
                        bet.setGame(oddsKey.get(0));
                        bet.setAmount(amountValue.getLong(userConfig.getAccount()));
                        bet.setContents(oddsKey.get(1));
                        bet.setOdds(oddsJson.containsKey(k) ? oddsJson.getDouble(k) : 9.966);   // 设置默认赔率
                        bets.add(bet);
                    });
                }
            }
        }
        if (null != twoSidedAmountJson && !twoSidedAmountJson.isEmpty()) {
            // 存在双面配置金额
            if (StringUtils.equals("positive", oddsType)) {
                JSONObject posJson = twoSidedAmountJson.getJSONObject("positive");
                posJson.forEach((k, v) -> {
                    List<String> oddsKey = oddsKyeSplitter(k);
                    JSONObject amountValue = JSONUtil.parseObj(v);
                    Bet bet = new Bet();
                    bet.setGame(oddsKey.get(0));
                    bet.setAmount(amountValue.getLong(userConfig.getAccount()));
                    bet.setContents(oddsKey.get(1));
                    bet.setOdds(oddsJson.containsKey(k) ? oddsJson.getDouble(k) : 9.966);   // 设置默认赔率
                    bets.add(bet);
                });
            } else {
                JSONObject posJson = twoSidedAmountJson.getJSONObject("reverse");
                posJson.forEach((k, v) -> {
                    List<String> oddsKey = oddsKyeSplitter(k);
                    JSONObject amountValue = JSONUtil.parseObj(v);
                    Bet bet = new Bet();
                    bet.setGame(oddsKey.get(0));
                    bet.setAmount(amountValue.getLong(userConfig.getAccount()));
                    bet.setContents(oddsKey.get(1));
                    bet.setOdds(oddsJson.containsKey(k) ? oddsJson.getDouble(k) : 9.966);   // 设置默认赔率
                    bets.add(bet);
                });
            }
        }
        order.setBets(bets);
        return order;
    }

    /**
     * 封装发起提交下注
     * @param order
     * @param userConfig
     * @param plan
     * @param drawNumber
     * @return
     */
    private void submitOrder(String username, OrderVO order, UserConfig userConfig, ConfigPlanVO plan, String drawNumber, List<UserConfig> successAccounts, List<UserConfig> failedAccounts, AtomicInteger successCount, AtomicInteger failureCount) {
        log.info("保存订单记录, 平台用户:{}, 账号:{}, 期数:{}, 方案:{}, 账号token:{}, 订单:{}", username, userConfig.getAccount(), drawNumber, plan.getName(), userConfig.getToken(), JSONUtil.parseObj(order));

        // ✅ 记录Redis订单请求
        redisson.getBucket(KeyUtil.genKey(
                RedisConstants.USER_BET_PERIOD_REQ_PREFIX,
                plan.getLottery(),
                ToDayRangeUtil.getToDayRange(),
                drawNumber,
                username,
                userConfig.getAccount(),
                String.valueOf(plan.getId())
        )).set(JSONUtil.toJsonStr(order), Duration.ofHours(24));

    }

    /**
     * 批量封装订单发起提交下注
     * @param userConfig
     * @param plans
     * @param drawNumber
     * @return
     */
    private String mergeSubmitOrder(String username, UserConfig userConfig, List<ConfigPlanVO> plans, String drawNumber, long closeTime) {
        log.info("合并下单, 平台用户:{}, 账号:{}, 期数:{}, 方案数:{}, 账号token:{}", username, userConfig.getAccount(), drawNumber, plans.size(), userConfig.getToken());
        TimeInterval timeBet = DateUtil.timer();
        boolean success = true;
        String lottery = plans.get(0).getLottery();

        String url = userConfig.getBaseUrl() + "member/bet";
        Map<String, String> headers = new HashMap<>();
        headers.put("accept", "*/*");
        headers.put("cookie", "defaultLT=" + lottery + "; token=" + userConfig.getToken());
        headers.put("sec-ch-ua-mobile", "?0");
        headers.put("sec-ch-ua-platform", "\"Windows\"");
        headers.put("sec-fetch-dest", "empty");
        headers.put("sec-fetch-mode", "cors");
        headers.put("sec-fetch-site", "same-origin");
        headers.put("user-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36");
        headers.put("x-requested-with", "XMLHttpRequest");

        // ✅ 获取Redis指定账户期数的所有订单请求
        OrderVO order = new OrderVO();
        List<Bet> bets = new ArrayList<>();
        for (ConfigPlanVO plan : plans) {
            Object orderObj = redisson.getBucket(KeyUtil.genKey(
                    RedisConstants.USER_BET_PERIOD_REQ_PREFIX,
                    lottery,
                    ToDayRangeUtil.getToDayRange(),
                    drawNumber,
                    username,
                    userConfig.getAccount(),
                    String.valueOf(plan.getId())
            )).get();
            if (orderObj == null) {
                log.info("合并下单, 平台用户:{}, 账号:{}, 期数:{}, 方案:{}, 没有订单请求", username, userConfig.getAccount(), drawNumber, plan.getName());
                continue;
            }
            OrderVO orderTemp = JSONUtil.toBean(orderObj.toString(), OrderVO.class);
            bets.addAll(orderTemp.getBets());
        }
        if (bets.isEmpty()) {
            log.info("合并下单, 平台用户:{}, 账号:{}, 期数:{}, 方案数:{}, 订单请求为空", username, userConfig.getAccount(), drawNumber, plans.size());
            return null;
        }
        log.info("合并下单, 平台用户:{}, 账号:{}, 期数:{}, 封盘时间:{}, 方案数:{}, 订单bet数:{}", username, userConfig.getAccount(), drawNumber, LocalDateTimeUtil.of(closeTime), plans.size(), bets.size());
        order.setDrawNumber(drawNumber);
        order.setIgnore(false);
        order.setLottery(lottery);
        order.setBets(bets);
        order.setFastBets(false);

        // ✅ 记录合并订单请求
        redisson.getBucket(KeyUtil.genKey(
                RedisConstants.USER_BET_PERIOD_REQ_PREFIX,
                lottery,
                ToDayRangeUtil.getToDayRange(),
                drawNumber,
                username,
                userConfig.getAccount(),
                "merge"
        )).set(JSONUtil.toJsonStr(order), Duration.ofHours(24));

        String result = null;
        try {
            // ✅ 使用 OkHttpProxyDispatcher 调用，并封装异步 + 超时
            CompletableFuture<OkHttpProxyDispatcher.HttpResult> future = CompletableFuture.supplyAsync(() -> {
                try {
                    log.info("进行下单-发起下单请求 平台用户:{},游戏:{},期数:{},封盘时间:{}", username, lottery, drawNumber, LocalDateTimeUtil.of(closeTime));
                    return dispatcher.execute("POST", url, JSONUtil.toJsonStr(order), headers, userConfig, false);
                } catch (IOException e) {
                    // 抛出包装异常，供外层 get() 捕获
                    throw new CompletionException(e);
                }
            });
            OkHttpProxyDispatcher.HttpResult resultHttp = future.get(10, TimeUnit.SECONDS); // 10 秒超时
            result = StringUtils.isNotBlank(resultHttp.getBody()) ? resultHttp.getBody() : null;
            log.info("下单结果, 平台用户:{}, 账号:{}, 期数:{}, 封盘时间:{}, 返回信息{}", username, userConfig.getAccount(), drawNumber, LocalDateTimeUtil.of(closeTime), result);

            if (StringUtils.isBlank(result) || !JSONUtil.isTypeJSONObject(result)) {
                log.error("下单失败, 平台用户:{}, 账号:{}, 期数:{}, 方案数:{}, 返回信息{}", username, userConfig.getAccount(), drawNumber, plans.size(), result);
                JSONObject failed = new JSONObject();
                failed.putOpt("status", 1);
                failed.putOpt("lottery", lottery);
                failed.putOpt("drawNumber", drawNumber);
                failed.putOpt("createTime", LocalDateTimeUtil.format(LocalDateTime.now(), DatePattern.NORM_DATETIME_PATTERN));
                failed.putOpt("account", userConfig.getAccount());
                failed.putOpt("isRepair", 0);
                failed.putOpt("message", StringUtils.isBlank(result) ? "账号异常,请检查登录状态以及账号是否正常" : "代理异常");
                return JSONUtil.toJsonStr(failed);
            }
        } catch (Exception e) {
            // ---------------------- 进入注单校验逻辑 ----------------------
            log.info("开始注单确认检查,平台用户:{},盘口账号:{},期数:{},封盘时间:{},进入确认是否需要补单逻辑：下注请求超时，启动最大 15 秒注单匹配流程",
                    username, userConfig.getAccount(), drawNumber, LocalDateTimeUtil.of(closeTime));
            long betTime = closeTime - System.currentTimeMillis();
            boolean successBet = false;                     // 标记是否确认投注成功
            long fixStart = System.currentTimeMillis();     // 补单判断阶段起始时间戳
            int maxTotalMillis = (int) (betTime - 5 * 1000);       // 补单判断总时长限制在关闭投注的5秒前
            long maxDurationMs = betTime - 5 * 1000;        // 最大注单匹配时间

            List<Bet> betAll = order.getBets();
            if (CollUtil.isNotEmpty(betAll)) {
                boolean[] found = new boolean[betAll.size()];  // 标记各注单是否匹配成功
                int foundCnt = 0;                            // 已匹配成功注单计数
                int maxRetries = 5;                          // 最大轮询次数
                int retryInterval = 2_000;                   // 每轮查询间隔时间（毫秒）
                int pageSize = 15;                           // 接口分页大小

                // 多轮查询已下注注单，确认是否已成功
                for (int attempt = 1; attempt <= maxRetries && foundCnt < betAll.size(); attempt++) {
                    long elapsed = System.currentTimeMillis() - fixStart;

                    // 超过最大判断时间，提前终止补单判断
                    if (elapsed >= maxTotalMillis) {
                        log.warn("开始注单确认检查,平台用户:{},盘口账号:{},期数:{},方案数:{},补单确认阶段超过期数关闭时间，提前结束查询，已匹配 {}/{} 注单",
                                username, userConfig.getAccount(), drawNumber, plans.size(), foundCnt, betAll.size());
                        break;
                    }

                    // 若剩余时间不足5秒，避免被 OkHttp 阻塞，跳过当前轮
                    if (maxDurationMs - elapsed < 5000) {
                        log.warn("开始注单确认检查,平台用户:{},盘口账号:{},期数:{},方案数:{},剩余时间不足5秒，跳过第{}次注单确认查询",
                                username, userConfig.getAccount(), drawNumber, plans.size(), attempt);
                        break;
                    }

                    int curPage = 1, totalPages = 1;

                    try {
                        // 分页查询注单列表
                        while (curPage <= totalPages && foundCnt < betAll.size()) {
                            if (System.currentTimeMillis() - fixStart >= maxTotalMillis) break;

                            long remainingTime = maxDurationMs - (System.currentTimeMillis() - fixStart);
                            int finalCurPage = curPage;

                            // 异步调用 settled() 接口查询注单，控制单次查询超时
                            CompletableFuture<JSONObject> future = CompletableFuture.supplyAsync(() ->
                                    JSONUtil.parseObj(settled(username, userConfig.getId(), false, finalCurPage, false))
                            );

                            JSONObject settledPg;
                            try {
                                settledPg = future.get(remainingTime, TimeUnit.MILLISECONDS);
                            } catch (TimeoutException ex) {
                                log.info("开始注单确认检查,平台用户:{},盘口账号:{},期数:{},方案数:{},settled() 查询超时，终止当前页注单查询",
                                        username, userConfig.getAccount(), drawNumber, plans.size());
                                break;
                            }

                            JSONArray pageList = settledPg.getJSONArray("list");

                            if (CollUtil.isNotEmpty(pageList)) {
                                if (curPage == 1) {
                                    int total = settledPg.getInt("total", 0);
                                    totalPages = Math.max(1, (total + pageSize - 1) / pageSize);
                                }

                                // 遍历当前页注单列表，逐个匹配订单中的下注注单
                                for (Object obj : pageList) {
                                    JSONObject jo = JSONUtil.parseObj(obj);
                                    String draw = jo.getStr("drawNumber", "");
                                    String play = jo.getStr("play", "");
                                    long amount = jo.getLong("amount", -1L);

                                    for (int i = 0; i < betAll.size(); i++) {
                                        if (!found[i]) {
                                            Bet b = betAll.get(i);
                                            String betPlay = getPlayOrderBets(b);
                                            if (draw.contains(drawNumber)
                                                    && play.contains(betPlay)
                                                    && amount == b.getAmount()) {
                                                found[i] = true;
                                                ++foundCnt;
                                                log.info("开始注单确认检查,平台用户:{},盘口账号:{},期数:{},方案数:{},匹配注单成功({}/{}): {}",
                                                        username, userConfig.getAccount(), drawNumber, plans.size(), foundCnt, betAll.size(), b);
                                                break;
                                            }
                                        }
                                    }
                                }
                            }
                            curPage++;
                        }
                    } catch (Exception ex) {
                        log.warn("开始注单确认检查,平台用户:{},盘口账号:{},期数:{},方案数:{},注单确认查询异常，第{}次尝试失败，错误：{}",
                                username, userConfig.getAccount(), drawNumber, plans.size(), attempt, ex.getMessage());
                    }

                    log.info("开始注单确认检查,平台用户:{},盘口账号:{},期数:{},方案数:{},注单确认进度: 第{}/{}次尝试，已匹配 {}/{} 注单",
                            username, userConfig.getAccount(), drawNumber, plans.size(), attempt, maxRetries, foundCnt, betAll.size());

                    if (foundCnt >= betAll.size()) {
                        break;
                    }

                    if (attempt < maxRetries) {
                        if (System.currentTimeMillis() - fixStart + retryInterval >= maxTotalMillis) {
                            log.warn("开始注单确认检查,平台用户:{},盘口账号:{},期数:{},方案数:{},剩余时间不足，放弃后续轮询",
                                    username, userConfig.getAccount(), drawNumber, plans.size());
                            break;
                        }
                        try {
                            Thread.sleep(retryInterval);
                        } catch (InterruptedException ex) {
                            Thread.currentThread().interrupt();
                            log.warn("开始注单确认检查,平台用户:{},盘口账号:{},期数:{},方案数:{},补单确认轮询被中断",
                                    username, userConfig.getAccount(), drawNumber, plans.size());
                            break;
                        }
                    }
                }

                successBet = (foundCnt >= betAll.size());
            } else {
                log.warn("开始注单确认检查,平台用户:{},盘口账号:{},期数:{},方案数:{},订单注单列表为空，无法确认下注成功",
                        username, userConfig.getAccount(), drawNumber, plans.size());
            }

            JSONObject resultObj = new JSONObject();
            if (successBet) {
                log.info("下注请求超时,但投注成功, 平台用户:{}, 账号:{}, 期数:{}, 方案数:{}", username, userConfig.getAccount(), drawNumber, plans.size());
                resultObj.putOpt("status", 0);
                resultObj.putOpt("message", "盘口投注成功");
            } else {
                log.info("下注请求超时,未投注成功, 平台用户:{}, 账号:{}, 期数:{}, 方案数:{}", username, userConfig.getAccount(), drawNumber, plans.size());
                resultObj.putOpt("status", 1);
                resultObj.putOpt("message", "代理链接超时");
            }
            resultObj.putOpt("lottery", lottery);
            resultObj.putOpt("drawNumber", drawNumber);
            resultObj.putOpt("createTime", LocalDateTimeUtil.format(LocalDateTime.now(), DatePattern.NORM_DATETIME_PATTERN));
            resultObj.putOpt("account", userConfig.getAccount());
            resultObj.putOpt("isRepair", 0);
            success = successBet;
            return JSONUtil.toJsonStr(resultObj);
        } finally {
            log.info("下单结束, 平台用户:{}, 账号:{}, 期数:{}, 方案数:{}, 返回信息{}, 网络请求成功:{}, 下单花费:{}毫秒", username, userConfig.getAccount(), drawNumber, plans.size(), result, success, timeBet.intervalMs());
        }

        return result;
    }

    /**
     * 下注前校验下单api是否能正常请求
     * @param username
     * @param userConfig
     * @return
     */
    public Boolean validOrder(String username, UserConfig userConfig, String lottery) {
        TimeInterval timeBet = DateUtil.timer();
        boolean success = false;

        String url = userConfig.getBaseUrl() + "member/bet";
        Map<String, String> headers = new HashMap<>();
        headers.put("accept", "*/*");
        headers.put("cookie", "defaultLT=" + lottery + "; token=" + userConfig.getToken());
        headers.put("sec-ch-ua-mobile", "?0");
        headers.put("sec-ch-ua-platform", "\"Windows\"");
        headers.put("sec-fetch-dest", "empty");
        headers.put("sec-fetch-mode", "cors");
        headers.put("sec-fetch-site", "same-origin");
        headers.put("user-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36");
        headers.put("x-requested-with", "XMLHttpRequest");

        try {
            CompletableFuture<OkHttpProxyDispatcher.HttpResult> future = CompletableFuture.supplyAsync(() -> {
                try {
                    return dispatcher.execute("POST", url, null, headers, userConfig, true);
                } catch (IOException e) {
                    throw new CompletionException(e);
                }
            });
            OkHttpProxyDispatcher.HttpResult resultHttp = future.get(10, TimeUnit.SECONDS);
            String result = StringUtils.isNotBlank(resultHttp.getBody()) ? resultHttp.getBody() : null;
            if (StringUtils.isNotBlank(result)) {
                success = true;
            }
        } catch (TimeoutException e) {
            log.info("请求超时 > 10s，强制中断，账号: {}", userConfig.getAccount());
        } catch (Exception e) {
            Throwable cause = e.getCause();
            if (cause instanceof UnknownHostException) {
                log.info("DNS解析失败，账号: {}", userConfig.getAccount());
            } else if (cause instanceof ConnectException) {
                log.info("连接超时，账号: {}", userConfig.getAccount());
            } else if (cause instanceof SocketTimeoutException) {
                log.info("读取超时，账号: {}", userConfig.getAccount());
            } else if (cause instanceof IOException) {
                log.info("IO异常，账号: {}", userConfig.getAccount());
            } else {
                log.info("未知异常，账号: {}", userConfig.getAccount(), e);
            }
        } finally {
            log.info("下单校验结束，平台用户:{}，账号:{}，是否通过:{}，耗时:{}ms",
                    username, userConfig.getAccount(), success, timeBet.intervalMs());
        }

        return success;
    }

    /**
     * 封装处理下注结果
     *
     * @param result
     * @param userConfig
     * @param drawNumber
     * @param plans
     * @param successAccounts
     * @param failedAccounts
     */
    private void handleOrderResult(String username, String result, UserConfig userConfig, String drawNumber, List<ConfigPlanVO> plans, List<UserConfig> successAccounts, List<UserConfig> failedAccounts, AtomicInteger successCount, AtomicInteger failureCount, AtomicInteger successUserCount, AtomicInteger failureUserCount) {
        if (result == null) {
            // 结果为空可能是没有进行请求
            return;
        }
        boolean isSuccess = true;
        JSONObject resultJson = JSONUtil.parseObj(result);
        String lottery = plans.get(0).getLottery();
        if (!resultJson.isEmpty()) {
            int status = resultJson.getInt("status");
            resultJson.putOpt("type", 1);// 类型 1:方案 2:计划
            if (status == 0) {
                // 成功逻辑
                redisson.getBucket(KeyUtil.genKey(
                        RedisConstants.USER_BET_PERIOD_RES_PREFIX,
                        lottery,
                        ToDayRangeUtil.getToDayRange(),
                        drawNumber,
                        "success",
                        username,
                        userConfig.getAccount(),
                        "merge"
                )).set(JSONUtil.toJsonStr(resultJson), Duration.ofHours(24));
                successAccounts.add(userConfig);
                successCount.incrementAndGet();
                successUserCount.incrementAndGet();
                log.info("下单成功, 平台用户:{}, 账号:{}, 期数:{}", username, userConfig.getAccount(), drawNumber);
            } else if (status == 2) {
                // 失败逻辑
                isSuccess = false;
                resultJson.putOpt("message", "投注超时");
            } else {
                // 失败逻辑
                isSuccess = false;
            }
        } else {
            isSuccess = false;
        }

        if (!isSuccess) {
            log.error("下单失败, 平台用户:{}, 账号:{}, 期数:{}, 返回信息{}", username, userConfig.getAccount(), drawNumber, resultJson);
            resultJson.putOpt("confirm", 0); // 待确认
            resultJson.putOpt("lottery", lottery);
            resultJson.putOpt("drawNumber", drawNumber);
            // resultJson.putOpt("planName", plan.getName());
            resultJson.putOpt("createTime", LocalDateTimeUtil.format(LocalDateTime.now(), DatePattern.NORM_DATETIME_PATTERN));
            resultJson.putOpt("account", userConfig.getAccount());
            resultJson.putOpt("isRepair", 0);// 是否为补单 0:否 1:是
            resultJson.putOpt("type", 1);// 类型 1:方案 2:计划
            // 记录失败信息
            businessUserRedissonClient.getBucket(KeyUtil.genKey(
                    RedisConstants.USER_BET_PERIOD_RES_PREFIX,
                    lottery,
                    ToDayRangeUtil.getToDayRange(),
                    drawNumber,
                    "failed",
                    username,
                    userConfig.getAccount(),
                    "merge"
            )).set(resultJson, Duration.ofHours(24));
            failedAccounts.add(userConfig);
            failureCount.incrementAndGet();
            failureUserCount.incrementAndGet();
        }
    }

    /**
     * 处理下单失败的反补操作
     * @param failedAccounts
     * @param successAccounts
     * @param plans
     * @param drawNumber
     */
    private void processReverseBet(AdminLoginDTO admin, List<UserConfig> failedAccounts, List<UserConfig> successAccounts, List<ConfigPlanVO> plans, String drawNumber, long closeTime, AtomicInteger reverseCount, AtomicInteger reverseUserCount) {
        if (CollectionUtil.isNotEmpty(failedAccounts)) {
            String lottery = plans.get(0).getLottery();
            // 当前方案存在账号下注失败
            for (UserConfig account : failedAccounts) {
                // 获取到下注失败的请求参数
                String key = KeyUtil.genKey(
                        RedisConstants.USER_BET_PERIOD_REQ_PREFIX,
                        lottery,
                        ToDayRangeUtil.getToDayRange(),
                        drawNumber,
                        admin.getUsername(),
                        account.getAccount(),
                        "merge"
                );
                log.info("平台用户:{}, 失败账号:{}, 方案:{}, 存在账号下注失败,开始反补,下注失败的请求参数key:{}", admin.getUsername(), account.getAccount(), "合并型方案", key);
                String failedReq = (String) redisson.getBucket(key).get();
                if (StringUtils.isBlank(failedReq)) {
                    log.info("平台用户:{}, 失败账号:{}, 方案:{}, 存在账号下注失败,反补失败,下注失败的请求参数key:{}不存在", admin.getUsername(), account.getAccount(), "合并型方案", key);
                    continue;
                }
                // 通过当前方案账号进行反补
                boolean isRever = false;
                for (UserConfig sucAccount : successAccounts) {
                    // 正投账号失败由反投账号补单，反之反投失败由正投账号补单
                    if (Objects.equals(sucAccount.getBetType(), account.getBetType())) continue;
                    isRever = true;
                    boolean reverseSuccess = handleReverseBet(admin.getUsername(), account, sucAccount, failedReq, plans, drawNumber, closeTime, reverseCount, reverseUserCount);
                    if (reverseSuccess) {
                        break; // 反补成功退出
                    }
                }
                if (!isRever) {
                    log.info("平台用户:{}, 失败账号:{}, 方案数:{}, 期数:{}, 没有进行补单,成功账户集合:{}", admin.getUsername(), account.getAccount(), plans.size(), drawNumber, successAccounts);
                }
            }
            // 关闭自动下注
            admin.setAutoBet(0);
            businessUserRedissonClient.getBucket(KeyUtil.genKey(
                            RedisConstants.USER_ADMIN_PREFIX,
                    admin.getUsername()
                    )).set(JSONUtil.toJsonStr(admin));
            // 保险起见-再把当前平台用户下的账号全部下线
            // batchLoginOut(username);
            // 把当前平台用户下的所有方案停用
            // List<ConfigPlanVO> configPlans = configService.getAllPlans(admin.getUsername(), null);
            // configPlans.forEach(configPlan -> {
            //     configPlan.setEnable(0);
            //     configService.plan(admin.getUsername(), configPlan);
            // });
        }
    }

    private boolean handleReverseBet(String username, UserConfig failedAccount, UserConfig sucAccount, String failedReq, List<ConfigPlanVO> plans, String drawNumber, long closeTime, AtomicInteger reverseCount, AtomicInteger reverseUserCount) {
        String lottery = plans.get(0).getLottery();
        String reverseLockKey = "reverse:lock:random:" + username + ":" + drawNumber + ":" + failedAccount.getId();
        RLock lock = redisson.getLock(reverseLockKey);
        try {
            boolean locked = lock.tryLock(2, 10, TimeUnit.SECONDS);
            if (!locked) {
                log.warn("补单正在进行中,跳过此次补单, 平台用户:{}, 失败账号:{}, 期数:{}, 方案数:{}", username, failedAccount, drawNumber, plans.size());
                return false;
            }
        } catch (InterruptedException e) {
            log.info("发起补单, 平台用户:{}, 游戏:{}, 期数:{}, 方案:{}, 补单时获取锁失败", username, lottery, drawNumber, plans.size());
        }

        log.info("发起补单, 平台用户:{}, 失败账号:{}, 对冲账号:{}, 账号token:{}, 期数:{}, 封盘时间:{}, 方案:{}, 补单参数:{}", username, failedAccount.getAccount(), sucAccount.getAccount(), failedAccount.getToken(), drawNumber, LocalDateTimeUtil.of(closeTime), "合并型方案", failedReq);

        // 获取赔率
        TimeInterval timerOdds = DateUtil.timer();
        String odds = null;
        try {
            odds = getOddsFromCache(username, drawNumber, sucAccount.getId(), lottery, "合并型方案");
        } catch (Exception e) {
            log.info("进行补单-获取赔率异常 平台用户:{},游戏:{},期数:{},方案:{}", username, lottery, drawNumber, "合并型方案");
            if (lock.isHeldByCurrentThread()) lock.unlock();

            JSONObject failed = new JSONObject();
            failed.putOpt("status", 1);
            failed.putOpt("lottery", lottery);
            failed.putOpt("drawNumber", drawNumber);
            failed.putOpt("planName", "合并型方案");
            failed.putOpt("createTime", LocalDateTimeUtil.format(LocalDateTime.now(), DatePattern.NORM_DATETIME_PATTERN));
            failed.putOpt("account", failedAccount.getAccount());
            failed.putOpt("message", "账号获取赔率失败");
            failed.putOpt("isRepair", 1);
            failed.putOpt("type", 1);
            businessUserRedissonClient.getBucket(KeyUtil.genKey(
                    RedisConstants.USER_BET_PERIOD_RES_PREFIX,
                    lottery,
                    ToDayRangeUtil.getToDayRange(),
                    drawNumber,
                    "failed",
                    username,
                    sucAccount.getAccount(),
                    "merge"
            )).set(failed, Duration.ofHours(24));
            return false;
        }
        if (StringUtils.isBlank(odds)) {
            log.info("进行补单-获取赔率失败 游戏:{},期数:{},平台用户:{},方案:{}", lottery, drawNumber, username, "合并型方案");
            if (lock.isHeldByCurrentThread()) lock.unlock();

            JSONObject failed = new JSONObject();
            failed.putOpt("status", 1);
            failed.putOpt("lottery", lottery);
            failed.putOpt("drawNumber", drawNumber);
            failed.putOpt("planName", "合并型方案");
            failed.putOpt("createTime", LocalDateTimeUtil.format(LocalDateTime.now(), DatePattern.NORM_DATETIME_PATTERN));
            failed.putOpt("account", failedAccount.getAccount());
            failed.putOpt("message", "账号获取赔率失败");
            failed.putOpt("isRepair", 1);
            failed.putOpt("type", 1);
            businessUserRedissonClient.getBucket(KeyUtil.genKey(
                    RedisConstants.USER_BET_PERIOD_RES_PREFIX,
                    lottery,
                    ToDayRangeUtil.getToDayRange(),
                    drawNumber,
                    "failed",
                    username,
                    sucAccount.getAccount(),
                    "merge"
            )).set(failed, Duration.ofHours(24));
            return false;
        }
        log.info("进行补单-获取赔率成功 平台用户:{},游戏:{},期数:{},封盘时间:{},获取赔率用时:{}ms", username, lottery, drawNumber, LocalDateTimeUtil.of(closeTime), timerOdds.interval());

        JSONObject oddsJsonNew = JSONUtil.parseObj(odds);

        // 解析失败请求
        OrderVO orderVO = JSONUtil.toBean(failedReq, OrderVO.class);

        int reverseAmount = 0;
        int positiveAmount = 0;
        for (ConfigPlanVO plan : plans) {
            reverseAmount += plan.getReverseAmount();
            positiveAmount += plan.getPositiveAmount();
        }

        double ratio;
        if (failedAccount.getBetType() == 1) {
            ratio = (double) reverseAmount / positiveAmount;
        } else {
            ratio = (double) positiveAmount / reverseAmount;
        }

        orderVO.getBets().forEach(bet -> {
            long amount = Math.round(bet.getAmount() * ratio);
            bet.setAmount(Math.max(amount, 2));
            bet.setOdds(oddsJsonNew.getDouble(bet.getGame() + "_" + bet.getContents()));
        });

        Map<String, Object> map = new HashMap<>();
        map.put("account", orderVO.getAccount());
        map.put("token", orderVO.getToken());
        map.put("drawNumber", orderVO.getDrawNumber());
        map.put("ignore", orderVO.isIgnore());
        map.put("lottery", orderVO.getLottery());
        map.put("bets", orderVO.getBets());
        map.put("fastBets", orderVO.isFastBets());

        JSONObject compensateReqJson = new JSONObject(map);

        // 记录反补的请求参数
        redisson.getBucket(KeyUtil.genKey(
                RedisConstants.USER_BET_PERIOD_REQ_PREFIX,
                lottery,
                ToDayRangeUtil.getToDayRange(),
                drawNumber,
                username,
                sucAccount.getAccount(),
                "merge",
                "reverse"
        )).set(JSONUtil.toJsonStr(compensateReqJson), Duration.ofHours(24));

        String result = null;
        String now = LocalDateTimeUtil.format(LocalDateTime.now(), DatePattern.NORM_DATETIME_PATTERN);
        long nowTimestamp = System.currentTimeMillis();
        boolean resultBol = false;

        // ✅ 使用 OkHttpProxyDispatcher 替换 HttpRequest，业务逻辑完全不变
        Map<String, String> headers = new HashMap<>();
        headers.put("accept", "*/*");
        headers.put("cookie", "defaultLT=" + lottery + "; token=" + sucAccount.getToken());
        headers.put("sec-ch-ua-mobile", "?0");
        headers.put("sec-ch-ua-platform", "\"Windows\"");
        headers.put("sec-fetch-dest", "empty");
        headers.put("sec-fetch-mode", "cors");
        headers.put("sec-fetch-site", "same-origin");
        headers.put("user-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36");
        headers.put("x-requested-with", "XMLHttpRequest");

        try {
            CompletableFuture<OkHttpProxyDispatcher.HttpResult> future = CompletableFuture.supplyAsync(() -> {
                try {
                    log.info("进行补单-发起补单请求 平台用户:{},游戏:{},期数:{},封盘时间:{}", username, lottery, drawNumber, LocalDateTimeUtil.of(closeTime));
                    return dispatcher.execute("POST", sucAccount.getBaseUrl() + "member/bet", compensateReqJson.toString(), headers, sucAccount, false);
                } catch (IOException e) {
                    throw new CompletionException(e);
                }
            });

            // 这里可以根据需要调整超时时间
            OkHttpProxyDispatcher.HttpResult resultHttp = future.get(10, TimeUnit.SECONDS);
            result = StringUtils.isNotBlank(resultHttp.getBody()) ? resultHttp.getBody() : null;

            log.info("补单结束, 平台用户:{}, 失败账号:{}, 对冲账号:{}, 期数:{}, 返回信息{}", username, failedAccount.getAccount(), sucAccount.getAccount(), drawNumber, result);

            if (!JSONUtil.isTypeJSONObject(result)) {
                log.error("补单失败, 平台用户:{}, 失败账号:{}, 对冲账号:{}, 期数:{}, 返回信息{}", username, failedAccount.getAccount(), sucAccount.getAccount(), drawNumber, result);
                JSONObject resultJson = new JSONObject();
                resultJson.putOpt("confirm", 0);
                resultJson.putOpt("lottery", lottery);
                resultJson.putOpt("drawNumber", drawNumber);
                resultJson.putOpt("planName", "合并型方案");
                resultJson.putOpt("createTime", now);
                resultJson.putOpt("account", sucAccount.getAccount());
                resultJson.putOpt("message", "代理异常");
                resultJson.putOpt("isRepair", 1);
                resultJson.putOpt("type", 1);
                businessUserRedissonClient.getBucket(KeyUtil.genKey(
                        RedisConstants.USER_BET_PERIOD_RES_PREFIX,
                        lottery,
                        ToDayRangeUtil.getToDayRange(),
                        drawNumber,
                        "failed",
                        username,
                        sucAccount.getAccount(),
                        "merge"
                )).set(resultJson, Duration.ofHours(24));

                String failedKey = KeyUtil.genKey(
                        RedisConstants.USER_BET_PERIOD_RES_PREFIX,
                        lottery,
                        ToDayRangeUtil.getToDayRange(),
                        drawNumber,
                        "failed",
                        username,
                        failedAccount.getAccount(),
                        "merge"
                );
                JSONObject failedJson = JSONUtil.parseObj(businessUserRedissonClient.getBucket(failedKey).get());
                if (null != failedJson && !failedJson.isEmpty()) {
                    failedJson.putOpt("repair", 0);
                    failedJson.putOpt("repairAccount", (StringUtils.isBlank(failedJson.getStr("repairAccount")) ? "" : failedJson.getStr("repairAccount") + ",") + sucAccount.getAccount());
                    failedJson.putOpt("type", 1);
                    businessUserRedissonClient.getBucket(failedKey).set(failedJson, Duration.ofHours(24));
                }
                return resultBol;
            }
        } catch (Exception e) {
            Throwable cause = e.getCause();
            if (cause instanceof UnknownHostException) {
                log.error("反补请求代理异常, 失败账号:{}, 对冲账号:{}, 期数:{}, 异常:{}", failedAccount.getAccount(), sucAccount.getAccount(), drawNumber, e.getMessage());
            } else if (cause instanceof ConnectException) {
                log.error("反补请求代理异常, 失败账号:{}, 对冲账号:{}, 期数:{}, 异常:{}", failedAccount.getAccount(), sucAccount.getAccount(), drawNumber, e.getMessage());
            } else if (cause instanceof SocketTimeoutException) {
                log.error("反补请求代理异常, 失败账号:{}, 对冲账号:{}, 期数:{}, 异常:{}", failedAccount.getAccount(), sucAccount.getAccount(), drawNumber, e.getMessage());
            } else if (cause instanceof IOException) {
                log.error("反补请求代理异常, 失败账号:{}, 对冲账号:{}, 期数:{}, 异常:{}", failedAccount.getAccount(), sucAccount.getAccount(), drawNumber, e.getMessage());
            } else {
                log.error("反补请求代理异常, 失败账号:{}, 对冲账号:{}, 期数:{}, 异常:{}", failedAccount.getAccount(), sucAccount.getAccount(), drawNumber, e.getMessage());
            }

            // ---------------------- 进入注单校验逻辑 ----------------------
            log.info("开始补单确认检查,平台用户:{},盘口账号:{},期数:{},封盘时间:{},进入确认是否需要补单逻辑：下注请求超时，启动最大 15 秒注单匹配流程",
                    username, sucAccount.getAccount(), drawNumber, LocalDateTimeUtil.of(closeTime));
            boolean successBet = false;                     // 标记是否确认投注成功
            long fixStart = System.currentTimeMillis();     // 补单判断阶段起始时间戳
            int maxTotalMillis = 30_000;       // 补单判断总时长限制在关闭投注的5秒前
            long maxDurationMs = 30 * 1000;       // 最大注单匹配时间

            List<Bet> betAll = orderVO.getBets();
            if (CollUtil.isNotEmpty(betAll)) {
                boolean[] found = new boolean[betAll.size()];  // 标记各注单是否匹配成功
                int foundCnt = 0;                            // 已匹配成功注单计数
                int maxRetries = 5;                          // 最大轮询次数
                int retryInterval = 2_000;                   // 每轮查询间隔时间（毫秒）
                int pageSize = 15;                           // 接口分页大小

                // 多轮查询已下注注单，确认是否已成功
                for (int attempt = 1; attempt <= maxRetries && foundCnt < betAll.size(); attempt++) {
                    long elapsed = System.currentTimeMillis() - fixStart;

                    // 超过最大判断时间，提前终止补单判断
                    if (elapsed >= maxTotalMillis) {
                        log.warn("开始补单确认检查,平台用户:{},盘口账号:{},期数:{},方案数:{},补单确认阶段超过期数关闭时间，提前结束查询，已匹配 {}/{} 注单",
                                username, sucAccount.getAccount(), drawNumber, plans.size(), foundCnt, betAll.size());
                        break;
                    }

                    // 若剩余时间不足5秒，避免被 OkHttp 阻塞，跳过当前轮
                    if (maxDurationMs - elapsed < 5000) {
                        log.warn("开始补单确认检查,平台用户:{},盘口账号:{},期数:{},方案数:{},剩余时间不足5秒，跳过第{}次注单确认查询",
                                username, sucAccount.getAccount(), drawNumber, plans.size(), attempt);
                        break;
                    }

                    int curPage = 1, totalPages = 1;

                    try {
                        // 分页查询注单列表
                        while (curPage <= totalPages && foundCnt < betAll.size()) {
                            if (System.currentTimeMillis() - fixStart >= maxTotalMillis) break;

                            long remainingTime = maxDurationMs - (System.currentTimeMillis() - fixStart);
                            int finalCurPage = curPage;

                            // 异步调用 settled() 接口查询注单，控制单次查询超时
                            CompletableFuture<JSONObject> future = CompletableFuture.supplyAsync(() ->
                                    JSONUtil.parseObj(settled(username, sucAccount.getId(), false, finalCurPage, false))
                            );

                            JSONObject settledPg;
                            try {
                                settledPg = future.get(remainingTime, TimeUnit.MILLISECONDS);
                            } catch (TimeoutException ex) {
                                log.info("开始补单确认检查,平台用户:{},盘口账号:{},期数:{},方案数:{},settled() 查询超时，终止当前页注单查询",
                                        username, sucAccount.getAccount(), drawNumber, plans.size());
                                break;
                            }

                            JSONArray pageList = settledPg.getJSONArray("list");

                            if (CollUtil.isNotEmpty(pageList)) {
                                if (curPage == 1) {
                                    int total = settledPg.getInt("total", 0);
                                    totalPages = Math.max(1, (total + pageSize - 1) / pageSize);
                                }

                                // 遍历当前页注单列表，逐个匹配订单中的下注注单
                                for (Object obj : pageList) {
                                    JSONObject jo = JSONUtil.parseObj(obj);
                                    String draw = jo.getStr("drawNumber", "");
                                    String play = jo.getStr("play", "");
                                    long amount = jo.getLong("amount", -1L);

                                    for (int i = 0; i < betAll.size(); i++) {
                                        if (!found[i]) {
                                            Bet b = betAll.get(i);
                                            String betPlay = getPlayOrderBets(b);
                                            if (draw.contains(drawNumber)
                                                    && play.contains(betPlay)
                                                    && amount == b.getAmount()) {
                                                found[i] = true;
                                                ++foundCnt;
                                                log.info("开始补单确认检查,平台用户:{},盘口账号:{},期数:{},方案数:{},匹配注单成功({}/{}): {}",
                                                        username, sucAccount.getAccount(), drawNumber, plans.size(), foundCnt, betAll.size(), b);
                                                break;
                                            }
                                        }
                                    }
                                }
                            }
                            curPage++;
                        }
                    } catch (Exception ex) {
                        log.warn("开始补单确认检查,平台用户:{},盘口账号:{},期数:{},方案数:{},注单确认查询异常，第{}次尝试失败，错误：{}",
                                username, sucAccount.getAccount(), drawNumber, plans.size(), attempt, ex.getMessage());
                    }

                    log.info("开始补单确认检查,平台用户:{},盘口账号:{},期数:{},方案数:{},注单确认进度: 第{}/{}次尝试，已匹配 {}/{} 注单",
                            username, sucAccount.getAccount(), drawNumber, plans.size(), attempt, maxRetries, foundCnt, betAll.size());

                    if (foundCnt >= betAll.size()) {
                        break;
                    }

                    if (attempt < maxRetries) {
                        if (System.currentTimeMillis() - fixStart + retryInterval >= maxTotalMillis) {
                            log.warn("开始补单确认检查,平台用户:{},盘口账号:{},期数:{},方案数:{},剩余时间不足，放弃后续轮询",
                                    username, sucAccount.getAccount(), drawNumber, plans.size());
                            break;
                        }
                        try {
                            Thread.sleep(retryInterval);
                        } catch (InterruptedException ex) {
                            Thread.currentThread().interrupt();
                            log.warn("开始补单确认检查,平台用户:{},盘口账号:{},期数:{},方案数:{},补单确认轮询被中断",
                                    username, sucAccount.getAccount(), drawNumber, plans.size());
                            break;
                        }
                    }
                }

                successBet = (foundCnt >= betAll.size());
            } else {
                log.warn("开始补单确认检查,平台用户:{},盘口账号:{},期数:{},方案数:{},订单注单列表为空，无法确认下注成功",
                        username, sucAccount.getAccount(), drawNumber, plans.size());
            }

            JSONObject resultObj = new JSONObject();
            if (successBet) {
                log.info("补单请求超时,但投注成功, 平台用户:{}, 账号:{}, 期数:{}, 方案数:{}", username, sucAccount.getAccount(), drawNumber, plans.size());
                resultObj.putOpt("status", 0);
                resultObj.putOpt("message", "补单成功");
            } else {
                log.info("补单请求超时,未投注成功, 平台用户:{}, 账号:{}, 期数:{}, 方案数:{}", username, sucAccount.getAccount(), drawNumber, plans.size());
                resultObj.putOpt("status", 1);
                resultObj.putOpt("message", "代理链接超时");
            }
            resultObj.putOpt("lottery", lottery);
            resultObj.putOpt("drawNumber", drawNumber);
            resultObj.putOpt("createTime", now);
            resultObj.putOpt("isRepair", 1);
            result = JSONUtil.toJsonStr(resultObj);
        } finally {
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }

        result = StringUtils.isEmpty(result) ? null : result;
        JSONObject resultJson = JSONUtil.parseObj(result);
        if (!resultJson.isEmpty()) {
            int status = resultJson.getInt("status");
            resultJson.putOpt("type", 1);
            if (status == 0) {
                log.info("补单成功, 平台用户:{}, 失败账号:{}, 对冲账号:{}, 期数:{}", username, failedAccount.getAccount(), sucAccount.getAccount(), drawNumber);
                redisson.getBucket(KeyUtil.genKey(
                        RedisConstants.USER_BET_PERIOD_RES_PREFIX,
                        lottery,
                        ToDayRangeUtil.getToDayRange(),
                        drawNumber,
                        "reverse",
                        username,
                        sucAccount.getAccount(),
                        "merge"
                )).set(resultJson, Duration.ofHours(24));

                String failedKey = KeyUtil.genKey(
                        RedisConstants.USER_BET_PERIOD_RES_PREFIX,
                        lottery,
                        ToDayRangeUtil.getToDayRange(),
                        drawNumber,
                        "failed",
                        username,
                        failedAccount.getAccount(),
                        "merge"
                );
                JSONObject failedJson = JSONUtil.parseObj(businessUserRedissonClient.getBucket(failedKey).get());
                if (null != failedJson && !failedJson.isEmpty()) {
                    failedJson.putOpt("repair", 1);
                    failedJson.putOpt("repairAccount", sucAccount.getAccount());
                    failedJson.putOpt("type", 1);
                    businessUserRedissonClient.getBucket(failedKey).set(failedJson, Duration.ofHours(24));
                }

                reverseCount.incrementAndGet();
                reverseUserCount.incrementAndGet();
                return true;
            } else if (status == 2) {
                if (closeTime - nowTimestamp <= 2000) {
                    resultJson.putOpt("message", "补单投注失败，当期已封盘");
                } else {
                    resultJson.putOpt("message", "补单投注超时");
                }
            }
        }

        if (!resultBol) {
            log.info("补单失败, 平台用户:{}, 失败账号:{}, 对冲账号:{}, 期数:{}, 返回信息{}", username, failedAccount.getAccount(), sucAccount.getAccount(), drawNumber, resultJson);
            resultJson.putOpt("confirm", 0);
            resultJson.putOpt("lottery", lottery);
            resultJson.putOpt("drawNumber", drawNumber);
            resultJson.putOpt("planName", "合并型方案");
            resultJson.putOpt("createTime", now);
            resultJson.putOpt("account", sucAccount.getAccount());
            resultJson.putOpt("isRepair", 1);
            resultJson.putOpt("type", 1);
            businessUserRedissonClient.getBucket(KeyUtil.genKey(
                    RedisConstants.USER_BET_PERIOD_RES_PREFIX,
                    lottery,
                    ToDayRangeUtil.getToDayRange(),
                    drawNumber,
                    "failed",
                    username,
                    sucAccount.getAccount(),
                    "merge"
            )).set(resultJson, Duration.ofHours(24));

            String failedKey = KeyUtil.genKey(
                    RedisConstants.USER_BET_PERIOD_RES_PREFIX,
                    lottery,
                    ToDayRangeUtil.getToDayRange(),
                    drawNumber,
                    "failed",
                    username,
                    failedAccount.getAccount(),
                    "merge"
            );
            JSONObject failedJson = JSONUtil.parseObj(businessUserRedissonClient.getBucket(failedKey).get());
            if (null != failedJson && !failedJson.isEmpty()) {
                failedJson.putOpt("repair", 0);
                failedJson.putOpt("repairAccount", (StringUtils.isBlank(failedJson.getStr("repairAccount")) ? "" : failedJson.getStr("repairAccount") + ",") + sucAccount.getAccount());
                failedJson.putOpt("type", 1);
                businessUserRedissonClient.getBucket(failedKey).set(failedJson, Duration.ofHours(24));
            }
        }

        return resultBol;
    }


    /**
     * 根据odds切割key
     * @param input
     * @return
     */
    public static List<String> oddsKyeSplitter(String input) {
        List<String> parts = new ArrayList<>();
        String[] splitPart = input.split("_");
        parts.add(splitPart[0]);// _前的部分
        parts.add(splitPart[1]);// _后的部分
        return parts;
    }

    /**
     * 获取正反投的数字
     * @param betNum
     * @return
     */
    public static Map<String, List<Integer>> generateBetContents(int betNum) {
        Map<String, List<Integer>> contents = new HashMap<>();
        // 用来存放正投和反投的数字
        Set<Integer> positiveContentsSet = new HashSet<>(); // 使用Set来避免重复
        List<Integer> reverseContents = new ArrayList<>();

        // 创建随机数生成器
        Random random = ThreadLocalRandom.current();

        // 生成 betNum 个 1 到 10 之间的随机数，存入Set来避免重复
        while (positiveContentsSet.size() < betNum) {
            int randomNum = random.nextInt(10) + 1;  // 生成1到10之间的随机数
            positiveContentsSet.add(randomNum);  // 添加到Set中
        }

        // 将Set转换为List以便返回
        List<Integer> positiveContents = new ArrayList<>(positiveContentsSet);

        // 计算剩余的数字，用于反投
        for (int i = 1; i <= 10; i++) {
            if (!positiveContentsSet.contains(i)) {
                reverseContents.add(i);  // 如果正投列表中没有此数字，则加入反投列表
            }
        }

        // 将结果添加到Map中
        contents.put("positive", positiveContents);
        contents.put("reverse", reverseContents);

        return contents;
    }

    public static void main(String[] args) {
        System.out.println(RandomUtil.randomStringLower(16));
        System.out.println(10000 - 5 * 1000);
        System.out.println(LocalDateTimeUtil.of(1753363351000L));
        Map<Integer, Map<String, List<String>>> oddsMap1 = new HashMap<>();
        Map<String, List<String>> map1 = new HashMap<>();
        List<String> pos1 = Arrays.asList("B1_1", "B1_2");
        List<String> rev1 = Arrays.asList("B1_3", "B1_4", "B1_5", "B1_6", "B1_7", "B1_8", "B1_9", "B1_10");
        map1.put("positive", pos1);
        map1.put("reverse", rev1);
        oddsMap1.put(1, map1);
        oddsMap1.put(2, map1);
        List<UserConfig> positiveAccounts1 = new ArrayList<>();
        positiveAccounts1.add(new UserConfig("account1", 2));
        positiveAccounts1.add(new UserConfig("account2", 2));
        positiveAccounts1.add(new UserConfig("account3", 2));
        List<UserConfig> reverseAccounts1 = new ArrayList<>();
        reverseAccounts1.add(new UserConfig("account4", 2));
        reverseAccounts1.add(new UserConfig("account5", 2));
        reverseAccounts1.add(new UserConfig("account6", 2));
        reverseAccounts1.add(new UserConfig("account7", 2));
        ConfigPlanVO plan1 = new ConfigPlanVO();
        plan1.setPositiveNum(2);
        plan1.setPositiveAmount(10);
        plan1.setReverseAmount(10);
        JSONObject resultJson1 = distributeAmount(oddsMap1, positiveAccounts1, reverseAccounts1, plan1);


        // 投注数
        int betNum = 8;
        // 投注金额
        long amount = 5;


        List<Integer> pos = new ArrayList<>();
        pos.add(1);
        pos.add(3);
        pos.add(4);

        JSONObject json = new JSONObject();
        json.putOpt("B1_01", 9.979);
        json.putOpt("B1_02", 9.979);
        json.putOpt("B1_03", 9.979);
        json.putOpt("B1_04", 9.979);
        json.putOpt("B2_01", 9.979);
        json.putOpt("B2_02", 9.979);
        json.putOpt("B2_03", 9.979);
        json.putOpt("B2_04", 9.979);

        int positive = 2; // 随机获取正样本数
        int reverse = 1;  // 随机获取反样本数

        pos.forEach(po -> {
            // 筛选出与当前 po 匹配的键值对
            List<String> matchedKeys = json.keySet().stream()
                    .filter(key -> key.contains("B" + po))
                    .collect(Collectors.toList());

            // 随机抽取正样本
            List<String> positiveKeys = getRandomElements(matchedKeys, positive);

            // 剩余键
            List<String> remainingKeys = new ArrayList<>(matchedKeys);
            remainingKeys.removeAll(positiveKeys);

            // 随机抽取反样本
            List<String> reverseKeys = getRandomElements(remainingKeys, reverse);

            // 打印结果
            System.out.println("Matched keys for B" + po + ": " + matchedKeys);
            System.out.println("Positive keys: " + positiveKeys);
            System.out.println("Reverse keys: " + reverseKeys);
        });

        List<UserConfig> userConfigs = new ArrayList<>();

        userConfigs.add(new UserConfig("1", 1));
        userConfigs.add(new UserConfig("2", 1));
        userConfigs.add(new UserConfig("3", 1));
        userConfigs.add(new UserConfig("4", 1));
        userConfigs.add(new UserConfig("5", 1));
        userConfigs.add(new UserConfig("6", 2));
        userConfigs.add(new UserConfig("7", 2));
        userConfigs.add(new UserConfig("8", 2));
        userConfigs.add(new UserConfig("9", 2));
        userConfigs.add(new UserConfig("10", 2));

        // 获取正投账号数
        List<UserConfig> positiveAccounts = getRandomAccount(userConfigs, 2, 1);
        // 获取反投账号数
        List<UserConfig> reverseAccounts = getRandomAccount(userConfigs, 3, 2);

        // 示例代码，用于测试
        List<String> items = new ArrayList<>();
        // 假设添加一些示例数据
        items.add("Item1");
        items.add("Item2");
        items.add("Item3");
        items.add("Item4");
        items.add("Item5");

        // 输出正反投结果
        Map<String, List<String>> odds = getOdds(items, betNum);
        // 切割赔率key
        oddsKyeSplitter("B1_01");
    }

    /**
     * 随机抽取指定数量的元素
     */
    private static List<String> getRandomElements(List<String> list, int betNum) {
        log.info("betNum:{} list.size:{}",betNum,list.size());
        if (list.size() < betNum) {
            System.out.println("Warning: Requested count (" + betNum + ") exceeds available items (" + list.size() + "). Returning all items.");
        }
        Random random = ThreadLocalRandom.current();
        List<String> result = new ArrayList<>();
        List<String> temp = new ArrayList<>(list);
        for (int i = 0; i < betNum && !temp.isEmpty(); i++) {
            int randomIndex = random.nextInt(temp.size());
            result.add(temp.remove(randomIndex));
        }
        log.info("指定赔率json key:{}",result);
        return result;
    }

    /**
     * 分配正反投位置， 根据betNum随机获取list中的数据赋值给positive，剩余数据则赋予reverse，随机获取的positive不能重复
     * @param list
     * @param betNum
     * @return
     */
    private static Map<String, List<String>> getOdds(List<String> list, int betNum) {
        Map<String, List<String>> odds = new HashMap<>();
        log.info("betNum:{} list.size:{}", betNum, list.size());

        // 如果请求的数量大于列表的大小，直接返回所有元素
        if (list.size() < betNum) {
            odds.put("positive", new ArrayList<>(list));
            odds.put("reverse", new ArrayList<>());
            return odds;
        }

        Random random = ThreadLocalRandom.current();
        List<String> temp = new ArrayList<>(list); // 创建一个临时副本来进行随机选择
        List<String> positive = new ArrayList<>();
        List<String> reverse = new ArrayList<>();

        // 随机选取betNum个元素作为positive
        for (int i = 0; i < betNum; i++) {
            int randomIndex = random.nextInt(temp.size()); // 从剩余的元素中随机选择
            positive.add(temp.remove(randomIndex)); // 移除已选中的元素，避免重复
        }

        // 剩下的元素作为reverse
        reverse.addAll(temp);

        // 将正反投结果存入Map
        odds.put("positive", positive);
        odds.put("reverse", reverse);

        log.info("正投赔率key:{}", positive);
        log.info("反投赔率key:{}", reverse);
        return odds;
    }

    public Map<String, Map<String, List<String>>> assignPositions(List<UserConfig> positiveAccounts, List<UserConfig> reverseAccounts,
                                List<String> twoSidedDxPositions, List<String> twoSidedDsPositions,
                                List<String> twoSidedLhPositions) {
        Random random = ThreadLocalRandom.current();
        Map<String, Map<String, List<String>>> selections = new HashMap<>();
        // 存储正投和反投结果
        Map<String, List<String>> positiveSelections = new HashMap<>();
        Map<String, List<String>> reverseSelections = new HashMap<>();

        // 记录已分配过的位置，避免重复分配
        Set<String> allocatedPositions = new HashSet<>();

        // 处理所有投项
        processPositions(twoSidedDxPositions, positiveAccounts, reverseAccounts, "_D", "_X", positiveSelections, reverseSelections, allocatedPositions, random);
        processPositions(twoSidedDsPositions, positiveAccounts, reverseAccounts, "_D", "_S", positiveSelections, reverseSelections, allocatedPositions, random);
        processPositions(twoSidedLhPositions, positiveAccounts, reverseAccounts, "_L", "_H", positiveSelections, reverseSelections, allocatedPositions, random);

        selections.put("positive", positiveSelections);
        selections.put("reverse", reverseSelections);
        return selections;
    }

    private void processPositions(List<String> positions, List<UserConfig> positiveAccounts, List<UserConfig> reverseAccounts,
                                  String suffixPositiveKey, String suffixReverseKey,
                                  Map<String, List<String>> positiveSelections, Map<String, List<String>> reverseSelections,
                                  Set<String> allocatedPositions, Random random) {

        // 按照基础位置（不含后缀）对投项进行分组
        Map<String, List<String>> groupedPositions = new HashMap<>();
        for (String pos : positions) {
            String basePosition = pos.substring(0, pos.lastIndexOf('_')); // 提取基础位置
            groupedPositions.computeIfAbsent(basePosition, k -> new ArrayList<>()).add(pos);
        }

        // 对每个基础位置的投项进行处理
        for (Map.Entry<String, List<String>> entry : groupedPositions.entrySet()) {
            String basePosition = entry.getKey();
            List<String> positionList = entry.getValue();

            // 确保每个位置只分配一次
            if (allocatedPositions.contains(basePosition)) {
                continue; // 如果该位置已分配，跳过
            }

            // 随机决定正投和反投的后缀
            boolean nextBoolean = random.nextBoolean();
            String selectedPositiveKey = nextBoolean ? suffixPositiveKey : suffixReverseKey;
            String selectedReverseKey = nextBoolean ? suffixReverseKey : suffixPositiveKey;

            // 存储正投和反投的账户列表
            List<String> positiveSelection = new ArrayList<>();
            List<String> reverseSelection = new ArrayList<>();

            // 遍历该位置的所有投项并进行分配
            for (String pos : positionList) {
                // 为正投选择账户
                positiveAccounts.forEach(account -> {
                    if (pos.endsWith(selectedPositiveKey)) {
                        positiveSelection.add(account.getAccount());
                    }
                });

                // 为反投选择账户
                reverseAccounts.forEach(account -> {
                    if (pos.endsWith(selectedReverseKey)) {
                        reverseSelection.add(account.getAccount());
                    }
                });
            }

            // 如果正投选择不为空，则记录该选择
            if (!positiveSelection.isEmpty()) {
                positiveSelections.put(basePosition + selectedPositiveKey, positiveSelection);
            }

            // 如果反投选择不为空，则记录该选择
            if (!reverseSelection.isEmpty()) {
                reverseSelections.put(basePosition + selectedReverseKey, reverseSelection);
            }

            // 标记该基础位置已分配
            allocatedPositions.add(basePosition);
        }
    }

    /**
     * 随机抽取指定数量的投注账户
     */
    private static List<UserConfig> getRandomAccount(List<UserConfig> list, int betNum, int betType) {
        log.info("betNum:{} list.size:{} betType:{}", betNum, list.size(), betType);

        // 过滤出符合betType的UserConfig列表
        List<UserConfig> filteredList = new ArrayList<>();
        for (UserConfig userConfig : list) {
            if (betType == userConfig.getBetType()) {
                filteredList.add(userConfig);
            }
        }

        // 如果过滤后的list的大小小于betNum，则返回整个过滤后的list
        if (filteredList.size() <= betNum) {
            log.info("过滤后的list的大小小于betNum，则返回整个过滤后的list 指定账户json key:{}", filteredList);
            return filteredList;
        }

        Random random = ThreadLocalRandom.current();
        List<UserConfig> result = new ArrayList<>();
        List<UserConfig> temp = new ArrayList<>(filteredList); // 创建一个临时副本来进行随机选择

        // 随机选取betNum个元素
        for (int i = 0; i < betNum; i++) {
            int randomIndex = random.nextInt(temp.size()); // 从剩余的元素中随机选择
            result.add(temp.remove(randomIndex)); // 移除已选中的元素，避免重复
        }

        log.info("指定账户json key:{}", result);
        return result;
    }

    /**
     * 金额分配逻辑，首先遍历oddsMap，获取oddsMap下每个位置key，比如下标1有positive值的B1_1、B1_2、B1_3和reverse值的B1_4、B1_5、B1_6，然后根据userConfigs里的betType对应的账户，然后根据plan的betNum和oddsMap下每个位置key的数量，计算每个账户应该分配的金额，最后将金额分配给每个账户
     *
      */
    public static JSONObject distributeAmount(Map<Integer, Map<String, List<String>>> oddsMap, List<UserConfig> positiveAccounts, List<UserConfig> reverseAccounts, ConfigPlanVO plan) {
        JSONObject resultJson = new JSONObject();

        // 遍历 oddsMap 中的每个 odds 数据
        oddsMap.forEach((oddsKey, v) -> {
            // 分配 positive 账户的金额
            JSONObject posJson = new JSONObject();
            List<String> posOddsKey = v.get("positive");
            posOddsKey.forEach(odds -> {
                JSONObject amountJson = distributeAmountsForAccounts(positiveAccounts, plan.getPositiveAmount());
                posJson.putOpt(odds, amountJson);
            });

            // 分配 reverse 账户的金额
            JSONObject revJson = new JSONObject();
            List<String> revOddsKey = v.get("reverse");
            revOddsKey.forEach(odds -> {
                JSONObject amountJson = distributeAmountsForAccounts(reverseAccounts, plan.getReverseAmount());
                revJson.putOpt(odds, amountJson);
            });

            // 创建一个对象包含 pos 和 rev 的分配结果
            JSONObject posRevJson = new JSONObject();
            posRevJson.putOpt("positive", posJson);
            posRevJson.putOpt("reverse", revJson);

            // 将 oddsKey 对应的结果放入最终的结果中
            resultJson.putOpt(String.valueOf(oddsKey), posRevJson);
        });

        return resultJson;
    }

    /**
     * 双面金额分配
     * @param oddsMap
     * @param positiveAccounts
     * @param reverseAccounts
     * @param plan
     * @return
     */
    public static JSONObject twoSidedDistributeAmount(Map<String, Map<String, List<String>>> oddsMap, List<UserConfig> positiveAccounts, List<UserConfig> reverseAccounts, ConfigPlanVO plan) {
        // 遍历 oddsMap 中的每个 odds 数据
        // 分配 positive 账户的金额
        JSONObject posJson = new JSONObject();
        Map<String, List<String>> posOddsKey = oddsMap.get("positive");
        posOddsKey.forEach((odds, accounts) -> {
            JSONObject amountJson = distributeAmountsForAccounts(positiveAccounts, plan.getPositiveAmount());
            posJson.putOpt(odds, amountJson);
        });

        // 分配 reverse 账户的金额
        JSONObject revJson = new JSONObject();
        Map<String, List<String>> revOddsKey = oddsMap.get("reverse");
        revOddsKey.forEach((odds, accounts) -> {
            JSONObject amountJson = distributeAmountsForAccounts(reverseAccounts, plan.getReverseAmount());
            revJson.putOpt(odds, amountJson);
        });

        // 创建一个对象包含 pos 和 rev 的分配结果
        JSONObject posRevJson = new JSONObject();
        posRevJson.putOpt("positive", posJson);
        posRevJson.putOpt("reverse", revJson);

        return posRevJson;
    }

    private static JSONObject distributeAmountsForAccounts(List<UserConfig> accounts, int totalAmount) {
        JSONObject amountJson = new JSONObject();
        int betNum = accounts.size();

        if (totalAmount <= 0) {
            log.info("投注金额必须大于0");
            return amountJson;
        }

        if (betNum == 0) {
            log.info("没有可用的投注账号");
            return amountJson;
        }

        // 初始分配：保证每个账号至少分配2个单位
        int initialShare = 1;
        int remainingAmount = totalAmount - betNum * initialShare;

        if (remainingAmount < 0) {
            log.info("金额不足以保证每个投注至少 1 元");
            return amountJson;
        }

        // 随机分配权重
        int[] weights = new int[betNum];
        int totalWeight = 0;
        Random random = ThreadLocalRandom.current();
        for (int i = 0; i < betNum; i++) {
            weights[i] = random.nextInt(100) + 1; // 确保权重为正整数
            totalWeight += weights[i];
        }

        // 分配剩余金额
        int[] distributedAmounts = new int[betNum];
        int distributedSum = 0;
        for (int i = 0; i < betNum; i++) {
            if (i == betNum - 1) {
                // 最后一个账号分配剩余金额，避免误差
                distributedAmounts[i] = remainingAmount - distributedSum;
            } else {
                distributedAmounts[i] = (int) Math.floor((double) remainingAmount * weights[i] / totalWeight);
                distributedSum += distributedAmounts[i];
            }
        }

        // 最终金额分配
        for (int i = 0; i < betNum; i++) {
            int finalAmount = distributedAmounts[i] + initialShare;
            amountJson.putOpt(accounts.get(i).getAccount(), finalAmount);
        }

        // 校验并修正总金额
        int actualTotal = amountJson.values().stream().mapToInt(value -> (int) value).sum();
        if (actualTotal != totalAmount) {
            log.warn("分配金额:{}不等于配置金额:{},进行强制修正", actualTotal, totalAmount);
            int adjustment = totalAmount - actualTotal;

            if (adjustment > 0) {
                // 情况1：actualTotal < totalAmount
                String lastAccount = accounts.get(betNum - 1).getAccount();
                int correctedAmount = amountJson.getInt(lastAccount) + adjustment;

                if (correctedAmount > 0) {
                    amountJson.putOpt(lastAccount, correctedAmount);
                } else {
                    log.warn("分配金额小于等于0，进行强制修正");
                    // 强制修正逻辑：分配最小值2，剩余分散到其他账户
                    int extraAdjustment = -correctedAmount;
                    amountJson.putOpt(lastAccount, initialShare); // 最小分配5
                    for (int i = 0; i < betNum - 1; i++) {
                        String account = accounts.get(i).getAccount();
                        int currentAmount = amountJson.getInt(account);
                        if (currentAmount > 1) {
                            int toAdjust = Math.min(extraAdjustment, currentAmount - 1);
                            amountJson.putOpt(account, currentAmount - toAdjust);
                            extraAdjustment -= toAdjust;
                            if (extraAdjustment == 0) break;
                        }
                    }
                    if (extraAdjustment > 0) {
                        log.warn("无法完全分散调整金额，当前剩余差值: {}", extraAdjustment);
                    }
                }
            } else {
                // 情况2：actualTotal > totalAmount
                adjustment = -adjustment; // 转为正值处理
                log.warn("分配金额超出预算，进行调整");

                // 从金额最大的账号开始调整
                Map<String, Integer> amountMap = new HashMap<>();
                for (String key : amountJson.keySet()) {
                    amountMap.put(key, amountJson.getInt(key));
                }
                List<Map.Entry<String, Integer>> sortedAccounts = amountMap.entrySet().stream()
                        .sorted((a, b) -> Integer.compare(b.getValue(), a.getValue())) // 降序排序
                        .toList();

                for (Map.Entry<String, Integer> entry : sortedAccounts) {
                    String account = entry.getKey();
                    int currentAmount = entry.getValue();

                    if (currentAmount > 1) {
                        int toAdjust = Math.min(adjustment, currentAmount - 1);
                        amountJson.putOpt(account, currentAmount - toAdjust);
                        adjustment -= toAdjust;
                    }

                    if (adjustment == 0) break;
                }

                if (adjustment > 0) {
                    log.error("调整后仍有剩余差值，逻辑可能有问题，剩余差值: {}", adjustment);
                }
            }
        }

        // 再次校验，确保所有分配金额大于2
        for (int i = 0; i < betNum; i++) {
            String account = accounts.get(i).getAccount();
            int amount = amountJson.getInt(account);
            if (amount < initialShare) {
                log.error("分配金额出现小于5的情况 accounts大小:{}, totalAmount:{}, 分配结果:{}", accounts.size(), totalAmount, amountJson);
            }
        }
        return amountJson;
    }

    /**
     * 自动刷新今日汇总 - 用于防止盘口token失效
     */
    public void autoRefreshBalance() {
        TimeInterval timer = DateUtil.timer();
        List<AdminLoginDTO> adminUsers = getAutoBetAdminUsers();
        // 如果没有有效用户，直接退出
        if (CollUtil.isEmpty(adminUsers)) {
            return;
        }

        // 基于任务总量和 CPU 核数动态调整线程池大小
        int cpuCoreCount = Runtime.getRuntime().availableProcessors();
        int threadPoolSize = Math.min(adminUsers.size(), Math.max(cpuCoreCount * 2, 100));
        log.info("自动刷新账号额度 cpu核数: {}，配置线程池大小: {}", cpuCoreCount, threadPoolSize);

        // 使用一个大的线程池来管理所有任务
        ExecutorService executorService = Executors.newFixedThreadPool(threadPoolSize);

        // 创建一个List来存储所有的 CompletableFuture 任务
        List<CompletableFuture<Void>> futures = adminUsers.stream()
                .map(admin -> CompletableFuture.runAsync(() -> {
                    List<UserConfig> accounts = configService.accounts(admin.getUsername(), null);
                    if (CollUtil.isEmpty(accounts)) {
                        return;
                    }
                    List<UserConfig> tokenValidConfigs = accounts.stream()
                            .filter(userConfig -> 1 == userConfig.getIsTokenValid() && StringUtils.isNotBlank(userConfig.getToken()))
                            .toList();
                    if (CollUtil.isEmpty(tokenValidConfigs)) {
                        return;
                    }

                    // 处理账户列表的任务
                    processTokenValidConfigs(admin, tokenValidConfigs, cpuCoreCount);

                }, executorService))  // 提交给线程池
                .toList();

        // 等待所有的 CompletableFuture 完成
        CompletableFuture<Void> allOf = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
        allOf.join();  // 阻塞当前线程，直到所有任务完成

        // 关闭线程池
        shutdownExecutor(executorService);
        log.info("自动刷新账号额度 平台用户数:{}，总刷新花费:{}毫秒", adminUsers.size(), timer.interval());
    }

    /**
     * 获取每个 admin 用户下的有效账户额度任务
     * @param admin 当前 admin 用户
     * @param tokenValidConfigs 该 admin 下的有效账户列表
     * @param cpuCoreCount 系统 CPU 核心数，用来计算线程池大小
     */
    public void processTokenValidConfigs(AdminLoginDTO admin, List<UserConfig> tokenValidConfigs, int cpuCoreCount) {
        // 基于任务总量和 CPU 核数动态调整线程池大小
        int threadPoolAccountSize = Math.min(tokenValidConfigs.size(), Math.max(cpuCoreCount * 2, 10));
        ExecutorService executorService = Executors.newFixedThreadPool(threadPoolAccountSize);

        // 为每个有效账户创建 CompletableFuture 任务
        List<CompletableFuture<Void>> futuresAccount = tokenValidConfigs.stream()
                .map(account -> CompletableFuture.runAsync(() -> {
                    try {
                        // 更新账户信息
                        account(admin.getUsername(), account.getId());
                    } catch (Exception e) {
                        log.error("获取平台用户:{} 账号:{}额度失败", admin.getUsername(), account.getAccount(), e);
                    }
                }, executorService))
                .toList();

        // 等待所有账户的任务完成
        CompletableFuture<Void> allAccount = CompletableFuture.allOf(futuresAccount.toArray(new CompletableFuture[0]));
        allAccount.join();  // 阻塞当前线程，直到所有任务完成

        // 关闭账户线程池
        shutdownExecutor(executorService);
    }

    /**
     * 获取今日汇总
     * @return
     */
    public List<UserConfig> summaryToday(String username, String startDate, String endDate) {
        TimeInterval timer = DateUtil.timer();
        AdminLoginDTO adminLoginDTO = configService.getUser(username);
        boolean isAgent = adminLoginDTO.getRoles().contains("agent");
        List<UserConfig> accounts = configService.accounts(username, null);
        if (CollUtil.isEmpty(accounts)) {
            return Collections.emptyList();
        }
        List<UserConfig> tokenValidConfigs = accounts.stream()
                .filter(userConfig -> 1 == userConfig.getIsTokenValid() && StringUtils.isNotBlank(userConfig.getToken()))
                .toList();
        if (CollUtil.isEmpty(tokenValidConfigs)) {
            return Collections.emptyList();
        }
        int keysCount = tokenValidConfigs.size();
        // 基于任务总量和 CPU 核数动态调整线程池大小
        int cpuCoreCount = Runtime.getRuntime().availableProcessors();
        int threadPoolSize = Math.min(keysCount, Math.max(cpuCoreCount * 2, 10));
        log.info("获取今日汇总 cpu核数: {}，配置线程池大小: {}", cpuCoreCount, threadPoolSize);
        // 初始化线程池（动态线程池，避免任务阻塞）
        ExecutorService executorService = Executors.newFixedThreadPool(threadPoolSize);

        // 创建一个List来存储所有的 CompletableFuture 任务
        List<CompletableFuture<Void>> futures = tokenValidConfigs.stream()
            .map(account -> CompletableFuture.runAsync(() -> {
            try {
                // 更新余额和未结算金额
                JSONObject balance = account(username, account.getId());
                if (balance.isEmpty()) {
                    account.setBalance(BigDecimal.ZERO);
                    account.setBetting(BigDecimal.ZERO);
                } else {
                    account.setBalance(balance.getBigDecimal("YuWo"));
                }
                // 获取账号今日流水
                JSONObject settled = JSONUtil.parseObj(settled(username, account.getId(), true, 1, true));
                // 更新网络请求延迟
                account.setDelayMs(settled.getLong("delayMs"));
                JSONArray list = settled.getJSONArray("list");
                if (list.isEmpty()) {
                    account.setAmount(BigDecimal.ZERO);
                    account.setResult(BigDecimal.ZERO);
                } else {
                    list.forEach(object -> {
                        // 获取账号今日汇总
                        JSONObject jsonObject = JSONUtil.parseObj(object);
                        // 获取今日流水和盈亏
                        jsonObject.forEach((key, value) -> {
                            if (jsonObject.containsKey("account") && jsonObject.containsKey("totalAmount")) {
                                // 总金额
                                account.setAmount(jsonObject.getBigDecimal("totalAmount"));
                                account.setResult(jsonObject.getBigDecimal("totalResult"));
                            }
                        });
                    });
                }
                if (isAgent) {
                    try {
                        JSONArray jsonArray = agent(username, account.getId(), startDate, endDate);
                        if (!jsonArray.isEmpty()) {
                            final BigDecimal[] totalForceAmount = {BigDecimal.ZERO};
                            final BigDecimal[] totalReceivable = {BigDecimal.ZERO};
                            final BigDecimal[] totalForceAmountTwoSided = {BigDecimal.ZERO};
                            final BigDecimal[] totalReceivableTwoSided = {BigDecimal.ZERO};
                            final BigDecimal[] totalForceAmountNonTwoSided = {BigDecimal.ZERO};
                            final BigDecimal[] totalReceivableNonTwoSided = {BigDecimal.ZERO};

                            jsonArray.forEach(object -> {
                                JSONObject jsonObject = JSONUtil.parseObj(object);
                                // 获取 forceAmount 和 receivable，默认值为 BigDecimal.ZERO
                                BigDecimal forceAmount = jsonObject.getBigDecimal("forceAmount", BigDecimal.ZERO);
                                BigDecimal receivable = jsonObject.getBigDecimal("receivable", BigDecimal.ZERO);
                                if ("两面".equals(jsonObject.getStr("play"))) {
                                    // 叠加
                                    totalForceAmountTwoSided[0] = totalForceAmountTwoSided[0].add(forceAmount);
                                    totalReceivableTwoSided[0] = totalReceivableTwoSided[0].add(receivable);
                                } else if ("1-10车号".equals(jsonObject.getStr("play"))) {
                                    totalForceAmountNonTwoSided[0] = totalForceAmountNonTwoSided[0].add(forceAmount);
                                    totalReceivableNonTwoSided[0] = totalReceivableNonTwoSided[0].add(receivable);
                                }
                                // 总叠加
                                totalForceAmount[0] = totalForceAmount[0].add(forceAmount);
                                totalReceivable[0] = totalReceivable[0].add(receivable);
                            });

                            account.setForceAmountTwoSided(customRound(totalForceAmountTwoSided[0]));
                            account.setReceivableTwoSided(customRound(totalReceivableTwoSided[0]));
                            account.setForceAmountNonTwoSided(customRound(totalForceAmountNonTwoSided[0]));
                            account.setReceivableNonTwoSided(customRound(totalReceivableNonTwoSided[0]));
                            account.setForceAmount(customRound(totalForceAmount[0]));
                            account.setReceivable(customRound(totalReceivable[0]));
                        } else {
                            account.setForceAmountTwoSided(BigDecimal.ZERO);
                            account.setReceivableTwoSided(BigDecimal.ZERO);
                            account.setForceAmountNonTwoSided(BigDecimal.ZERO);
                            account.setReceivableNonTwoSided(BigDecimal.ZERO);
                            account.setForceAmount(BigDecimal.ZERO);
                            account.setReceivable(BigDecimal.ZERO);
                        }
                    } catch (Exception e) {
                        account.setForceAmountTwoSided(BigDecimal.ZERO);
                        account.setReceivableTwoSided(BigDecimal.ZERO);
                        account.setForceAmountNonTwoSided(BigDecimal.ZERO);
                        account.setReceivableNonTwoSided(BigDecimal.ZERO);
                        account.setForceAmount(BigDecimal.ZERO);
                        account.setReceivable(BigDecimal.ZERO);
                        log.error("获取平台用户:{} 账号:{} 代理报表失败", username, account.getAccount(), e);
                    }
                }
                account.setUpdateTime(LocalDateTimeUtil.format(LocalDateTime.now(), DatePattern.NORM_DATETIME_PATTERN));
                // 更新流水 Redis 缓存
                updateRedisCache(username, account);
            } catch (Exception e) {
                log.error("获取平台用户:{} 账号:{}今日汇总失败", username, account.getAccount(), e);
            }
        }, executorService)).toList();  // 提交给线程池
        // 等待所有的 CompletableFuture 完成
        CompletableFuture<Void> allOf = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
        allOf.join();  // 阻塞当前线程，直到所有任务完成
        // 关闭线程池
        shutdownExecutor(executorService);
        log.info("获取今日汇总 平台用户:{}，刷新花费:{}毫秒", username, timer.interval());
        return accounts;
    }

    /**
     * 自定义四舍五入规则：
     * 小数部分 < 0.25 向下舍入
     * 小数部分 >= 0.25 向上舍入
     */
    private BigDecimal customRound(BigDecimal value) {
        if (value == null) return BigDecimal.ZERO;

        // 获取整数部分和小数部分
        BigDecimal integerPart = value.setScale(0, RoundingMode.DOWN);
        BigDecimal fractionalPart = value.subtract(integerPart);

        // 提取第二位小数
        BigDecimal secondFractional = fractionalPart.remainder(BigDecimal.TEN);

        // 根据规则判断第二位小数四舍五入
        if (secondFractional.compareTo(new BigDecimal("5")) >= 0) {
            return integerPart.add(BigDecimal.ONE).setScale(1, RoundingMode.HALF_UP);
        } else {
            return integerPart.add(fractionalPart).setScale(1, RoundingMode.HALF_UP);
        }
    }


    private void updateRedisCache(String username, UserConfig account) {
        String cacheKey = KeyUtil.genKey(RedisConstants.USER_PROXY_PREFIX, username, account.getId());
        businessUserRedissonClient.getBucket(cacheKey).set(JSONUtil.toJsonStr(account));
    }

    private String getAccountType(int type) {
        return switch (type) {
            case 0 -> "A";
            case 1 -> "B";
            case 2 -> "C";
            case 3 -> "D";
            default -> "UNKNOWN";
        };
    }

    /**
     * 获取今日未结/已结明细（新API）
     * @param username 用户名
     * @param id 账号ID
     * @param settled true=已结明细，false=未结明细
     * @param pageNo 页码（从0开始）
     * @param isSummary 是否汇总
     * @return JSON字符串
     */
    public String settled(String username, String id, Boolean settled, Integer pageNo, Boolean isSummary) {
        // 获取用户配置
        List<UserConfig> userConfigs = configService.accounts(username, id);
        if (CollectionUtil.isEmpty(userConfigs)) {
            throw new BusinessException(SystemError.USER_1007);
        }
        UserConfig userConfig = userConfigs.get(0);
        String baseUrl = userConfig.getBaseUrl();
        String token = userConfig.getToken();
        
        if (pageNo == null) {
            pageNo = 0; // 新API页码从0开始
        } else {
            // 前端传递的页码通常从1开始，需要转换为从0开始
            pageNo = pageNo - 1;
            if (pageNo < 0) {
                pageNo = 0;
            }
        }
        
        // 构建新API URL
        String url;
        Map<String, String> headers = new HashMap<>();
        
        if (Boolean.TRUE.equals(settled)) {
            // 今日已结明细：app/control/Report.php?uid=xxx&dts=2026-01-15&dte=2026-01-15&active=1&gtype=0&pages=0
            String today = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
            url = baseUrl + "app/control/Report.php?uid=" + token + "&dts=" + today + "&dte=" + today + "&active=1&gtype=0&pages=" + pageNo;
        } else {
            // 今日未结明细：app/control/betDetail.php?uid=xxx&active=list&gametype=9&pages=0
            url = baseUrl + "app/control/betDetail.php?uid=" + token + "&active=list&gametype=9&pages=" + pageNo;
        }

        // 设置请求头
        headers.put("accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7");
        headers.put("accept-language", "zh-CN,zh;q=0.9");
        headers.put("cache-control", "no-cache");
        headers.put("pragma", "no-cache");
        headers.put("priority", "u=0, i");
        headers.put("referer", baseUrl + "app/control/" + (Boolean.TRUE.equals(settled) ? "Report.php" : "betDetail.php") + "?uid=" + token);
        headers.put("sec-ch-ua", "\"Google Chrome\";v=\"143\", \"Chromium\";v=\"143\", \"Not A(Brand\";v=\"24\"");
        headers.put("sec-ch-ua-mobile", "?0");
        headers.put("sec-ch-ua-platform", "\"Windows\"");
        headers.put("sec-fetch-dest", "iframe");
        headers.put("sec-fetch-mode", "navigate");
        headers.put("sec-fetch-site", "same-origin");
        headers.put("sec-fetch-user", "?1");
        headers.put("upgrade-insecure-requests", "1");
        headers.put("user-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36");
        
        // 设置Cookie（新API需要的Cookie格式）
        // 注意：PHPSESSID应该从登录响应中获取，这里暂时使用token
        // 如果登录时返回了PHPSESSID，应该存储在userConfig中
        String cookie = "gtype=9; nowpage=game.html; PHPSESSID=" + token;
        headers.put("cookie", cookie);

        String result;
        long delayMs;
        try {
            // 用okHttp代理调度器发起GET请求
            OkHttpProxyDispatcher.HttpResult resultHttp = dispatcher.execute("GET", url, null, headers, userConfig, false);
            result = StringUtils.isNotBlank(resultHttp.getBody()) ? resultHttp.getBody() : null;
            delayMs = resultHttp.getDurationMs();
        } catch (Exception e) {
            Throwable cause = e.getCause();
            if (cause instanceof UnknownHostException) {
                log.error("代理请求失败：主机未知。可能是域名解析失败或代理地址有误。异常信息：{}", e.getMessage(), e);
                throw new BusinessException(SystemError.SYS_419, userConfig.getAccount(), "代理请求失败");
            } else if (cause instanceof ConnectException) {
                log.error("代理请求失败：连接异常。可能是代理服务器未开启或网络不可达。异常信息：{}", e.getMessage(), e);
                throw new BusinessException(SystemError.SYS_419, userConfig.getAccount(), "连接异常");
            } else if (cause instanceof SocketTimeoutException) {
                log.error("代理请求失败：连接超时。可能是网络延迟过高或代理服务器响应缓慢。异常信息：{}", e.getMessage(), e);
                throw new BusinessException(SystemError.SYS_419, userConfig.getAccount(), "连接超时");
            } else if (cause instanceof IOException) {
                log.error("代理请求失败：IO异常。可能是数据传输错误或代理配置不正确。异常信息：{}", e.getMessage(), e);
                throw new BusinessException(SystemError.SYS_419, userConfig.getAccount(), "IO异常");
            } else {
                log.error("代理请求失败：未知异常。异常信息：{}", e.getMessage(), e);
                throw new BusinessException(SystemError.SYS_419, userConfig.getAccount(), "未知异常");
            }
        }

        // 解析HTML中的JavaScript变量dataArr
        JSONObject resultJson = new JSONObject();
        JSONArray tableData = new JSONArray();
        resultJson.putOpt("delayMs", delayMs);

        try {
            // 从HTML中提取JavaScript变量dataArr
            // 匹配格式：var dataArr = [{...}]; 或 var dataArr = [];
            Pattern dataArrPattern = Pattern.compile("var\\s+dataArr\\s*=\\s*(\\[.*?\\]);", Pattern.DOTALL);
            Matcher dataArrMatcher = dataArrPattern.matcher(result);
            
            if (dataArrMatcher.find()) {
                String dataArrStr = dataArrMatcher.group(1);
                // 处理空数组
                if (dataArrStr.trim().equals("[]")) {
                    log.info("新API返回空数组，无注单数据");
                } else {
                    // 解析JSON数组
                    JSONArray dataArr = JSONUtil.parseArray(dataArrStr);
                    
                    // 解析每个注单数据
                    for (Object obj : dataArr) {
                        JSONObject dx = JSONUtil.parseObj(obj);
                        JSONObject rowData = new JSONObject();
                        
                        // 账号
                        rowData.putOpt("account", userConfig.getAccount());
                        // 注单号
                        rowData.putOpt("orderNo", dx.getStr("id"));
                        // 时间
                        rowData.putOpt("time", dx.getStr("adddate"));
                        // 游戏类型
                        rowData.putOpt("type", dx.getStr("gname"));
                        // 期号
                        rowData.putOpt("drawNumber", dx.getStr("round"));
                        // 下注类型
                        rowData.putOpt("play", dx.getStr("wname"));
                        // 盘口（ltype: 1=A, 2=B, 3=C, ...）
                        String ltypeStr = getLtypeString(dx.getInt("ltype", 1));
                        rowData.putOpt("range", ltypeStr + "盘");
                        // 注单明细
                        rowData.putOpt("detail", dx.getStr("text") + "@" + dx.getStr("rate"));
                        // 下注金额
                        rowData.putOpt("amount", dx.getStr("amount"));
                        // 退水(%)
                        if (Boolean.TRUE.equals(settled)) {
                            rowData.putOpt("rebate", dx.getStr("wgold")); // 已结明细使用wgold
                        } else {
                            rowData.putOpt("rebate", dx.getStr("back")); // 未结明细使用back
                        }
                        // 状态/结果
                        if (Boolean.TRUE.equals(settled)) {
                            // 已结明细：rame=2已取消，rame=5和局，其他显示退水后结果
                            int rame = dx.getInt("rame", 0);
                            if (rame == 2) {
                                rowData.putOpt("result", "已取消");
                            } else if (rame == 5) {
                                rowData.putOpt("result", "和局");
                            } else {
                                // 退水后结果 = wingold + wgold
                                BigDecimal wingold = dx.getBigDecimal("wingold", BigDecimal.ZERO);
                                BigDecimal wgold = dx.getBigDecimal("wgold", BigDecimal.ZERO);
                                BigDecimal resultAmount = wingold.add(wgold);
                                rowData.putOpt("result", resultAmount.toString());
                            }
                        } else {
                            // 未结明细：rame=0有效，rame=2已取消
                            int rame = dx.getInt("rame", 0);
                            if (rame == 2) {
                                rowData.putOpt("result", "已取消");
                            } else {
                                rowData.putOpt("result", "有效");
                            }
                        }
                        
                        // 额外字段（用于前端显示）
                        rowData.putOpt("playtype", dx.getStr("playtype"));
                        rowData.putOpt("wtype", dx.getStr("wtype"));
                        rowData.putOpt("rate", dx.getStr("rate"));
                        rowData.putOpt("wingold", dx.getStr("wingold"));
                        rowData.putOpt("mutinum", dx.getStr("mutinum"));
                        
                        tableData.add(rowData);
                    }
                }
            } else {
                log.warn("新API返回的HTML中未找到dataArr变量");
            }
            
            // 解析分页信息
            Pattern amountPagePattern = Pattern.compile("var\\s+amount_page\\s*=\\s*(\\d+)");
            Pattern nowPagePattern = Pattern.compile("var\\s+nowPage\\s*=\\s*(\\d+)\\s*\\+\\s*1");
            Pattern totalPattern = Pattern.compile("共\\s+(\\d+)\\s+条记录");
            
            Matcher amountPageMatcher = amountPagePattern.matcher(result);
            Matcher nowPageMatcher = nowPagePattern.matcher(result);
            Matcher totalMatcher = totalPattern.matcher(result);
            
            int totalPages = 1;
            int currentPage = pageNo + 1;
            int total = 0;
            
            if (amountPageMatcher.find()) {
                totalPages = Integer.parseInt(amountPageMatcher.group(1));
            }
            if (nowPageMatcher.find()) {
                currentPage = Integer.parseInt(nowPageMatcher.group(1));
            }
            if (totalMatcher.find()) {
                total = Integer.parseInt(totalMatcher.group(1));
            }
            
            resultJson.putOpt("list", tableData);
            resultJson.putOpt("total", total);
            resultJson.putOpt("pageSize", 15); // 默认每页15条
            resultJson.putOpt("currentPage", currentPage);
            resultJson.putOpt("totalPages", totalPages);
            
            // 如果是汇总模式，计算总计
            if (Boolean.TRUE.equals(isSummary)) {
                BigDecimal totalAmount = BigDecimal.ZERO;
                BigDecimal totalResult = BigDecimal.ZERO;
                
                for (Object obj : tableData) {
                    JSONObject row = JSONUtil.parseObj(obj);
                    String amountStr = row.getStr("amount");
                    if (StringUtils.isNotBlank(amountStr)) {
                        try {
                            totalAmount = totalAmount.add(new BigDecimal(amountStr));
                        } catch (Exception e) {
                            log.warn("解析金额失败: {}", amountStr);
                        }
                    }
                    
                    if (Boolean.TRUE.equals(settled)) {
                        String resultStr = row.getStr("result");
                        if (StringUtils.isNotBlank(resultStr) && !resultStr.equals("已取消") && !resultStr.equals("和局")) {
                            try {
                                totalResult = totalResult.add(new BigDecimal(resultStr));
                            } catch (Exception e) {
                                log.warn("解析结果金额失败: {}", resultStr);
                            }
                        }
                    }
                }
                
                JSONObject summaryData = new JSONObject();
                summaryData.putOpt("account", userConfig.getAccount());
                summaryData.putOpt("totalAmount", totalAmount.toString());
                summaryData.putOpt("totalResult", totalResult.toString());
                tableData.add(summaryData);
            }
            
        } catch (Exception e) {
            log.error("解析新API返回数据失败: {}", e.getMessage(), e);
            resultJson.putOpt("list", new JSONArray());
            resultJson.putOpt("total", 0);
            resultJson.putOpt("pageSize", 15);
            resultJson.putOpt("currentPage", pageNo + 1);
            resultJson.putOpt("totalPages", 1);
        }

        return resultJson.toString();
    }
    
    /**
     * 获取盘口字符串（ltype转换为A/B/C等）
     */
    private String getLtypeString(int ltype) {
        return switch (ltype) {
            case 1 -> "A";
            case 2 -> "B";
            case 3 -> "C";
            case 4 -> "D";
            case 5 -> "E";
            case 6 -> "F";
            case 7 -> "G";
            case 8 -> "H";
            default -> "A";
        };
    }
    
    /**
     * 获取注单详细赔率（新API）
     * @param username 用户名
     * @param id 账号ID
     * @param betID 注单ID
     * @param gametype 游戏类型
     * @param orderdt 订单日期（格式：yyyy-MM-dd）
     * @param wtype 投注类型（如MSA0）
     * @return JSON字符串
     */
    public String betDetail(String username, String id, String betID, String gametype, String orderdt, String wtype) {
        // 获取用户配置
        List<UserConfig> userConfigs = configService.accounts(username, id);
        if (CollectionUtil.isEmpty(userConfigs)) {
            throw new BusinessException(SystemError.USER_1007);
        }
        UserConfig userConfig = userConfigs.get(0);
        String baseUrl = userConfig.getBaseUrl();
        String token = userConfig.getToken();
        
        // 构建新API URL
        // betDetail.php?showBetDetail=showBetDetail&uid=xxx&betID=xxx&gametype=xxx&orderdt=xxx
        String url = baseUrl + "app/control/betDetail.php?showBetDetail=showBetDetail&uid=" + token 
                + "&betID=" + betID + "&gametype=" + gametype + "&orderdt=" + orderdt;
        
        // 设置请求头
        Map<String, String> headers = new HashMap<>();
        headers.put("accept", "application/json, text/javascript, */*; q=0.01");
        headers.put("accept-language", "zh-CN,zh;q=0.9");
        headers.put("cache-control", "no-cache");
        headers.put("pragma", "no-cache");
        headers.put("priority", "u=1, i");
        headers.put("referer", baseUrl + "app/control/betDetail.php?uid=" + token);
        headers.put("sec-ch-ua", "\"Google Chrome\";v=\"143\", \"Chromium\";v=\"143\", \"Not A(Brand\";v=\"24\"");
        headers.put("sec-ch-ua-mobile", "?0");
        headers.put("sec-ch-ua-platform", "\"Windows\"");
        headers.put("sec-fetch-dest", "empty");
        headers.put("sec-fetch-mode", "cors");
        headers.put("sec-fetch-site", "same-origin");
        headers.put("user-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36");
        headers.put("x-requested-with", "XMLHttpRequest");
        
        // 设置Cookie
        String cookie = "gtype=9; nowpage=game.html; PHPSESSID=" + token;
        headers.put("cookie", cookie);
        
        String result;
        try {
            // 用okHttp代理调度器发起GET请求
            OkHttpProxyDispatcher.HttpResult resultHttp = dispatcher.execute("GET", url, null, headers, userConfig, false);
            result = StringUtils.isNotBlank(resultHttp.getBody()) ? resultHttp.getBody() : null;
        } catch (Exception e) {
            Throwable cause = e.getCause();
            if (cause instanceof UnknownHostException) {
                log.error("获取详细赔率失败：主机未知。异常信息：{}", e.getMessage(), e);
                throw new BusinessException(SystemError.SYS_419, userConfig.getAccount(), "代理请求失败");
            } else if (cause instanceof ConnectException) {
                log.error("获取详细赔率失败：连接异常。异常信息：{}", e.getMessage(), e);
                throw new BusinessException(SystemError.SYS_419, userConfig.getAccount(), "连接异常");
            } else if (cause instanceof SocketTimeoutException) {
                log.error("获取详细赔率失败：连接超时。异常信息：{}", e.getMessage(), e);
                throw new BusinessException(SystemError.SYS_419, userConfig.getAccount(), "连接超时");
            } else if (cause instanceof IOException) {
                log.error("获取详细赔率失败：IO异常。异常信息：{}", e.getMessage(), e);
                throw new BusinessException(SystemError.SYS_419, userConfig.getAccount(), "IO异常");
            } else {
                log.error("获取详细赔率失败：未知异常。异常信息：{}", e.getMessage(), e);
                throw new BusinessException(SystemError.SYS_419, userConfig.getAccount(), "未知异常");
            }
        }
        
        // 解析JSON响应
        if (StringUtils.isBlank(result)) {
            log.warn("获取详细赔率返回为空, betID:{}, gametype:{}", betID, gametype);
            JSONObject errorResult = new JSONObject();
            errorResult.putOpt("title", 0);
            errorResult.putOpt("message", "返回数据为空");
            return errorResult.toString();
        }
        
        try {
            JSONObject resultJson = JSONUtil.parseObj(result);
            // 验证响应格式
            if (resultJson.getInt("title", -1) != 1) {
                log.warn("获取详细赔率失败, betID:{}, gametype:{}, 响应:{}", betID, gametype, result);
            }
            return resultJson.toString();
        } catch (Exception e) {
            log.error("解析详细赔率响应失败: {}", e.getMessage(), e);
            JSONObject errorResult = new JSONObject();
            errorResult.putOpt("title", 0);
            errorResult.putOpt("message", "解析响应失败: " + e.getMessage());
            return errorResult.toString();
        }
    }


    /**
     * 获取两周报表历史流水记录
     *
     * @return 结果
     */
    public String history(String username, String id, String lottery) {
        List<UserConfig> userConfigs = configService.accounts(username, id);
        if (CollectionUtil.isEmpty(userConfigs)) {
            throw new BusinessException(SystemError.USER_1007);
        }
        UserConfig userConfig = userConfigs.get(0);
        String baseUrl = userConfig.getBaseUrl();
        String token = userConfig.getToken();

        String url = baseUrl+"member/history";
        Map<String, String> headers = new HashMap<>();
        headers.put("accept", "*/*");
        headers.put("accept-language", "zh-CN,zh;q=0.9");
        headers.put("cookie", "defaultSetting=5%2C10%2C20%2C50%2C100%2C200%2C500%2C1000; settingChecked=0; page=lm; index=; index2=; defaultLT="+lottery+"; token=" + token);
        headers.put("priority", "u=1, i");
        headers.put("x-requested-with", "XMLHttpRequest");
        // 发送 HTTP 请求
        String result = null;

        try {
            // 用okHttp代理调度器发起GET请求，body传空字符串
            OkHttpProxyDispatcher.HttpResult resultHttp = dispatcher.execute("GET", url, null, headers, userConfig, false);
            result = StringUtils.isNotBlank(resultHttp.getBody()) ? resultHttp.getBody() : null;

        } catch (Exception e) {
            Throwable cause = e.getCause(); // 获取原始异常原因
            if (cause instanceof UnknownHostException) {
                log.error("代理请求失败：主机未知。可能是域名解析失败或代理地址有误。异常信息：{}", e.getMessage(), e);
                throw new BusinessException(SystemError.SYS_419, userConfigs.get(0).getAccount(), "代理请求失败");
            } else if (cause instanceof ConnectException) {
                log.error("代理请求失败：连接异常。可能是代理服务器未开启或网络不可达。异常信息：{}", e.getMessage(), e);
                throw new BusinessException(SystemError.SYS_419, userConfigs.get(0).getAccount(), "连接异常");
            } else if (cause instanceof SocketTimeoutException) {
                log.error("代理请求失败：连接超时。可能是网络延迟过高或代理服务器响应缓慢。异常信息：{}", e.getMessage(), e);
                throw new BusinessException(SystemError.SYS_419, userConfigs.get(0).getAccount(), "连接超时");
            } else if (cause instanceof IOException) {
                log.error("代理请求失败：IO异常。可能是数据传输错误或代理配置不正确。异常信息：{}", e.getMessage(), e);
                throw new BusinessException(SystemError.SYS_419, userConfigs.get(0).getAccount(), "IO异常");
            } else {
                log.error("代理请求失败：未知异常。异常信息：{}", e.getMessage(), e);
                throw new BusinessException(SystemError.SYS_419, userConfigs.get(0).getAccount(), "未知异常");
            }
        }
        // 解析 HTML
        Document doc = Jsoup.parse(result);
        JSONObject resultJson = new JSONObject();
        // 对应两个表格的键名
        String[] tableKeys = {"lastWeek", "thisWeek"};
        int columnCount = 6; // 表格的列数

        // 处理两个表格
        Elements tables = doc.select("table.list");
        for (int i = 0; i < tableKeys.length; i++) {
            if (i >= tables.size()) break; // 防止表格数量不足

            List<Map<String, String>> tableData = new ArrayList<>();
            Elements rows = tables.get(i).select("tbody tr");

            for (Element row : rows) {
                Elements cols = row.select("td");
                if (cols.size() >= columnCount) {
                    Map<String, String> rowData = new HashMap<>();
                    rowData.put("date", cols.get(0).text());
                    rowData.put("count", cols.get(1).text());
                    rowData.put("betAmount", cols.get(2).text());
                    rowData.put("forceAmount", cols.get(3).text());
                    rowData.put("cm", cols.get(4).text());
                    rowData.put("dividend", cols.get(5).text());
                    tableData.add(rowData);
                }
            }
            resultJson.putOpt(tableKeys[i], tableData);
        }

        // 返回结果 JSON
        return resultJson.toString();
    }

    private static final Pattern RETURN_DATA_PATTERN =
            Pattern.compile("var\\s+returnData\\s*=\\s*(\\[.*?\\]);", Pattern.DOTALL);

    /**
     * 获取开奖信息列表-获取最近50条
     * @param username
     * @param id
     * @return
     */
    public JSONArray dresults(String username, String id, String date, int page) {
        List<UserConfig> userConfigs = configService.accounts(username, id);
        if (CollectionUtil.isEmpty(userConfigs)) {
            throw new BusinessException(SystemError.USER_1007);
        }

        if (StringUtils.isBlank(date)) {
            date = LocalDateTimeUtil.now().format(DateTimeFormatter.ofPattern(DatePattern.NORM_DATE_PATTERN));
        }
        UserConfig userConfig = userConfigs.get(0);

        String url = userConfig.getBaseUrl()
                + "app/control/lottoHistory.php"
                + "?uid=" + userConfig.getToken()
                + "&active=1"
                + "&gtype=9"
                + "&pages=" + page
                + "&dts=" + date;

        Map<String, String> headers = new HashMap<>();
        headers.put("accept", "text/html,application/xhtml+xml,*/*");
        headers.put("referer", url);

        String html;
        try {
            OkHttpProxyDispatcher.HttpResult result =
                    dispatcher.execute("GET", url, null, headers, userConfig, false);

            html = result.getBody();
            if (StringUtils.isBlank(html)) {
                return new JSONArray();
            }

        } catch (Exception e) {
            log.error("获取历史开奖失败 lottoHistory", e);
            throw new BusinessException(SystemError.SYS_419, userConfig.getAccount(), "获取历史开奖失败");
        }

        // === 关键：提取 returnData ===
        Matcher matcher = RETURN_DATA_PATTERN.matcher(html);
        if (!matcher.find()) {
            log.warn("未找到 returnData，可能页面结构变更");
            return new JSONArray();
        }

        String jsonArrayStr = matcher.group(1);

        JSONArray sourceArray = JSONUtil.parseArray(jsonArrayStr);
        JSONArray result = new JSONArray();

        for (int i = 0; i < sourceArray.size(); i++) {
            JSONObject src = sourceArray.getJSONObject(i);

            JSONObject item = new JSONObject();
            item.putOpt("period", src.getStr("round"));
            item.putOpt("drawTime", src.getStr("openbet"));

            item.putOpt("n1", src.getStr("n1"));
            item.putOpt("n2", src.getStr("n1"));
            item.putOpt("n3", src.getStr("n1"));

            // hash（有些彩种会用）
            item.putOpt("hash", src.getStr("hash"));

            result.add(item);
        }

        return result;
    }


    /**
     * 获取指定期数开奖信息
     * @param username
     * @param id
     * @param lottery
     * @param drawNumber
     * @return
     */
    public JSONObject dresult(String username, String id, String lottery, String drawNumber) {
        // 获取用户配置
        List<UserConfig> userConfigs = configService.accounts(username, id);
        if (CollectionUtil.isEmpty(userConfigs)) {
            throw new BusinessException(SystemError.USER_1007);
        }
        UserConfig userConfig = userConfigs.get(0);
        String baseUrl = userConfig.getBaseUrl();
        String token = userConfig.getToken();

        // 拼接请求 URL
        String url = baseUrl + "member/dresult?lottery=" + lottery + "&date=" + ToDayRangeUtil.getCurrentRangeDate() + "&drawNumber=" + drawNumber;

        // 设置请求头
        Map<String, String> headers = new HashMap<>();
        headers.put("accept", "*/*");
        headers.put("accept-language", "zh-CN,zh;q=0.9");
        headers.put("cookie", "defaultSetting=5%2C10%2C20%2C50%2C100%2C200%2C500%2C1000; settingChecked=0; page=lm; index=; index2=; token=" + token);
        headers.put("priority", "u=1, i");
        headers.put("x-requested-with", "XMLHttpRequest");

        // 发送 HTTP 请求
        String result = null;
        try {
            // 执行请求
            OkHttpProxyDispatcher.HttpResult resultHttp = dispatcher.execute("GET", url, null, headers, userConfig, false);
            result = StringUtils.isNotBlank(resultHttp.getBody()) ? resultHttp.getBody() : null;

            //log.info("开奖信息请求成功，返回结果：{}", result);
        } catch (Exception e) {
            Throwable cause = e.getCause(); // 获取原始异常原因
            if (cause instanceof UnknownHostException) {
                log.error("代理请求失败：主机未知。可能是域名解析失败或代理地址有误。异常信息：{}", e.getMessage(), e);
                throw new BusinessException(SystemError.SYS_419, userConfigs.get(0).getAccount(), "代理请求失败");
            } else if (cause instanceof ConnectException) {
                log.error("代理请求失败：连接异常。可能是代理服务器未开启或网络不可达。异常信息：{}", e.getMessage(), e);
                throw new BusinessException(SystemError.SYS_419, userConfigs.get(0).getAccount(), "连接异常");
            } else if (cause instanceof SocketTimeoutException) {
                log.error("代理请求失败：连接超时。可能是网络延迟过高或代理服务器响应缓慢。异常信息：{}", e.getMessage(), e);
                throw new BusinessException(SystemError.SYS_419, userConfigs.get(0).getAccount(), "连接超时");
            } else if (cause instanceof IOException) {
                log.error("代理请求失败：IO异常。可能是数据传输错误或代理配置不正确。异常信息：{}", e.getMessage(), e);
                throw new BusinessException(SystemError.SYS_419, userConfigs.get(0).getAccount(), "IO异常");
            } else {
                log.error("代理请求失败：未知异常。异常信息：{}", e.getMessage(), e);
                throw new BusinessException(SystemError.SYS_419, userConfigs.get(0).getAccount(), "未知异常");
            }
        }

        // 解析 HTML 数据
        Document doc = Jsoup.parse(result);
        // 提取最后 5 个龙虎结果
        List<String> dragonTiger = new ArrayList<>();
        // 组装 JSON 数据
        JSONObject jsonObject = new JSONObject();
        // 解析表格内容
        Elements rows = doc.select("table.list tbody tr");
        // 获取开奖号码
        Elements ballElements = doc.select("td.name.ballname span");
        List<String> balls = ballElements.eachText();

        for (Element row : rows) {
            Elements cols = row.select("td");
            if (cols.size() > 0) {
                dragonTiger.add(cols.get(15).text());    // 双面 龙虎 第一位
                dragonTiger.add(cols.get(16).text());    // 双面 龙虎 第二位
                dragonTiger.add(cols.get(17).text());    // 双面 龙虎 第三位
                dragonTiger.add(cols.get(18).text());    // 双面 龙虎 第四位
                dragonTiger.add(cols.get(19).text());    // 双面 龙虎 第五位
            }
        }

        jsonObject.putOpt("balls", balls);
        jsonObject.putOpt("dragonTiger", dragonTiger);
        // 返回结果 JSON
        return jsonObject;
    }

    /**
     * 代理分类报表汇总
     * @param username
     * @param startDate
     * @return
     */
    public JSONArray agent(String username, String id, String startDate, String endDate) {
        if (StringUtils.isBlank(startDate) || StringUtils.isBlank(endDate)) {
            // 获取当前日期
            LocalDate today = LocalDate.now();
            // 格式化日期为 "yyyy-MM-dd"
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
            startDate = ToDayRangeUtil.getCurrentRangeDate(); // 默认开始日期为今天
            endDate = ToDayRangeUtil.getCurrentRangeDate();   // 默认结束日期为今天
        }

        List<UserConfig> userConfigs = configService.accounts(username, id);
        if (CollectionUtil.isEmpty(userConfigs)) {
            throw new BusinessException(SystemError.USER_1007);
        }
        UserConfig userConfig = userConfigs.get(0);
        String baseUrl = userConfig.getBaseUrl();
        String token = userConfig.getToken();

        String params = "&begin=" + startDate + "&end=" + endDate + "&settle=true&amount=&dividend=&lottery=PK10JSC,LUCKYSB&period=";
        String url = baseUrl + "agent/report/list_game?" + params;

        Map<String, String> headers = new HashMap<>();
        headers.put("accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7");
        headers.put("accept-language", "zh-CN,zh;q=0.9");
        headers.put("accept-encoding", "gzip, deflate, br, zstd");
        headers.put("sec-ch-ua", "\"Google Chrome\";v=\"131\", \"Chromium\";v=\"131\", \"Not_A Brand\";v=\"24\"");
        headers.put("sec-ch-ua-mobile", "?0");
        headers.put("sec-ch-ua-platform", "\"Windows\"");
        headers.put("sec-fetch-dest", "iframe");
        headers.put("sec-fetch-mode", "navigate");
        headers.put("sec-fetch-site", "same-origin");
        headers.put("sec-fetch-user", "?1");
        headers.put("priority", "u=0, i");
        headers.put("upgrade-insecure-requests", "1");
        headers.put("cookie", "b-user-id=26524651-2f28-ea47-ba0e-60d8403cf359; ssid1=33de784aa526ae7242e60a34ae9c4bcf; random=2947; __nxquid=2VpeLcL6SZeI0gRHiGka7eIv7z2qcA==0013; token=" + token);
        headers.put("user-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36");

        OkHttpProxyDispatcher.HttpResult result;

        try {
            // 发送 HTTP 请求，获取完整响应（含 header）
            result = dispatcher.executeFull("GET", url, null, headers, userConfig);
        } catch (Exception e) {
            Throwable cause = e.getCause(); // 获取原始异常原因
            if (cause instanceof UnknownHostException) {
                log.error("代理请求失败：主机未知。可能是域名解析失败或代理地址有误。异常信息：{}", e.getMessage(), e);
                throw new BusinessException(SystemError.SYS_419, userConfig.getAccount(), "代理请求失败");
            } else if (cause instanceof ConnectException) {
                log.error("代理请求失败：连接异常。可能是代理服务器未开启或网络不可达。异常信息：{}", e.getMessage(), e);
                throw new BusinessException(SystemError.SYS_419, userConfig.getAccount(), "连接异常");
            } else if (cause instanceof SocketTimeoutException) {
                log.error("代理请求失败：连接超时。可能是网络延迟过高或代理服务器响应缓慢。异常信息：{}", e.getMessage(), e);
                throw new BusinessException(SystemError.SYS_419, userConfig.getAccount(), "连接超时");
            } else if (cause instanceof IOException) {
                log.error("代理请求失败：IO异常。可能是数据传输错误或代理配置不正确。异常信息：{}", e.getMessage(), e);
                throw new BusinessException(SystemError.SYS_419, userConfig.getAccount(), "IO异常");
            } else {
                log.error("代理请求失败：未知异常。异常信息：{}", e.getMessage(), e);
                throw new BusinessException(SystemError.SYS_419, userConfig.getAccount(), "未知异常");
            }
        }

        // 判断是否被重定向
        List<String> locationHeaders = result.getHeaders().get("location");
        String location = (locationHeaders != null && !locationHeaders.isEmpty()) ? locationHeaders.get(0) : null;

        if (StringUtils.isNotBlank(location)) {
            if (location.contains("/agent/loginchecknotification")) {
                // 登录IP验证
                loginchecknotification(username, userConfig.getId(), token);
                return null;
            } else if (location.contains("/login")) {
                throw new BusinessException(SystemError.USER_1015);
            }
        }

        // 解析 HTML 返回结果
        return xmlToJson(result.getBody());
    }

    public JSONArray xmlToJson(String xmlStr) {
        if (StringUtils.isBlank(xmlStr)) {
            return new JSONArray();
        }
        if (xmlStr.contains("416 Range Not Satisfiable")) {
            throw new BusinessException(SystemError.USER_1002);
        }
        // 移除 DOCTYPE 声明
        xmlStr = xmlStr.replaceAll("(?i)<!DOCTYPE[^>]*>", "");

        // 将 XML 转换为 Map
        JSONObject json = JSONUtil.parseObj(XmlUtil.xmlToMap(xmlStr));

        // 获取 div 中的内容
        JSONObject bodyJson = json.getJSONObject("body");
        if (bodyJson == null || !bodyJson.containsKey("div")) {
            return new JSONArray();  // 如果没有 div 元素，直接返回空数组
        }

        JSONArray divArray = bodyJson.getJSONObject("div").getJSONArray("div");
        if (divArray == null) {
            return new JSONArray();  // 如果没有 div 数组，返回空数组
        }

        JSONArray tableArray = new JSONArray();

        // 遍历 divArray，查找包含 table 的 div
        divArray.forEach(div -> {
            if (StringUtils.isNotBlank(String.valueOf(div))) {
                JSONObject divJson = JSONUtil.parseObj(div);
                if (divJson.containsKey("table")) {
                    JSONObject tableJson = divJson.getJSONObject("table");
                    if (tableJson.containsKey("tbody")) {
                        JSONArray trArray = tableJson.getJSONObject("tbody").getJSONArray("tr");
                        if (trArray != null) {
                            tableArray.add(trArray);
                        }
                    }
                }
            }
        });

        // 获取第一个表格数据进行处理
        if (tableArray.isEmpty()) {
            return new JSONArray();  // 如果没有找到任何表格数据，返回空数组
        }

        JSONArray optimizedArray = tableArray.getJSONArray(0);
        JSONArray optimized = new JSONArray();

        // 遍历原始数据并提取所需字段
        for (Object obj : optimizedArray) {
            JSONObject record = (JSONObject) obj;
            JSONArray tdArray = record.getJSONArray("td");

            if (tdArray != null && tdArray.size() >= 8) {  // 确保 td 数组长度足够
                // 只提取所需的字段
                JSONObject optimizedRecord = new JSONObject();
                optimizedRecord.putOpt("lottery", tdArray.getStr(0));  // 游戏类型
                optimizedRecord.putOpt("play", tdArray.getStr(1));     // 游戏玩法
                optimizedRecord.putOpt("forceAmount", tdArray.getBigDecimal(4));  // 有效金额
                optimizedRecord.putOpt("receivable", tdArray.getBigDecimal(9));   // 应收下线

                optimized.add(optimizedRecord);  // 添加到优化后的数组
            }
        }

        // 返回优化后的 JSON 数组
        return optimized;
    }


}
