package com.example.demo.model.vo;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

import java.math.BigDecimal;

@Data
public class ConfigUserVO {
    private String id;
    // 投注类型 add/update
    @NotBlank(message = "操作类型必选")
    private String operationType;
    @NotBlank(message = "盘口地址不能为空")
    private String baseUrl;
    @NotBlank(message = "盘口账号不能为空")
    private String account;
    @NotBlank(message = "盘口密码不能为空")
    private String password;
    // 距离封盘时间内下注 默认20s
    private Integer closeTime = 0;
    // 投注类型 1正投 2反投
    @NotNull(message = "投注类型必选")
    private Integer betType;
    // 代理类型 1HTTP 2SOCKS
    private Integer proxyType;
    // 代理IP
    private String proxyHost;
    // 代理端口
    private Integer proxyPort;
    // 认证账号
    private String proxyUsername;
    // 认证密码
    private String proxyPassword;

    public void setBaseUrl(String baseUrl) {
        this.baseUrl = baseUrl == null ? null : baseUrl.trim();
    }

    public void setAccount(String account) {
        this.account = account == null ? null : account.trim();
    }

    public void setPassword(String password) {
        this.password = password == null ? null : password.trim();
    }

    public void setProxyHost(String proxyHost) {
        this.proxyHost = proxyHost == null ? null : proxyHost.trim();
    }

    public void setProxyUsername(String proxyUsername) {
        this.proxyUsername = proxyUsername == null ? null : proxyUsername.trim();
    }

    public void setProxyPassword(String proxyPassword) {
        this.proxyPassword = proxyPassword == null ? null : proxyPassword.trim();
    }
}
