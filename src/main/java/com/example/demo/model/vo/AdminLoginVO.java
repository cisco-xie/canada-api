package com.example.demo.model.vo;

import lombok.Data;

@Data
public class AdminLoginVO {
    private String username;
    private String password;
    private String group;
    private String roles;
    private String securityCode;    // 盘口安全码(加密后的字符串)
    private Integer platform = 4;   // 平台 1对冲 2跟投 3足球 4加拿大
}
