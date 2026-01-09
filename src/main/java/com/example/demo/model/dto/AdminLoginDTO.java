package com.example.demo.model.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class AdminLoginDTO {
    private String username;
    private String nickname;
    private String password;
    private List<String> roles;
    private List<String> permissions;
    private String securityCode;    // 盘口安全码(加密后的字符串)
    private String accessToken;
    private String refreshToken;
    private String expires;
    private Integer autoBet = 0;
    private Integer skip = 1;       // 是否跳过 0不跳过 1跳过
    private String group;
    private Integer platform = 4;   // 平台 1对冲 2跟投 3足球
}
