package com.example.demo.model.vo;

import lombok.Data;

import java.util.List;

@Data
public class AdminUserVO {
    private List<AdminLoginVO> users;
}
