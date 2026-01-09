package com.example.demo.config;

import jakarta.annotation.PostConstruct;
import org.springframework.context.annotation.Configuration;

@Configuration
public class JDKConfig {
    @PostConstruct
    public void httpsProxyConfig(){
        System.setProperty("jdk.http.auth.tunneling.disabledSchemes", "");
    }
}
