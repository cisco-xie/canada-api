package com.example.demo.core.support;


import cn.hutool.core.codec.Base64;
import cn.hutool.core.text.UnicodeUtil;
import cn.hutool.core.util.CharsetUtil;
import cn.hutool.core.util.URLUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import cn.hutool.jwt.JWT;
import cn.hutool.jwt.JWTUtil;
import com.example.demo.common.enmu.SystemError;
import com.example.demo.core.exception.BusinessException;
import com.example.demo.model.dto.AdminLoginDTO;
import jakarta.servlet.http.Cookie;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * @description 公共controller基类
 * @author 谢诗宏
 * @date 2024年11月27日
 */
@Slf4j
public class BaseController {

    /**
     * 认证信息Http请求头cookie
     */
    public static final String JWT_TOKEN_HEADER = "Authorization-Canada";

    /**
     * 令牌前缀
     */
    public static final String JWT_TOKEN_PREFIX = "Bearer ";

    /**
     * 得到request对象
     */
    public HttpServletRequest getRequest() {
        return ((ServletRequestAttributes) Objects.requireNonNull(RequestContextHolder.getRequestAttributes())).getRequest();
    }

    /**
     * 得到response对象
     */
    public HttpServletResponse getResponse() {
        return ((ServletRequestAttributes) Objects.requireNonNull(RequestContextHolder.getRequestAttributes())).getResponse();
    }

    /**
     * 获取当前用户token
     * @return
     */
    public AdminLoginDTO getUser() {
        String token = getRequest().getHeader(JWT_TOKEN_HEADER);
        if (StringUtils.isNotBlank(token)) {
            token = token.replace(JWT_TOKEN_PREFIX, "");
            final JWT jwt = JWTUtil.parseToken(token);
            AdminLoginDTO adminLoginDTO = JSONUtil.toBean(jwt.getPayload().getClaimsJson(), AdminLoginDTO.class);
            if (adminLoginDTO.getPlatform() != 4) {
                throw new BusinessException(SystemError.USER_1001);
            }
            return adminLoginDTO;
        } else {
            throw new BusinessException(SystemError.USER_1000);
        }
    }

    /**
     * id字符串转换成idList
     * @param ids
     * @return
     */
    public List<Integer> formatIds(String ids) {
        if (ids == null) {
            return null;
        }
        List<Integer> id = new ArrayList<>();
        String[] strings = ids.split(",");
        for (String string : strings) {
            id.add(Integer.parseInt(string.trim()));
        }
        return id;
    }

}
