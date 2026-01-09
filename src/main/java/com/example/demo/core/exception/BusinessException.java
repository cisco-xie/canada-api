package com.example.demo.core.exception;

import com.example.demo.common.constants.ErrCode;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author 谢诗宏
 * @description user相关异常
 * @date 2024年11月19日
 */
@Data
@NoArgsConstructor
public class BusinessException extends RuntimeException {

    private Integer code;

    public BusinessException(ErrCode errCode, Object... format) {
        super(String.format(errCode.getMsg(), format));
        this.code = errCode.getCode();
    }

}
