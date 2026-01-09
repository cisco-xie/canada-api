package com.example.demo;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.math.BigDecimal;
import java.math.RoundingMode;

import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
class CanadaApplicationTests {

    @Test
    void customRoundtest() {
        System.out.println(customRound(BigDecimal.valueOf(2341.45)));
        System.out.println(customRound(BigDecimal.valueOf(2342.91)));
        System.out.println(customRound(BigDecimal.valueOf(2342.95)));
        System.out.println(customRound(BigDecimal.valueOf(2343.98)));
    }

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

}
