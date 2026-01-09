// OrderVO.java

// YApi QuickType插件生成，具体参考文档:https://plugins.jetbrains.com/plugin/18847-yapi-quicktype/documentation

package com.example.demo.model.vo;
import lombok.Data;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

@Data
public class OrderVO {
    private String account;
    private String token;
    private String drawNumber;
    private boolean ignore;
    private String lottery;
    private List<Bet> bets = new CopyOnWriteArrayList<>();
    private boolean fastBets;
}

// Bet.java

// YApi QuickType插件生成，具体参考文档:https://plugins.jetbrains.com/plugin/18847-yapi-quicktype/documentation

