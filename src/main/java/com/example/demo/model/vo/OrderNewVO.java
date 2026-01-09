// OrderVO.java

// YApi QuickType插件生成，具体参考文档:https://plugins.jetbrains.com/plugin/18847-yapi-quicktype/documentation

package com.example.demo.model.vo;
import lombok.Data;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

@Data
public class OrderNewVO {
    private String uid;
    private String gid;
    private String gtype;
    private String wtype;
    private String ltype;
    private String ratechg;
    private List<BetNew> orders = new CopyOnWriteArrayList<>();
}

// Bet.java

// YApi QuickType插件生成，具体参考文档:https://plugins.jetbrains.com/plugin/18847-yapi-quicktype/documentation

