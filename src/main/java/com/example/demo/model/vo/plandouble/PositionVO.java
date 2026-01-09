// YApi QuickType插件生成，具体参考文档:https://plugins.jetbrains.com/plugin/18847-yapi-quicktype/documentation

package com.example.demo.model.vo.plandouble;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;

@Data
public class PositionVO {
    @Schema(description = "是否启用（0否1是）")
    private Integer enable;
    @Schema(description = "追踪号码-用于计划005")
    private String trackPositionNum;
    @Schema(description = "号码位移-用于计划005")
    private String shiftPositionNum;
    @Schema(description = "开出位置,用于计划003")
    private String winLocationNum;
    @Schema(description = "开出号码,用于计划003和025")
    private String winPositionNum;
    @Schema(description = "冷热位置-用于计划017")
    private String coolHotPositionNum;
    @Schema(description = "冷热排行-用于计划017")
    private String coolHotRank;
    @Schema(description = "号码,多个号码用逗号隔开")
    private String positionNum;
    @Schema(description = "位置,多个位置用逗号隔开")
    private String locationNum;
}
