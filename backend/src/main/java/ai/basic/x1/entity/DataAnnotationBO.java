package ai.basic.x1.entity;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import java.time.OffsetDateTime;

@Data
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
public class DataAnnotationBO {

    private Long id;

    /**
     * 数据集ID
     */
    private Long datasetId;

    /**
     * 数据ID
     */
    private Long dataId;

    /**
     * 类型ID
     */
    private Long classificationId;

    /**
     * 类型属性
     */
    private JsonNode classificationAttributes;

    /**
     * 创建时间
     */
    private OffsetDateTime createdAt;

    /**
     * 创建者
     */
    private Long createdBy;

}