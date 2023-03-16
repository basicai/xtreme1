package ai.basic.x1.entity;

import ai.basic.x1.adapter.port.rpc.dto.DatasetModelResultDTO;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * @author fyb
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DatasetModelResultBO {

    private String modelName;

    private Long modelId;

    private List<RunRecordBO> runRecords;

    @Data
    public static class RunRecordBO {
        private Long id;

        private String runNo;

    }
}