package ai.basic.x1.adapter.api.job;

import ai.basic.x1.adapter.dto.ApiResult;
import ai.basic.x1.adapter.dto.PreModelParamDTO;
import ai.basic.x1.adapter.port.dao.ModelDAO;
import ai.basic.x1.adapter.port.dao.ModelDataResultDAO;
import ai.basic.x1.adapter.port.dao.mybatis.model.Model;
import ai.basic.x1.adapter.port.dao.mybatis.model.ModelClass;
import ai.basic.x1.adapter.port.dao.mybatis.model.ModelDataResult;
import ai.basic.x1.adapter.port.rpc.PredImageCo80ModelHttpCaller;
import ai.basic.x1.adapter.port.rpc.dto.PredImageReqDTO;
import ai.basic.x1.adapter.port.rpc.dto.PredImageRespDTO;
import ai.basic.x1.entity.ModelMessageBO;
import ai.basic.x1.entity.PredImageModelObjectBO;
import ai.basic.x1.entity.enums.ModelCodeEnum;
import ai.basic.x1.usecase.exception.UsecaseCode;
import ai.basic.x1.usecase.exception.UsecaseException;
import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.collection.ConcurrentHashSet;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * @author Zhujh
 */
@Slf4j
public class PredictImageCo80ModelHandler extends AbstractModelMessageHandler<PredImageRespDTO> {

    @Autowired
    private ModelDataResultDAO modelDataResultDAO;

    @Autowired
    private PredImageCo80ModelHttpCaller modelHttpCaller;

    @Autowired
    private ModelDAO modelDAO;

    private final ModelRunSuccessHandler successHandler = new DefaultModelRunSuccessHandler();
    private final ModelRunFailureHandler failureHandler = (message, ex) -> {
       var modelObject = PredImageModelObjectBO.builder().code(-1)
                .message(ex.getMessage())
                .dataId(message.getDataId())
                .objects(null).build();
        try {
            modelDataResultDAO.update(ModelDataResult.builder()
                            .updatedAt(OffsetDateTime.now())
                            .updatedBy(message.getCreatedBy())
                            .modelResult(objectMapper.readValue(JSONUtil.toJsonStr(modelObject), JsonNode.class))
                            .build(),
                    Wrappers.lambdaUpdate(ModelDataResult.class)
                            .eq(ModelDataResult::getModelSerialNo, message.getModelSerialNo())
                            .eq(ModelDataResult::getDataId, message.getDataId()));
        } catch (JsonProcessingException e) {
            log.error("ModelRunFailureHandler convert jsonNode error. ", e);
        }
    };

    @Override
    boolean modelRun(ModelMessageBO message) {
        log.info("start model run. dataId: {}, modelSerialNo: {}",  message.getDataId(),
                message.getModelSerialNo());
        var apiResult = getRetryAbleApiResult(message);
        if (apiResult.getCode() == UsecaseCode.OK) {
            try {
                successHandler.onModelRunSuccess(apiResult.getData(), message);
            } catch (Exception e) {
                failureHandler.onModelRunFailure(message, e);
            }
        }
        return apiResult.getCode() == UsecaseCode.OK;
    }

    @Override
    ApiResult<PredImageRespDTO> callRemoteService(ModelMessageBO message) {
        try {
            var apiResult = modelHttpCaller
                    .callPredImageModel(buildRequestParam(message));

            if (CollUtil.isNotEmpty(apiResult.getData())) {
                return new ApiResult<>(apiResult.getCode(), apiResult.getMessage(),
                        apiResult.getData().get(0));
            }
            return new ApiResult<>(apiResult.getCode(), apiResult.getMessage());
        } catch (Exception e) {
            failureHandler.onModelRunFailure(message, e);
            throw new UsecaseException(UsecaseCode.UNKNOWN, e.getMessage());
        }
    }

    @Override
    ModelCodeEnum getModelCodeEnum() {
        return ModelCodeEnum.COCO_80;
    }

    public PredImageReqDTO buildRequestParam(ModelMessageBO message) {
        var dataInfo = message.getDataInfo();
        if (dataInfo == null) {
            throw new IllegalArgumentException(String.format("%s data is not found",
                    message.getDataId()));
        }
        var fileNodes = dataInfo.getContent();
        if (CollUtil.isEmpty(fileNodes)) {
            throw new IllegalArgumentException("file is not found");
        }
        String url = fileNodes.get(0).getFile().getUrl();
        if (StrUtil.isEmpty(url)) {
            throw new IllegalArgumentException("file url is empty");
        }
        return PredImageReqDTO.builder().datas(List.of(PredImageReqDTO.ImageData.builder()
                .imageId(dataInfo.getId()).imgUrl(url).build())).params("").build();
    }

    private interface ModelRunSuccessHandler {

        void onModelRunSuccess(PredImageRespDTO responseData, ModelMessageBO message);

    }

    private class DefaultModelRunSuccessHandler implements ModelRunSuccessHandler {

        private final Set<String> selectedClasses = new ConcurrentHashSet<>();
        private final Map<String, ModelClass> systemModelClassMap = new ConcurrentHashMap<>();

        private void initSystemModelClass() {
            Model model = modelDAO.getOne(new LambdaQueryWrapper<Model>().eq(Model::getModelCode,
                    getModelCodeEnum()));
            if (model == null) {
                throw new IllegalArgumentException(
                        String.format("%s not found system model", getModelCodeEnum()));
            }
            if (CollUtil.isEmpty(model.getClasses())) {
                throw new IllegalArgumentException(
                        String.format("%s model not have any class", getModelCodeEnum()));
            }
            for (var modelClass : model.getClasses()) {
                if (CollUtil.isNotEmpty(modelClass.getSubClasses())) {
                    for (var subModelClass : modelClass.getSubClasses()) {
                        if (subModelClass == null) {
                            continue;
                        }
                        if (systemModelClassMap.containsKey(subModelClass.getCode())) {
                            throw new IllegalArgumentException("subModelClass is duplicate. " +
                                    "code:" + subModelClass.getCode());
                        } else {
                            systemModelClassMap.put(subModelClass.getCode(), subModelClass);
                        }
                    }
                }
            }
        }

        @Override
        public void onModelRunSuccess(PredImageRespDTO responseData, ModelMessageBO message) {
            PredImageModelObjectBO modelObject;
            if (CollUtil.isEmpty(responseData.getPredictItems())) {
                modelObject = PredImageModelObjectBO.builder().code(1)
                        .message("success")
                        .dataId(message.getDataId())
                        .objects(List.of()).build();
            } else {
                this.initSystemModelClass();
                var filterPredItemCondition = JSONUtil.toBean(message.getResultFilterParam(),
                        PreModelParamDTO.class);
                log.info("start filter predItem. filter condition: " + JSONUtil.toJsonStr(filterPredItemCondition));
                var predObjects = responseData.getPredictItems()
                        .stream().filter(item -> filter(item, filterPredItemCondition))
                        .map(this::convert).collect(Collectors.toList());

                modelObject = PredImageModelObjectBO.builder().code(1)
                        .message("success")
                        .dataId(message.getDataId())
                        .objects(predObjects).build();
            }

            try {
                modelDataResultDAO.update(ModelDataResult.builder()
                                .updatedAt(OffsetDateTime.now())
                                .updatedBy(message.getCreatedBy())
                                .modelResult(objectMapper.readValue(JSONUtil.toJsonStr(modelObject), JsonNode.class))
                                .build(),
                        Wrappers.lambdaUpdate(ModelDataResult.class)
                                .eq(ModelDataResult::getModelSerialNo, message.getModelSerialNo())
                                .eq(ModelDataResult::getDataId, message.getDataId()));
            } catch (JsonProcessingException e) {
                log.error("ModelRunSuccessHandler convert jsonNode error. ", e);
            }
        }

        private PredImageModelObjectBO.ObjectBO convert(PredImageRespDTO.PredictItem predictItem) {
            var topLeft = PredImageModelObjectBO.Point
                    .builder()
                    .x(predictItem.getLeftTopX())
                    .y(predictItem.getLeftTopY()).build();
            var bottomRight = PredImageModelObjectBO.Point
                    .builder()
                    .x(predictItem.getRightBottomX())
                    .y(predictItem.getRightBottomY()).build();

            var modelClass = systemModelClassMap.get(String.valueOf(predictItem.getClsid()));
            return PredImageModelObjectBO
                    .ObjectBO.builder()
                    .confidence(predictItem.getScore())
                    .modelClass(modelClass.getName())
                    .modelClassId(Integer.valueOf(modelClass.getCode()))
                    .objType("rectangle")
                    .points(List.of(topLeft, bottomRight))
                    .build();
        }

        private boolean filter(PredImageRespDTO.PredictItem predictItem,
                               PreModelParamDTO filterPredItem) {
            if (filterPredItem == null || CollUtil.isEmpty(filterPredItem.getClasses())) {
               throw new IllegalArgumentException("model param is empty");
            }
            if (CollUtil.isEmpty(filterPredItem.getClasses())) {
                throw new IllegalArgumentException("model class select at least one class");
            }
            var maxConfidence = getMaxConfidence(filterPredItem.getMaxConfidence());
            var minConfidence = getMinConfidence(filterPredItem.getMinConfidence());
            if (selectedClasses.isEmpty()) {
                selectedClasses.addAll(filterPredItem.getClasses());
            }
            String clsId = String.valueOf(predictItem.getClsid());
            return systemModelClassMap.containsKey(clsId) && selectedClasses.contains(clsId) &&
                    betweenConfidence(predictItem.getScore(), minConfidence, maxConfidence);
        }

        private boolean betweenConfidence(BigDecimal predConfidence, BigDecimal minConfidence,
                                          BigDecimal maxConfidence) {
            return minConfidence.compareTo(predConfidence) <= 0 && maxConfidence.compareTo(predConfidence) >= 0;
        }

        private BigDecimal getMaxConfidence(BigDecimal confidence) {
            return confidence == null ? new BigDecimal(1) : confidence;
        }

        private BigDecimal getMinConfidence(BigDecimal confidence) {
            return confidence == null ? new BigDecimal(0) : confidence;
        }
    }

    private interface ModelRunFailureHandler {

        void onModelRunFailure(ModelMessageBO message, Exception exception);

    }

}