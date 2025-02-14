package ai.basic.x1.usecase;

import ai.basic.x1.adapter.dto.ApiResult;
import ai.basic.x1.adapter.port.dao.DataAnnotationObjectDAO;
import ai.basic.x1.adapter.port.dao.DataInfoDAO;
import ai.basic.x1.adapter.port.dao.DatasetDAO;
import ai.basic.x1.adapter.port.dao.UploadRecordDAO;
import ai.basic.x1.adapter.port.dao.mybatis.model.DataAnnotationObject;
import ai.basic.x1.adapter.port.dao.mybatis.model.DataInfo;
import ai.basic.x1.adapter.port.dao.mybatis.model.UploadRecord;
import ai.basic.x1.adapter.port.minio.MinioProp;
import ai.basic.x1.adapter.port.minio.MinioService;
import ai.basic.x1.adapter.port.rpc.PointCloudConvertRenderHttpCaller;
import ai.basic.x1.adapter.port.rpc.dto.*;
import ai.basic.x1.entity.*;
import ai.basic.x1.entity.enums.*;
import ai.basic.x1.usecase.exception.UsecaseCode;
import ai.basic.x1.usecase.exception.UsecaseException;
import ai.basic.x1.util.*;
import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.collection.ListUtil;
import cn.hutool.core.date.StopWatch;
import cn.hutool.core.img.Img;
import cn.hutool.core.io.FileUtil;
import cn.hutool.core.io.StreamProgress;
import cn.hutool.core.lang.UUID;
import cn.hutool.core.lang.tree.Tree;
import cn.hutool.core.lang.tree.TreeUtil;
import cn.hutool.core.thread.ThreadUtil;
import cn.hutool.core.util.*;
import cn.hutool.crypto.SecureUtil;
import cn.hutool.http.HttpUtil;
import cn.hutool.json.JSONArray;
import cn.hutool.json.JSONUtil;
import com.alibaba.ttl.TtlRunnable;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import lombok.extern.slf4j.Slf4j;
import net.coobird.thumbnailator.Thumbnails;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.transaction.annotation.Transactional;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static ai.basic.x1.entity.enums.DataUploadSourceEnum.LOCAL;
import static ai.basic.x1.entity.enums.DatasetTypeEnum.IMAGE;
import static ai.basic.x1.entity.enums.DatasetTypeEnum.TEXT;
import static ai.basic.x1.entity.enums.RelationEnum.*;
import static ai.basic.x1.entity.enums.SplitTypeEnum.NOT_SPLIT;
import static ai.basic.x1.entity.enums.UploadStatusEnum.*;
import static ai.basic.x1.usecase.exception.UsecaseCode.*;
import static ai.basic.x1.util.Constants.*;

@Slf4j
public class UploadDataUseCase {

    @Autowired
    private UploadUseCase uploadUseCase;

    @Autowired
    private ModelRunRecordUseCase modelRunRecordUseCase;

    @Autowired
    private PointCloudUploadUseCase pointCloudUploadUseCase;

    @Autowired
    private ImageUploadUseCase imageUploadUseCase;

    @Autowired
    private DataAnnotationObjectDAO dataAnnotationObjectDAO;

    @Autowired
    private DatasetDAO datasetDAO;

    @Autowired
    private DataInfoDAO dataInfoDAO;

    @Autowired
    private MinioService minioService;

    @Autowired
    private MinioProp minioProp;

    @Autowired
    private FileUseCase fileUseCase;

    @Autowired
    private DatasetClassUseCase datasetClassUseCase;

    @Autowired
    private UploadRecordDAO uploadRecordDAO;

    @Autowired
    private PointCloudConvertRenderHttpCaller pointCloudConvertRenderHttpCaller;

    @Autowired
    private DatasetSimilarityJobUseCase datasetSimilarityJobUseCase;

    @Value("${file.tempPath:/tmp/xtreme1/}")
    private String tempPath;

    @Value("${file.size.large:400}")
    private int largeFileSize;

    @Value("${file.size.medium:200}")
    private int mediumFileSize;

    @Value("${file.size.small:100}")
    private int smallFileSize;

    @Value("${file.prefix.large:large}")
    private String large;

    @Value("${file.prefix.medium:medium}")
    private String medium;

    @Value("${file.prefix.small:small}")
    private String small;

    @Value("${upload.url.whitelist}")
    private String whitelist;

    private static final ExecutorService executorService = ThreadUtil.newExecutor(2);
    private static final ExecutorService parseExecutorService = ThreadUtil.newExecutor(5);

    private final FileFilter textFileFilter = file -> {
        //if the file extension is json return true, else false
        return file.getAbsolutePath().toUpperCase().endsWith(JSON_SUFFIX) && Constants.TEXT.equalsIgnoreCase(FileUtil.getName(file.getParentFile()));
    };

    private static final Integer PC_RENDER_IMAGE_WIDTH = 2000;
    private static final Integer PC_RENDER_IMAGE_HEIGHT = 2000;

    private static final Integer RETRY_COUNT = 3;


    /**
     * Upload data
     *
     * @param dataInfoUploadBO Upload data object Contains a collection of file information
     */
    @Transactional(rollbackFor = RuntimeException.class)
    public Long upload(DataInfoUploadBO dataInfoUploadBO) {
        var uploadRecordBO = uploadUseCase.createUploadRecord(dataInfoUploadBO.getFileUrl());
        if(!checkUrlIsValid(whitelist,dataInfoUploadBO.getFileUrl())){
            uploadUseCase.updateUploadRecordStatus(uploadRecordBO.getId(), FAILED, DATASET_DATA_FILE_URL_ILLEGAL.getMessage());
            log.error("File url illegal,datasetId:{},userId:{},fileUrl:{}", dataInfoUploadBO.getDatasetId(), dataInfoUploadBO.getUserId(), dataInfoUploadBO.getFileUrl());
            return uploadRecordBO.getSerialNumber();
        }
        var boo = DecompressionFileUtils.validateUrl(dataInfoUploadBO.getFileUrl());
        if (!boo) {
            uploadUseCase.updateUploadRecordStatus(uploadRecordBO.getId(), FAILED, DATASET_DATA_FILE_URL_ERROR.getMessage());
            log.error("File url error,datasetId:{},userId:{},fileUrl:{}", dataInfoUploadBO.getDatasetId(), dataInfoUploadBO.getUserId(), dataInfoUploadBO.getFileUrl());
            return uploadRecordBO.getSerialNumber();
        }
        var dataset = datasetDAO.getById(dataInfoUploadBO.getDatasetId());
        if (ObjectUtil.isNull(dataset)) {
            uploadUseCase.updateUploadRecordStatus(uploadRecordBO.getId(), FAILED, DATASET_NOT_FOUND.getMessage());
            log.error("Dataset not found,datasetId:{},userId:{},fileUrl:{}", dataInfoUploadBO.getDatasetId(), dataInfoUploadBO.getUserId(), dataInfoUploadBO.getFileUrl());
            return uploadRecordBO.getSerialNumber();
        }
        dataInfoUploadBO.setType(dataset.getType());
        var fileUrl = DecompressionFileUtils.removeUrlParameter(dataInfoUploadBO.getFileUrl());
        var mimeType = FileUtil.getMimeType(fileUrl);
        if (!validateUrlFileSuffix(dataInfoUploadBO, mimeType)) {
            uploadUseCase.updateUploadRecordStatus(uploadRecordBO.getId(), FAILED, DATASET_DATA_FILE_FORMAT_ERROR.getMessage());
            log.error("Incorrect file format,datasetId:{},userId:{},fileUrl:{}", dataInfoUploadBO.getDatasetId(), dataInfoUploadBO.getUserId(), dataInfoUploadBO.getFileUrl());
            return uploadRecordBO.getSerialNumber();
        }
        dataInfoUploadBO.setUploadRecordId(uploadRecordBO.getId());
        executorService.execute(Objects.requireNonNull(TtlRunnable.get(() -> {
            try {
                if (IMAGE.equals(dataset.getType()) && IMAGE_DATA_TYPE.contains(mimeType)) {
                    this.downloadAndDecompressionFile(dataInfoUploadBO, this::parseImageUploadFile);
                } else if (IMAGE.equals(dataset.getType()) && COMPRESSED_DATA_TYPE.contains(mimeType)) {
                    this.downloadAndDecompressionFile(dataInfoUploadBO, this::parseImageCompressedUploadFile);
                } else if (TEXT.equals(dataset.getType())) {
                    this.downloadAndDecompressionFile(dataInfoUploadBO, this::parseTextUploadFile);
                } else {
                    this.downloadAndDecompressionFile(dataInfoUploadBO, this::parsePointCloudUploadFile);
                }
            } catch (IOException e) {
                log.error("Download decompression file error", e);
            }
        })));
        return uploadRecordBO.getSerialNumber();
    }


    public static boolean checkUrlIsValid(String whitelist, String url) {
        if(StrUtil.isEmpty(whitelist)){
            return true;
        }
        String[] substrings = whitelist.split(",");
        for (String substring : substrings) {
            if (url.contains(substring.trim())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Download the file and unzip the file
     *
     * @param dataInfoUploadBO Upload data parameter
     * @param function         function
     */
    private <T extends DataInfoUploadBO> void downloadAndDecompressionFile(T dataInfoUploadBO, Consumer<T> function) throws IOException {
        var fileUrl = URLUtil.decode(dataInfoUploadBO.getFileUrl());
        var datasetId = dataInfoUploadBO.getDatasetId();
        var path = DecompressionFileUtils.removeUrlParameter(fileUrl);
        dataInfoUploadBO.setFileName(FileUtil.getPrefix(path));
        var baseSavePath = String.format("%s%s/", tempPath, UUID.randomUUID().toString().replace("-", ""));
        var savePath = baseSavePath + FileUtil.getName(path);
        FileUtil.mkParentDirs(savePath);
        // Download the compressed package locally
        log.info("Get compressed package start fileUrl:{},savePath:{}", fileUrl, savePath);
        HttpUtil.downloadFileFromUrl(fileUrl, FileUtil.newFile(savePath), new StreamProgress() {
            @Override
            public void start() {
                uploadUseCase.updateUploadRecordStatus(dataInfoUploadBO.getUploadRecordId(), DOWNLOADING, null);
            }

            @Override
            public void progress(long total, long progressSize) {
                if (progressSize % PROCESS_VALUE_SIZE == 0 || total == progressSize) {
                    var uploadRecord = UploadRecord.builder()
                            .id(dataInfoUploadBO.getUploadRecordId())
                            .status(DOWNLOADING)
                            .totalFileSize(total)
                            .downloadedFileSize(progressSize).build();
                    uploadRecordDAO.updateById(uploadRecord);
                }
            }

            @Override
            public void finish() {
                uploadUseCase.updateUploadRecordStatus(dataInfoUploadBO.getUploadRecordId(), DOWNLOAD_COMPLETED, null);
            }
        });
        log.info("Get compressed package end fileUrl:{},savePath:{}", fileUrl, savePath);
        dataInfoUploadBO.setSavePath(savePath);
        dataInfoUploadBO.setBaseSavePath(baseSavePath);
        // A single image does not need to be decompressed
        if (IMAGE_DATA_TYPE.contains(FileUtil.getMimeType(path))) {
            function.accept(dataInfoUploadBO);
            FileUtil.clean(baseSavePath);
            return;
        }
        // Unzip files
        log.info("Start decompression,datasetId:{},filePath:{}", datasetId, savePath);
        DecompressionFileUtils.decompress(savePath, baseSavePath);
        function.accept(dataInfoUploadBO);
        FileUtil.clean(baseSavePath);
    }

    public void parsePointCloudUploadFile(DataInfoUploadBO dataInfoUploadBO) {
        this.commonParseUploadFile(dataInfoUploadBO, pointCloudUploadUseCase::findPointCloudParentList, pointCloudUploadUseCase::getDataNames);
    }

    private void parseImageUploadFile(DataInfoUploadBO dataInfoUploadBO) {
        var userId = dataInfoUploadBO.getUserId();
        var datasetId = dataInfoUploadBO.getDatasetId();
        var dataInfoBOBuilder = DataInfoBO.builder().datasetId(datasetId)
                .type(ItemTypeEnum.SINGLE_DATA)
                .status(DataStatusEnum.VALID)
                .annotationStatus(DataAnnotationStatusEnum.NOT_ANNOTATED)
                .createdAt(OffsetDateTime.now())
                .updatedAt(OffsetDateTime.now())
                .createdBy(userId)
                .isDeleted(false);
        var file = FileUtil.file(dataInfoUploadBO.getSavePath());
        var imageExtraInfoBO = ImageExtraInfoBO.builder().height(Img.from(file).getImg().getHeight(null))
                .width(Img.from(file).getImg().getWidth(null)).build();
        var fileUrl = DecompressionFileUtils.removeUrlParameter(URLUtil.decode(dataInfoUploadBO.getFileUrl()));
        var path = fileUrl.replace(minioProp.getEndpoint(), "").replace(minioProp.getBucketName() + "/", "");
        var fileBO = FileBO.builder().name(file.getName()).originalName(file.getName()).bucketName(minioProp.getBucketName())
                .size(file.length()).path(path).type(FileUtil.getMimeType(path)).extraInfo(JSONUtil.parseObj(imageExtraInfoBO)).build();
        var fileBOS = fileUseCase.saveBatchFile(userId, Collections.singletonList(fileBO));
        var fileNodeBO = DataInfoBO.FileNodeBO.builder().name(fileBO.getName())
                .fileId(CollectionUtil.getFirst(fileBOS).getId()).type(Constants.FILE).build();
        var dataName = getFilename(file);
        var content = DataInfoBO.FileNodeBO.builder().name(Constants.IMAGE_0).type(Constants.DIRECTORY).files(Collections.singletonList(fileNodeBO)).build();
        var dataInfoBO = dataInfoBOBuilder.name(dataName).orderName(NaturalSortUtil.convert(getFilename(file)))
                .content(Collections.singletonList(content)).splitType(SplitTypeEnum.NOT_SPLIT).build();
        var errorBuilder = new StringBuilder();
        try {
            dataInfoDAO.save(DefaultConverter.convert(dataInfoBO, DataInfo.class));
        } catch (DuplicateKeyException e) {
            log.error("Duplicate data name", e);
            errorBuilder.append("Duplicate data name;");
        }
        var rootPath = String.format("%s/%s", userId, datasetId);
        var newSavePath = tempPath + fileBO.getPath().replace(rootPath, "");
        FileUtil.copy(dataInfoUploadBO.getSavePath(), newSavePath, true);
        createUploadThumbnail(userId, fileBOS, rootPath);
        FileUtil.clean(newSavePath);
        var uploadRecordBO = UploadRecordBO.builder()
                .id(dataInfoUploadBO.getUploadRecordId()).totalDataNum(1L).parsedDataNum(1L).errorMessage(errorBuilder.toString()).status(PARSE_COMPLETED).build();
        uploadRecordDAO.updateById(DefaultConverter.convert(uploadRecordBO, UploadRecord.class));
        datasetSimilarityJobUseCase.submitJob(datasetId);
    }

    public void parseTextUploadFile(DataInfoUploadBO dataInfoUploadBO) {
        var userId = dataInfoUploadBO.getUserId();
        var datasetId = dataInfoUploadBO.getDatasetId();
        var files = FileUtil.loopFiles(Paths.get(dataInfoUploadBO.getBaseSavePath()), 10, textFileFilter);
        var rootPath = String.format("%s/%s", userId, datasetId);
        var errorBuilder = new StringBuilder();
        var dataInfoBOBuilder = DataInfoBO.builder().datasetId(datasetId)
                .parentId(Constants.DEFAULT_PARENT_ID)
                .type(ItemTypeEnum.SINGLE_DATA)
                .status(DataStatusEnum.VALID)
                .annotationStatus(DataAnnotationStatusEnum.NOT_ANNOTATED)
                .createdAt(OffsetDateTime.now())
                .updatedAt(OffsetDateTime.now())
                .createdBy(userId)
                .isDeleted(false);
        var totalDataNum = Long.valueOf(files.size());
        AtomicReference<Long> parsedDataNum = new AtomicReference<>(0L);
        var uploadRecordBOBuilder = UploadRecordBO.builder()
                .id(dataInfoUploadBO.getUploadRecordId()).totalDataNum(totalDataNum).parsedDataNum(parsedDataNum.get()).status(UploadStatusEnum.PARSING);
        if (CollectionUtil.isNotEmpty(files)) {
            CountDownLatch countDownLatch = new CountDownLatch(files.size());
            files.forEach(f -> parseExecutorService.submit(Objects.requireNonNull(TtlRunnable.get(() -> {
                try {
                    var dataInfoBOList = new ArrayList<DataInfoBO>();
                    var textJson = JSONUtil.readJSONArray(f, StandardCharsets.UTF_8);
                    var list = JSONUtil.toList(textJson.toString(), TextDataContentBO.class);
                    var pathList = this.getTreeAllPath(list);
                    if (CollUtil.isEmpty(pathList)) {
                        return;
                    }
                    var newTextFileList = new ArrayList<File>();
                    AtomicInteger i = new AtomicInteger(1);
                    pathList.forEach(path -> {
                        ListUtil.reverse(path);
                        var suffix = FileUtil.getSuffix(f);
                        var originalPath = f.getAbsolutePath();
                        var newPath = String.format("%s_%s.%s", StrUtil.removeSuffix(originalPath, String.format(".%s", suffix)), i.get(), suffix);
                        var file = FileUtil.writeString(JSONUtil.toJsonStr(path), newPath, StandardCharsets.UTF_8);
                        newTextFileList.add(file);
                        i.getAndIncrement();
                    });
                    var fileBOS = uploadFileList(rootPath, newTextFileList, dataInfoUploadBO);
                    createUploadThumbnail(userId, fileBOS, rootPath);
                    fileBOS.forEach(fileBO -> {
                        var tempDataId = ByteUtil.bytesToLong(SecureUtil.md5().digest(UUID.randomUUID().toString()));
                        var file = FileUtil.file(tempPath + fileBO.getPath().replace(rootPath, ""));
                        var fileNodeBO = DataInfoBO.FileNodeBO.builder().name(fileBO.getName())
                                .fileId(fileBO.getId()).type(FILE).build();
                        var dataName = getFilename(file);
                        var dataInfoBO = dataInfoBOBuilder.name(dataName).orderName(NaturalSortUtil.convert(dataName)).content(Collections.singletonList(fileNodeBO)).splitType(NOT_SPLIT).tempDataId(tempDataId).build();
                        dataInfoBOList.add(dataInfoBO);
                    });
                    if (CollectionUtil.isNotEmpty(dataInfoBOList)) {
                        insertBatch(dataInfoBOList, datasetId, errorBuilder, Constants.DEFAULT_PARENT_ID);
                    }
                } catch (Exception e) {
                    log.error("Handle data error", e);
                } finally {
                    parsedDataNum.set(parsedDataNum.get() + 1);
                    var uploadRecordBO = uploadRecordBOBuilder.parsedDataNum(parsedDataNum.get()).build();
                    uploadRecordDAO.updateById(DefaultConverter.convert(uploadRecordBO, UploadRecord.class));
                    countDownLatch.countDown();
                }

            }))));
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                log.error("Parse image count down latch error", e);
            }
            var uploadRecordBO = uploadRecordBOBuilder.parsedDataNum(totalDataNum).errorMessage(errorBuilder.toString()).status(PARSE_COMPLETED).build();
            uploadRecordDAO.updateById(DefaultConverter.convert(uploadRecordBO, UploadRecord.class));
            datasetSimilarityJobUseCase.submitJob(datasetId);
        } else {
            var uploadRecordBO = uploadRecordBOBuilder.status(FAILED).errorMessage(COMPRESSED_PACKAGE_EMPTY.getMessage()).build();
            uploadRecordDAO.updateById(DefaultConverter.convert(uploadRecordBO, UploadRecord.class));
            log.error("Image compressed package is empty,dataset id:{},filePath:{}", datasetId, dataInfoUploadBO.getFileUrl());
        }
    }

    public void parseImageCompressedUploadFile(DataInfoUploadBO dataInfoUploadBO) {
        if (DataFormatEnum.COCO.equals(dataInfoUploadBO.getDataFormat())) {
            var respPath = cocoConvertToX1(dataInfoUploadBO);
            var apiResult = new ApiResult<>();
            if (!FileUtil.exist(respPath) || !(OK.equals((apiResult = DefaultConverter.convert(JSONUtil.readJSONObject(FileUtil.file(respPath), Charset.defaultCharset()), ApiResult.class)).getCode()))) {
                var uploadRecordBOBuilder = UploadRecordBO.builder()
                        .id(dataInfoUploadBO.getUploadRecordId());
                var uploadRecordBO = uploadRecordBOBuilder.status(FAILED).errorMessage(FileUtil.exist(respPath) ? apiResult.getMessage() : DATASET_DATA_FILE_FORMAT_ERROR.getMessage()).build();
                uploadRecordDAO.updateById(DefaultConverter.convert(uploadRecordBO, UploadRecord.class));
                FileUtil.del(respPath);
                return;
            }
            FileUtil.del(respPath);
        }
        this.addDeviceName(DatasetTypeEnum.IMAGE, dataInfoUploadBO.getSavePath());
        this.commonParseUploadFile(dataInfoUploadBO, imageUploadUseCase::findImageParentList, imageUploadUseCase::getDataNames);
    }

    public List<String> addDeviceName(DatasetTypeEnum datasetType, String dataPath) {
        // 图片数据集
        if (datasetType.equals(DatasetTypeEnum.IMAGE)) {
            var imageList = imageUploadUseCase.findImageList(dataPath);
            return addDeviceName(imageList, Constants.IMAGE_0);
        }
        return ListUtil.empty();
    }

    /**
     * Supplement the name of the upper-level device. If it already exists, do not add it.
     *
     * @param fileList   File List
     * @param deviceName Device name
     * @return Added file list of device names
     */
    private List<String> addDeviceName(List<File> fileList, String deviceName) {
        List<String> addedDeviceNameList = new ArrayList<>();
        if (CollUtil.isEmpty(fileList)) {
            return addedDeviceNameList;
        }
        fileList.forEach(file -> {
            var parentFile = file.getParentFile();
            if (!parentFile.getName().equals(deviceName)) {
                String newPath = parentFile.getAbsolutePath() + File.separator + deviceName + File.separator + file.getName();
                File newFile = new File(newPath);
                FileUtil.mkdir(newFile);
                FileUtil.moveContent(file, newFile, true);
                addedDeviceNameList.add(newFile.getAbsolutePath());
            }
        });
        return addedDeviceNameList;
    }

    public void commonParseUploadFile(DataInfoUploadBO dataInfoUploadBO, BiConsumer<String, Set<File>> sceneFileListConsumer,
                                      Function<File, List<String>> getDataNamesFunction) {
        var errorBuilder = new StringBuilder();
        var datasetId = dataInfoUploadBO.getDatasetId();
        var userId = dataInfoUploadBO.getUserId();
        var datasetType = dataInfoUploadBO.getType();
        // Get the parent folder whose folder name is image_. If it is a point cloud, it contains lidar_point_cloud_parent folder.
        var sceneFileList = new HashSet<File>();
        sceneFileListConsumer.accept(dataInfoUploadBO.getBaseSavePath(), sceneFileList);
        var rootPath = String.format("%s/%s", userId, datasetId);
        log.info("Get point_cloud datasetId:{},size:{}", datasetId, sceneFileList.size());
        if (CollectionUtil.isEmpty(sceneFileList)) {
            log.error("The format of the compression package is incorrect. It must contain point_cloud_ or image,userId:{},datasetId:{},fileUrl:{}",
                    userId, datasetId, dataInfoUploadBO.getFileUrl());
            uploadUseCase.updateUploadRecordStatus(dataInfoUploadBO.getUploadRecordId(), FAILED, COMPRESSED_FILE_ERROR.getMessage());
            return;
        }
        var totalDataNum = sceneFileList.stream().filter(Objects::nonNull).mapToLong(file -> getDataNamesFunction.apply(file).size()).sum();
        AtomicReference<Long> parsedDataNum = new AtomicReference<>(0L);
        var uploadRecordBOBuilder = UploadRecordBO.builder()
                .id(dataInfoUploadBO.getUploadRecordId()).totalDataNum(totalDataNum).parsedDataNum(parsedDataNum.get()).status(PARSING);
        if (totalDataNum <= 0) {
            uploadUseCase.updateUploadRecordStatus(dataInfoUploadBO.getUploadRecordId(), FAILED, COMPRESSED_PACKAGE_EMPTY.getMessage());
            throw new UsecaseException(COMPRESSED_PACKAGE_EMPTY);
        }
        Long sourceId = null;
        if (ObjectUtil.isNotNull(dataInfoUploadBO.getResultType())) {
            sourceId = -1L;
            if (ResultTypeEnum.MODEL_RUN.equals(dataInfoUploadBO.getResultType())) {
                sourceId = modelRunRecordUseCase.save(dataInfoUploadBO.getModelId(), datasetId, totalDataNum);
            }
        }
        var dataAnnotationObjectBOBuilder = DataAnnotationObjectBO.builder()
                .datasetId(datasetId).createdBy(userId).createdAt(OffsetDateTime.now()).sourceId(sourceId);
        sceneFileList.forEach(sceneFile -> {
            Long sceneId;
            try {
                sceneId = this.saveScene(sceneFile, dataInfoUploadBO);
            } catch (DuplicateKeyException e) {
                log.error("The scene already exists,scene name is {}", sceneFile.getName());
                errorBuilder.append("Duplicate scene names:").append(sceneFile.getName()).append(";");
                return;
            }

            var dataNameList = getDataNamesFunction.apply(sceneFile);
            if (CollectionUtil.isEmpty(dataNameList)) {
                log.error("The file in {} folder is empty", sceneFile);
                errorBuilder.append("The file in ").append(sceneFile.getName()).append(" folder is empty;");
                return;
            }
            log.info("Get data name,pointCloudParentName:{},dataName:{} ", sceneFile, JSONUtil.toJsonStr(dataNameList));
            var dataInfoBOBuilder = DataInfoBO.builder().datasetId(datasetId)
                    .parentId(sceneId)
                    .status(DataStatusEnum.VALID)
                    .annotationStatus(DataAnnotationStatusEnum.NOT_ANNOTATED)
                    .splitType(NOT_SPLIT)
                    .createdAt(OffsetDateTime.now())
                    .updatedAt(OffsetDateTime.now())
                    .createdBy(userId)
                    .isDeleted(false);
            var list = ListUtil.split(dataNameList, 5);
            CountDownLatch countDownLatch = new CountDownLatch(list.size());
            list.forEach(subDataNameList -> parseExecutorService.submit(Objects.requireNonNull(TtlRunnable.get(() -> {
                var dataInfoBOList = new ArrayList<DataInfoBO>();
                var dataAnnotationObjectBOList = new ArrayList<DataAnnotationObjectBO>();
                try {
                    subDataNameList.forEach(dataName -> {
                        var dataFiles = this.getSingleDataFiles(sceneFile, dataName, errorBuilder, datasetType);
                        if (CollectionUtil.isNotEmpty(dataFiles)) {
                            log.info("dataStart,dataName:{},dataFiles:{}",dataName,dataFiles.stream().map(File::getName).collect(Collectors.toList()));
                            var tempDataId = ByteUtil.bytesToLong(SecureUtil.md5().digest(UUID.randomUUID().toString()));
                            var dataAnnotationObjectBO = dataAnnotationObjectBOBuilder.build();
                            dataAnnotationObjectBO.setDataId(tempDataId);
                            handleDataResult(sceneFile, dataName, dataAnnotationObjectBO, dataAnnotationObjectBOList, errorBuilder);
                            var fileNodeList = this.assembleContent(dataFiles, rootPath, dataInfoUploadBO);
                            log.info("Get data content,frameName:{},content:{} ", dataName, JSONUtil.toJsonStr(fileNodeList));
                            var dataInfoBO = dataInfoBOBuilder.build();
                            dataInfoBO.setName(dataName);
                            dataInfoBO.setOrderName(NaturalSortUtil.convert(dataName));
                            dataInfoBO.setContent(fileNodeList);
                            dataInfoBO.setTempDataId(tempDataId);
                            dataInfoBOList.add(dataInfoBO);
                        }
                    });
                    if (CollectionUtil.isNotEmpty(dataInfoBOList)) {
                        log.info("dataInfoBOList:{}",dataInfoBOList.stream().map(DataInfoBO::getTempDataId).collect(Collectors.toList()));
                        log.info("dataAnnotationObjectBOList:{}",dataAnnotationObjectBOList.stream().map(DataAnnotationObjectBO::getDataId).collect(Collectors.toList()));
                        var resDataInfoList = this.insertBatch(dataInfoBOList, datasetId, errorBuilder, sceneId);
                        this.saveBatchDataResult(resDataInfoList, dataAnnotationObjectBOList);
                    }
                } catch (Exception e) {
                    log.error("Handle data error", e);
                } finally {
                    parsedDataNum.set(parsedDataNum.get() + subDataNameList.size());
                    var uploadRecordBO = uploadRecordBOBuilder.parsedDataNum(parsedDataNum.get()).build();
                    uploadRecordDAO.updateById(DefaultConverter.convert(uploadRecordBO, UploadRecord.class));
                    countDownLatch.countDown();
                }
            }))));
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                log.error("Parse point cloud count down latch error", e);
            }
        });
        var uploadRecordBO = uploadRecordBOBuilder.parsedDataNum(totalDataNum).errorMessage(errorBuilder.toString()).status(PARSE_COMPLETED).build();
        uploadRecordDAO.updateById(DefaultConverter.convert(uploadRecordBO, UploadRecord.class));
        if (ObjectUtil.isNotNull(sourceId) && ResultTypeEnum.MODEL_RUN.equals(dataInfoUploadBO.getResultType())) {
            modelRunRecordUseCase.updateById(sourceId, RunStatusEnum.SUCCESS);
        }
    }

    /**
     * Verify url file suffix
     *
     * @param dataInfoUploadBO Upload data object
     * @param mimeType         Mime type
     * @return boolean
     */
    private boolean validateUrlFileSuffix(DataInfoUploadBO dataInfoUploadBO, String mimeType) {
        boolean boo;
        boo = COMPRESSED_DATA_TYPE.contains(mimeType);
        if (IMAGE.equals(dataInfoUploadBO.getType()) && LOCAL.equals(dataInfoUploadBO.getSource())) {
            boo = boo || IMAGE_DATA_TYPE.contains(mimeType);
        }
        return boo;
    }

    /**
     * Find a file of data
     *
     * @param file        File
     * @param dataName    Data name
     * @param datasetType Dataset type
     */
    private List<File> getSingleDataFiles(File file, String dataName, StringBuilder stringBuilder, DatasetTypeEnum datasetType) {
        var singleDataFile = new ArrayList<File>();
        var isErr = false;
        for (var f : file.listFiles()) {
            var boo = this.validateFilenameByType(f, datasetType);
            if (boo) {
                var fcList = Arrays.stream(f.listFiles()).filter(fc -> (this.validateFileFormat(fc, datasetType) &&
                        this.getFilename(fc).equals(dataName))).collect(Collectors.toList());
                var count = fcList.size();
                switch (count) {
                    case 0:
                        break;
                    case 1:
                        singleDataFile.addAll(fcList);
                        break;
                    default:
                        log.error("There are duplicate files in {} folder,file name is {}", f.getName(), dataName);
                        stringBuilder.append("There are duplicate files in ").append(f.getName()).append(" folder,file name is ").append(dataName);
                        isErr = true;
                }
            }
        }
        return isErr ? ListUtil.empty() : singleDataFile;
    }

    /**
     * Verify that the file name is correct based on the dataset type
     *
     * @param file        Zip file
     * @param datasetType Dataset type
     * @return boolean
     */
    private boolean validateFilenameByType(File file, DatasetTypeEnum datasetType) {
        var filename = file.getName().toLowerCase().trim();
        if (DatasetTypeEnum.IMAGE.equals(datasetType)) {
            return file.isDirectory() && filename.startsWith(Constants.IMAGE);
        } else {
            return file.isDirectory() && (filename.startsWith(Constants.CAMERA_IMAGE) || filename.startsWith(LIDAR_POINT_CLOUD) ||
                    filename.equalsIgnoreCase(CAMERA_CONFIG));
        }
    }

    /**
     * Verify that the file format is correct
     *
     * @param file        file
     * @param datasetType Dataset type
     * @return Is the file format correct
     */
    private boolean validateFileFormat(File file, DatasetTypeEnum datasetType) {
        var boo = false;
        var mimeType = FileUtil.getMimeType(file.getAbsolutePath());
        var filename = file.getName();
        if (DatasetTypeEnum.IMAGE.equals(datasetType)) {
            boo = Constants.IMAGE_DATA_TYPE.contains(mimeType);
        } else {
            boo = Constants.IMAGE_DATA_TYPE.contains(mimeType) || Constants.PCD_SUFFIX.equalsIgnoreCase(FileUtil.getSuffix(filename)) ||
                    file.getAbsolutePath().toUpperCase().endsWith(Constants.JSON_SUFFIX);
        }
        if (!boo) {
            log.warn("this format file is not supported,filePath:{}", file.getAbsolutePath());
        }
        return boo;
    }

    @Transactional(rollbackFor = Exception.class)
    public Long saveScene(File file, DataInfoUploadBO uploadDataBO) {
        var fileName = FileUtil.getName(file);
        if (fileName.toLowerCase().startsWith(Constants.SCENE)) {
            var datasetId = uploadDataBO.getDatasetId();
            var userId = uploadDataBO.getUserId();
            var dataInfo = DataInfo.builder().datasetId(datasetId).name(fileName).orderName(NaturalSortUtil.convert(fileName)).parentId(Constants.DEFAULT_PARENT_ID)
                    .type(ItemTypeEnum.SCENE).createdBy(userId).createdAt(OffsetDateTime.now()).parentId(null).build();
            dataInfoDAO.save(dataInfo);
            return dataInfo.getId();
        }
        return Constants.DEFAULT_PARENT_ID;
    }

    /**
     * Process the annotation results corresponding to data
     *
     * @param file                       The image is the parent of data, and the point cloud file is point_cloud
     * @param dataName                   Data name
     * @param dataAnnotationObjectBO     Data annotation object
     * @param dataAnnotationObjectBOList Data annotation object list
     */
    public void handleDataResult(File file, String dataName, DataAnnotationObjectBO dataAnnotationObjectBO,
                                 List<DataAnnotationObjectBO> dataAnnotationObjectBOList, StringBuilder errorBuilder) {

        // Indicates that no result data was imported
        if (ObjectUtil.isNotNull(dataAnnotationObjectBO.getSourceId())) {
            var resultFile = FileUtil.loopFiles(file, 2, null).stream()
                    .filter(fc -> fc.getName().toUpperCase().endsWith(JSON_SUFFIX) && dataName.equals(FileUtil.getPrefix(fc))
                            && fc.getParentFile().getName().equalsIgnoreCase(RESULT)).findFirst();
            if (resultFile.isPresent()) {
                log.info("dataResult,dataName:{},resultFileName:{}",dataName,resultFile.get().getName());
                try {
                    var resultJson = JSONUtil.readJSON(resultFile.get(), Charset.defaultCharset());
                    var result = new DataImportResultBO();
                    if (resultJson instanceof JSONArray) {
                        var dataImportResultBOList = JSONUtil.toList(JSONUtil.toJsonStr(resultJson), DataImportResultBO.class);
                        var objects = new ArrayList<DataAnnotationResultObjectBO>();
                        dataImportResultBOList.stream().filter(dataImportResultBO -> CollUtil.isNotEmpty(dataImportResultBO.getObjects())).forEach(dataImportResultBO ->
                                objects.addAll(dataImportResultBO.getObjects()));
                        result.setObjects(objects);
                    } else {
                        result = JSONUtil.toBean(JSONUtil.toJsonStr(resultJson), DataImportResultBO.class);
                    }

                    this.verifyDataResult(result, dataAnnotationObjectBO.getDataId(), dataName, errorBuilder);
                    var classMap = getClassMap(dataAnnotationObjectBO.getDatasetId(), result.getObjects());
                    result.getObjects().forEach(object -> {
                        var insertDataAnnotationObjectBO = DefaultConverter.convert(dataAnnotationObjectBO, DataAnnotationObjectBO.class);
                        object.setId(IdUtil.fastSimpleUUID());
                        object.setVersion(0);
                        processClassAttributes(classMap, object);
                        Objects.requireNonNull(insertDataAnnotationObjectBO).setClassAttributes(JSONUtil.parseObj(object));
                        insertDataAnnotationObjectBO.setClassId(object.getClassId());
                        dataAnnotationObjectBOList.add(insertDataAnnotationObjectBO);
                    });
                } catch (Exception e) {
                    log.error("Handle result json error,userId:{},datasetId:{}", dataAnnotationObjectBO.getCreatedBy(), dataAnnotationObjectBO.getDatasetId(), e);
                }
            }
        }
    }

    private Map<Long, DatasetClassBO> getClassMap(Long datasetId, List<DataAnnotationResultObjectBO> objects) {
        if (CollUtil.isEmpty(objects)) {
            return new HashMap<>();
        }
        var classIds = objects.stream().map(DataAnnotationResultObjectBO::getClassId)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());
        if (CollUtil.isEmpty(classIds)) {
            return new HashMap<>();
        }
        var datasetClassBOList = datasetClassUseCase.findByIds(datasetId, new ArrayList<>(classIds));
        if (CollUtil.isNotEmpty(datasetClassBOList)) {
            return datasetClassBOList.stream().collect(Collectors.toMap(DatasetClassBO::getId,
                    t -> t));
        }
        return Map.of();
    }

    private void processClassAttributes(Map<Long, DatasetClassBO> classMap,
                                        DataAnnotationResultObjectBO object) {
        if (object == null) {
            return;
        }
        var classId = object.getClassId();
        if (classMap.containsKey(classId)) {
            object.setClassId(classId);
            object.setClassName(classMap.get(classId).getName());
        } else {
            object.setClassId(null);
            object.setClassValues(null);
            if (StrUtil.isNotEmpty(object.getClassName())) {
                object.setModelClass(object.getClassName());
                object.setClassName(null);
            }
        }
    }

    /**
     * Assemble the content of a single frame
     *
     * @param dataFiles        Data composition file
     * @param rootPath         Root path
     * @param dataInfoUploadBO Compression package name
     * @return content
     */
    private List<DataInfoBO.FileNodeBO> assembleContent(List<File> dataFiles, String rootPath, DataInfoUploadBO dataInfoUploadBO) {
        var nodeList = new ArrayList<DataInfoBO.FileNodeBO>();
        var files = new ArrayList<File>();
        dataFiles.forEach(dataFile -> {
            var parentName = dataFile.getParentFile().getName();
            var node = DataInfoBO.FileNodeBO.builder()
                    .name(parentName)
                    .type(DIRECTORY)
                    .files(getDirList(dataFile, rootPath, files)).build();
            nodeList.add(node);
        });
        var fileBOS = uploadFileList(rootPath, files, dataInfoUploadBO);
        createUploadThumbnail(dataInfoUploadBO.getUserId(), fileBOS, rootPath);
        fileBOS.forEach(fileBO -> {
            if (fileBO.getName().toUpperCase().endsWith(PCD_SUFFIX)) {
                handelPointCloudConvertRender(fileBO);
            }
        });
        var fileIdMap = fileBOS.stream().collect(Collectors.toMap(FileBO::getPathHash, FileBO::getId));
        replaceFileId(nodeList, fileIdMap);
        nodeList.sort(Comparator.comparing(DataInfoBO.FileNodeBO::getName));
        return nodeList;
    }

    private List<DataInfoBO.FileNodeBO> getDirList(File node, String rootPath, List<File> files) {
        List<DataInfoBO.FileNodeBO> nodeList = new ArrayList<>();
        if (node.isDirectory()) {
            for (File n : Objects.requireNonNull(node.listFiles())) {
                nodeList.add(getNode(n, rootPath, files));
            }
        } else {
            nodeList.add(getNode(node, rootPath, files));
        }
        return nodeList;
    }

    private DataInfoBO.FileNodeBO getNode(File node, String rootPath, List<File> files) {
        if (node.isDirectory()) {
            return DataInfoBO.FileNodeBO.builder()
                    .name(StrUtil.toCamelCase(node.getName()))
                    .type(DIRECTORY)
                    .files(getDirList(node, rootPath, files)).build();
        } else {
            var path = rootPath + FileUtil.getAbsolutePath(node.getAbsolutePath()).replace(FileUtil.getAbsolutePath(FileUtil.file(tempPath).getAbsolutePath()), "");
            var fileId = ByteUtil.bytesToLong(SecureUtil.md5().digest(path));
            files.add(node);
            return DataInfoBO.FileNodeBO.builder()
                    .name(node.getName())
                    .fileId(fileId)
                    .type(FILE).build();
        }
    }

    /**
     * Replace the fileId in content with the real fileId
     *
     * @param nodeList  File node list
     * @param fileIdMap File id map
     */
    private void replaceFileId(List<DataInfoBO.FileNodeBO> nodeList, Map<Long, Long> fileIdMap) {
        nodeList.forEach(fileNodeBO -> {
            if (fileNodeBO.getType().equals(FILE)) {
                fileNodeBO.setFileId(fileIdMap.get(fileNodeBO.getFileId()));
            } else if (fileNodeBO.getType().equals(DIRECTORY) && CollectionUtil.isNotEmpty(fileNodeBO.getFiles())) {
                replaceFileId(fileNodeBO.getFiles(), fileIdMap);
            }
        });
    }

    /**
     * Batch save data results
     *
     * @param dataInfoBOList             Data list
     * @param dataAnnotationObjectBOList Data annotation object list
     */
    public void saveBatchDataResult(List<DataInfoBO> dataInfoBOList, List<DataAnnotationObjectBO> dataAnnotationObjectBOList) {
        var dataIdMap = dataInfoBOList.stream()
                .collect(Collectors.toMap(DataInfoBO::getTempDataId, DataInfoBO::getId, (k1, k2) -> k1));
        if (CollectionUtil.isNotEmpty(dataAnnotationObjectBOList)) {
            var newDataAnnotationObjectBOList = dataAnnotationObjectBOList.stream().filter(d -> dataIdMap.containsKey(d.getDataId())).collect(Collectors.toList());
            if (CollUtil.isNotEmpty(newDataAnnotationObjectBOList)) {
                newDataAnnotationObjectBOList.forEach(d -> d.setDataId(dataIdMap.get(d.getDataId())));
                dataAnnotationObjectDAO.getBaseMapper().insertBatch(DefaultConverter.convert(dataAnnotationObjectBOList, DataAnnotationObject.class));
                newDataAnnotationObjectBOList.clear();
            }
            dataAnnotationObjectBOList.clear();
        }
    }

    /**
     * Batch upload files
     *
     * @param rootPath         Path prefix
     * @param files            File list
     * @param dataInfoUploadBO Data upload information
     * @return File information list
     */
    public List<FileBO> uploadFileList(String rootPath, List<File> files, DataInfoUploadBO dataInfoUploadBO) {
        var bucketName = minioProp.getBucketName();
        try {
            minioService.uploadFileList(bucketName, rootPath, tempPath, files);
        } catch (Exception e) {
            log.error("Batch upload file error,filesPath:{}", JSONUtil.parseArray(files.stream().map(File::getAbsolutePath).collect(Collectors.toList())), e);
        }

        var fileBOS = new ArrayList<FileBO>();
        files.forEach(file -> {
            var startingPosition = FileUtil.getAbsolutePath(FileUtil.file(dataInfoUploadBO.getBaseSavePath()).getAbsolutePath()).length() + 1;
            var zipPath = FileUtil.getAbsolutePath(file.getAbsolutePath()).substring(startingPosition);
            var path = String.format("%s%s", rootPath, FileUtil.getAbsolutePath(file.getAbsolutePath()).replace(FileUtil.getAbsolutePath(FileUtil.file(tempPath).getAbsolutePath()), ""));
            zipPath = zipPath.startsWith(dataInfoUploadBO.getFileName()) ? zipPath : String.format("%s/%s", dataInfoUploadBO.getFileName(), zipPath);
            var mimeType = FileUtil.getMimeType(path);
            var fileBO = FileBO.builder().name(file.getName()).originalName(file.getName()).bucketName(bucketName)
                    .size(file.length()).path(path).zipPath(zipPath).type(mimeType).build();
            if (Constants.IMAGE_DATA_TYPE.contains(mimeType)) {
                var imageExtraInfoBO = ImageExtraInfoBO.builder().height(Img.from(file).getImg().getHeight(null))
                        .width(Img.from(file).getImg().getWidth(null)).build();
                fileBO.setExtraInfo(JSONUtil.parseObj(imageExtraInfoBO));
            }
            fileBOS.add(fileBO);
        });
        return fileUseCase.saveBatchFile(dataInfoUploadBO.getUserId(), fileBOS);
    }

    /**
     * Generate thumbnail and upload
     *
     * @param userId   User id
     * @param fileBOS  File collection
     * @param rootPath Root path
     */
    private void createUploadThumbnail(Long userId, List<FileBO> fileBOS, String rootPath) {
        try {
            var thumbnailFileBOS = new ArrayList<FileBO>();
            var files = new ArrayList<File>();
            for (FileBO fileBO : fileBOS) {
                var mimeType = fileBO.getType();
                var savePath = tempPath + fileBO.getPath().replace(rootPath, "");
                if (IMAGE_DATA_TYPE.contains(mimeType)) {
                    var filePath = fileBO.getPath();
                    var basePath = filePath.substring(0, filePath.lastIndexOf(SLANTING_BAR) + 1);
                    var fileName = filePath.substring(filePath.lastIndexOf(SLANTING_BAR) + 1);
                    var fileBOBuilder = FileBO.builder().name(fileBO.getName()).originalName(fileBO.getName())
                            .bucketName(fileBO.getBucketName()).type(mimeType);
                    var file = FileUtil.file(savePath);
                    var suffix = FileUtil.getSuffix(fileName);
                    suffix = suffix.equalsIgnoreCase(TIFF_SUFFIX) || suffix.equalsIgnoreCase(TIF_SUFFIX) ? "jpg" : suffix;
                    var prefix = FileUtil.getPrefix(fileName);
                    var baseSavePath = file.getParentFile().getAbsolutePath();
                    var largeFile = FileUtil.file(baseSavePath, String.format("%s_%s.%s", prefix, large, suffix));
                    var mediumFile = FileUtil.file(baseSavePath, String.format("%s_%s.%s", prefix, medium, suffix));
                    var smallFile = FileUtil.file(baseSavePath, String.format("%s_%s.%s", prefix, small, suffix));
                    Thumbnails.of(file).size(largeFileSize, largeFileSize).toFile(largeFile);
                    Thumbnails.of(largeFile).size(mediumFileSize, mediumFileSize).toFile(mediumFile);
                    Thumbnails.of(largeFile).size(smallFileSize, smallFileSize).toFile(smallFile);
                    // large thumbnail
                    var largePath = String.format("%s%s", basePath, FileUtil.getName(largeFile));
                    var largeFileBO = fileBOBuilder.path(largePath).relation(LARGE_THUMBTHUMBNAIL).relationId(fileBO.getId()).build();
                    // medium thumbnail
                    var mediumPath = String.format("%s%s", basePath, FileUtil.getName(mediumFile));
                    var mediumFileBO = fileBOBuilder.path(mediumPath).relation(MEDIUM_THUMBTHUMBNAIL).relationId(fileBO.getId()).build();
                    // small thumbnail
                    var smallPath = String.format("%s%s", basePath, FileUtil.getName(smallFile));
                    var smallFileBO = fileBOBuilder.path(smallPath).relation(SMALL_THUMBTHUMBNAIL).relationId(fileBO.getId()).build();
                    thumbnailFileBOS.addAll(ListUtil.toList(largeFileBO, mediumFileBO, smallFileBO));
                    files.addAll(ListUtil.toList(largeFile, mediumFile, smallFile));
                }
            }
            try {
                minioService.uploadFileList(minioProp.getBucketName(), rootPath, tempPath, files);
            } catch (Exception e) {
                log.error("Batch upload file error,filesPath:{}", JSONUtil.parseArray(files.stream().map(File::getAbsolutePath).collect(Collectors.toList())), e);
            }
            fileUseCase.saveBatchFile(userId, thumbnailFileBOS);
        } catch (IOException e) {
            log.error("Upload thumbnail error", e);
        }
    }

    public void handelPointCloudConvertRender(FileBO pcdFileBO) {
        String filePath = pcdFileBO.getPath();
        String basePath = "";
        String fileName;
        String binaryFileName = "";
        String imageFileName = "";
        if (filePath.contains(Constants.SLANTING_BAR)) {
            basePath = filePath.substring(0, filePath.lastIndexOf(Constants.SLANTING_BAR) + 1);
            fileName = filePath.substring(filePath.lastIndexOf(Constants.SLANTING_BAR) + 1);
            binaryFileName = "binary-" + fileName;
            imageFileName = "render-" + UUID.randomUUID() + ".png";
        }

        String binaryPath = String.format("%s%s", basePath, binaryFileName);
        String imagePath = String.format("%s%s", basePath, imageFileName);
        PresignedUrlBO binaryPreSignUrlBO;
        PresignedUrlBO imagePreSignUrlBO;
        try {
            binaryPreSignUrlBO = minioService.generatePresignedUrl(pcdFileBO.getBucketName(), binaryPath, Boolean.FALSE);
            imagePreSignUrlBO = minioService.generatePresignedUrl(pcdFileBO.getBucketName(), imagePath, Boolean.FALSE);

        } catch (Throwable throwable) {
            log.error("generate preSignUrl error!", throwable);
            return;
        }
        List<PointCloudCRRespDTO> pointCloudCRRespDTOS = callPointCloudConvertRender(pcdFileBO, binaryPreSignUrlBO, imagePreSignUrlBO);
        if (CollUtil.isNotEmpty(pointCloudCRRespDTOS)) {
            for (PointCloudCRRespDTO pointCloudCRRespDTO : pointCloudCRRespDTOS) {
                if (pointCloudCRRespDTO.getCode() == 0) {
                    FileBO binaryPcdFile = FileBO.builder().name(binaryFileName)
                            .originalName(binaryFileName)
                            .path(binaryPath)
                            .type(pcdFileBO.getType())
                            .size(pointCloudCRRespDTO.getBinaryPcdSize())
                            .bucketName(pcdFileBO.getBucketName())
                            .createdAt(OffsetDateTime.now())
                            .createdBy(pcdFileBO.getCreatedBy())
                            .relation(RelationEnum.BINARY)
                            .relationId(pcdFileBO.getId())
                            .pathHash(ByteUtil.bytesToLong(SecureUtil.md5().digest(binaryPath))).build();
                    FileBO imageFile = FileBO.builder().name(imageFileName)
                            .originalName(imageFileName)
                            .path(imagePath)
                            .type(FileUtil.getMimeType(imageFileName))
                            .size(pointCloudCRRespDTO.getImageSize())
                            .bucketName(pcdFileBO.getBucketName())
                            .createdAt(OffsetDateTime.now())
                            .createdBy(pcdFileBO.getCreatedBy())
                            .relation(RelationEnum.POINT_CLOUD_RENDER_IMAGE)
                            .relationId(pcdFileBO.getId())
                            .pathHash(ByteUtil.bytesToLong(SecureUtil.md5().digest(imagePath)))
                            .extraInfo(JSONUtil.parseObj(pointCloudCRRespDTO.getPointCloudRange())
                                    .set("width", PC_RENDER_IMAGE_WIDTH)
                                    .set("height", PC_RENDER_IMAGE_HEIGHT))
                            .build();
                    fileUseCase.saveBatchFile(pcdFileBO.getCreatedBy(), List.of(binaryPcdFile, imageFile));
                }
            }
        }
    }

    private List<PointCloudCRRespDTO> callPointCloudConvertRender(FileBO relationFileBO, PresignedUrlBO binaryPreSignUrlBO, PresignedUrlBO imagePreSignUrlBO) {
        PointCloudCRReqDTO pointCloudCRReqDTO = PointCloudCRReqDTO.builder()
                .data(List.of(buildPointCloutFileInfo(relationFileBO, binaryPreSignUrlBO, imagePreSignUrlBO)))
                .type(1)
                .renderParam(buildRenderParam())
                .convertParam(ConvertParam.builder().extraFields(List.of("rgb")).build()).build();
        ApiResult<List<PointCloudCRRespDTO>> apiResult = null;

        StopWatch stopWatch = new StopWatch();
        int count = 0;
        while (count <= RETRY_COUNT && ObjectUtil.isNull(apiResult)) {
            try {
                stopWatch.start();
                apiResult = pointCloudConvertRenderHttpCaller.callConvertRender(pointCloudCRReqDTO);
                stopWatch.stop();
                log.info("call pointCloudConvertRender took:{},req:{},resp:{}", stopWatch.getLastTaskTimeMillis(), JSONUtil.toJsonStr(pointCloudCRReqDTO), JSONUtil.toJsonStr(apiResult));
                break;
            } catch (Throwable throwable) {
                if (stopWatch.isRunning()) {
                    stopWatch.stop();
                }
                log.error("call pointCloudConvertRender service error! req:{}, resp:{}", JSONUtil.toJsonStr(pointCloudCRReqDTO), JSONUtil.toJsonStr(apiResult), throwable);
            }
            count++;
        }
        if (ObjectUtil.isNull(apiResult) || apiResult.getCode() != UsecaseCode.OK) {
            return null;
        }
        return apiResult.getData();
    }

    private PointCloudFileInfo buildPointCloutFileInfo(FileBO fileBO, PresignedUrlBO binaryPreSignUrlBO, PresignedUrlBO imagePreSignUrlBO) {
        return PointCloudFileInfo.builder().pointCloudFile(fileBO.getInternalUrl())
                .uploadBinaryPcdPath(binaryPreSignUrlBO.getPresignedUrl())
                .uploadImagePath(imagePreSignUrlBO.getPresignedUrl())
                .build();
    }

    private RenderParam buildRenderParam() {
        return RenderParam.builder().colors(List.of(6313414L, 3640543L, 1886145L, 3402883L, 8385883L))
                .zRange(null)
                .width(PC_RENDER_IMAGE_WIDTH)
                .height(PC_RENDER_IMAGE_HEIGHT)
                .numStd(3).build();
    }

    private void verifyDataResult(DataImportResultBO dataImportResultBO, Long dataId, String dataName, StringBuilder errorBuilder) {
        if (CollectionUtil.isEmpty(dataImportResultBO.getObjects())) {
            log.error("Objects is empty，dataId:{},dataName:{}", dataId, dataName);
            errorBuilder.append(FileUtil.getPrefix(dataName)).append(".json the objects in the result file cannot be empty;");
            return;
        }
        var objects = new ArrayList<DataAnnotationResultObjectBO>();
        var types = Arrays.stream(ObjectTypeEnum.values()).map(ObjectTypeEnum::getValue).collect(Collectors.toSet());
        dataImportResultBO.getObjects().forEach(dataAnnotationResultObjectBO -> {
            if (!types.contains(dataAnnotationResultObjectBO.getType())) {
                log.error("Object type error，dataId:{},dataName:{}", dataId, dataName);
                errorBuilder.append(FileUtil.getPrefix(dataName)).append(".json the objects exist object type error;");
            } else if (ObjectUtil.isNull(dataAnnotationResultObjectBO.getContour())) {
                log.error("Contour miss，dataId:{},dataName:{}", dataId, dataName);
                errorBuilder.append(FileUtil.getPrefix(dataName)).append(".json the objects exist object contour miss;");
            } else {
                objects.add(dataAnnotationResultObjectBO);
            }
        });
        dataImportResultBO.setObjects(objects);
    }

    /**
     * Get file name remove suffix
     *
     * @param file File or folder
     * @return File name
     */
    public String getFilename(File file) {
        var fileName = file.getName();
        if (FileUtil.isFile(file)) {
            fileName = fileName.substring(0, fileName.lastIndexOf("."));
        }
        return fileName;
    }

    /**
     * Batch insert
     *
     * @param dataInfoBOList Collection of data details
     */
    public List<DataInfoBO> insertBatch(List<DataInfoBO> dataInfoBOList, Long datasetId, StringBuilder errorBuilder, Long parentId) {
        var names = dataInfoBOList.stream().map(DataInfoBO::getName).collect(Collectors.toList());
        var existDataInfoList = this.findByNames(datasetId, parentId, names);
        if (CollUtil.isNotEmpty(existDataInfoList)) {
            var existNames = existDataInfoList.stream().map(DataInfoBO::getName).collect(Collectors.toList());
            dataInfoBOList = dataInfoBOList.stream().filter(dataInfoBO -> !existNames.contains(dataInfoBO.getName())).collect(Collectors.toList());
            if (!errorBuilder.toString().contains("Duplicate")) {
                errorBuilder.append("Duplicate data names;");
            }
        }
        if (CollUtil.isEmpty(dataInfoBOList)) {
            return List.of();
        }
        try {
            List<DataInfo> infos = DefaultConverter.convert(dataInfoBOList, DataInfo.class);
            dataInfoDAO.saveBatch(infos);
            return DefaultConverter.convert(infos, DataInfoBO.class);
        } catch (DuplicateKeyException e) {
            log.error("Duplicate data name", e);
            if (!errorBuilder.toString().contains("Duplicate")) {
                errorBuilder.append("Duplicate data names;");
            }
            return List.of();
        }
    }

    private List<DataInfoBO> findByNames(Long datasetId, Long parentId, List<String> names) {
        var dataInfoLambdaQueryWrapper = Wrappers.lambdaQuery(DataInfo.class);
        dataInfoLambdaQueryWrapper.eq(DataInfo::getDatasetId, datasetId);
        dataInfoLambdaQueryWrapper.in(DataInfo::getName, names);
        dataInfoLambdaQueryWrapper.eq(DataInfo::getParentId, parentId);
        return DefaultConverter.convert(dataInfoDAO.list(dataInfoLambdaQueryWrapper), DataInfoBO.class);
    }

    private String cocoConvertToX1(DataInfoUploadBO dataInfoUploadBO) {
        var fileName = FileUtil.getPrefix(dataInfoUploadBO.getSavePath());
        var baseSavePath = String.format("%s%s/", tempPath, IdUtil.fastSimpleUUID());
        String srcPath = dataInfoUploadBO.getBaseSavePath();
        String outPath = String.format("%s%s", baseSavePath, fileName);
        var respPath = String.format("%s%s/resp.json", tempPath, IdUtil.fastSimpleUUID());
        DataFormatUtil.convert(Constants.CONVERT_UPLOAD, srcPath, outPath, respPath);
        dataInfoUploadBO.setBaseSavePath(baseSavePath);
        return respPath;
    }

    /**
     * Get all paths in the tree list
     *
     * @param list tree list
     * @return
     */
    private List<List<TextDataContentBO>> getTreeAllPath(List<TextDataContentBO> list) {
        list = list.stream().filter(t -> StrUtil.isNotEmpty(t.getId()) && StrUtil.isNotEmpty(t.getRole()) && StrUtil.isNotEmpty(t.getText())).collect(Collectors.toList());
        if (CollUtil.isEmpty(list)) {
            return List.of();
        }
        // convert to tree
        List<Tree<String>> treeNodes = TreeUtil.build(list, null,
                (treeNode, tree) -> {
                    tree.setId(treeNode.getId());
                    tree.setParentId(treeNode.getParentId());
                    tree.setName(treeNode.getId());
                    // Extended properties ...
                    tree.putExtra("text", treeNode.getText());
                    tree.putExtra("role", treeNode.getRole());
                });

        var leafNodeList = new ArrayList<Tree<String>>();
        getLeafNodeList(treeNodes, leafNodeList);
        // get all links
        List<List<TextDataContentBO>> paths = new ArrayList<>();
        for (Tree<String> treeNode : leafNodeList) {
            List<TextDataContentBO> path = new ArrayList<>();
            path.add(DefaultConverter.convert(treeNode, TextDataContentBO.class));
            Tree<String> parent = treeNode.getParent();
            while (parent != null) {
                if (ObjectUtil.isNotNull(parent.getId())) {
                    path.add(DefaultConverter.convert(parent, TextDataContentBO.class));
                }
                parent = parent.getParent();
            }
            paths.add(path);
        }
        return paths;
    }

    /**
     * Get all leaf nodes under the tree
     *
     * @param treeNodes    tree node
     * @param leafNodeList collection of leaf nodes
     */
    private void getLeafNodeList(List<Tree<String>> treeNodes, List<Tree<String>> leafNodeList) {
        treeNodes.forEach(tree -> {
            if (CollUtil.isNotEmpty(tree.getChildren())) {
                getLeafNodeList(tree.getChildren(), leafNodeList);
            } else {
                leafNodeList.add(tree);
            }
        });
    }
}