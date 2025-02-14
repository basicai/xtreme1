package ai.basic.x1.usecase.exception;

/**
 * @author Jagger Wang
 */
public enum UsecaseCode {

    // Common
    OK("OK", "OK"),
    ERROR("ERROR", "Service error"),
    UNKNOWN("UNKNOWN", "Unknown error"),
    NOT_FOUND("NOT_FOUND", "Resource not found"),
    DUPLICATED("DUPLICATED", "Resource duplicated"),
    PARAM_ERROR("PARAM_ERROR", "Param error"),

    NAME_DUPLICATED("NAME_DUPLICATED", "name already exists"),

    // User
    USERNAME_AND_PASSWORD_NOT_MATCH("USERNAME_AND_PASSWORD_NOT_MATCH", "Username and password not match"),

    LOGIN_STATUS_TIMEOUT("LOGIN_STATUS_TIMEOUT", "User login status timeout"),

    USER_EXIST("USER_EXIST", "User already existed"),

    USER_NOT_REGISTER("USER_NOT_REGISTER", "User not register"),

    FILE_TYPE_NOT_SUPPORT("FILE_TYPE_NOT_SUPPORT", "File type not support"),

    // Dataset
    DATASET_NAME_DUPLICATED("DATASET_NAME_DUPLICATED", "Dataset duplicate name"),

    DATASET_DATA_FILE_URL_ERROR("DATASET_DATA_FILE_URL_ERROR", "File url error"),

    DATASET_DATA_FILE_URL_ILLEGAL("DATASET_DATA_FILE_URL_ILLEGAL", "File url illegal"),

    DATASET_DATA_FILE_FORMAT_ERROR("DATASET_DATA_FILE_FORMAT_ERROR", "Incorrect file format"),

    DATASET_NOT_FOUND("DATASET_NOT_FOUND", "Dataset not found"),

    DEFAULT_DATASET_NOT_FOUND("DEFAULT_DATASET_NOT_FOUND", "Default dataset not found"),
    DATA_NOT_FOUND("DATA_NOT_FOUND", "Data not found"),

    DATASET__DATA__DATA_HAS_BEEN_UNLOCKED("DATASET__DATA_DATA_HAS_BEEN_UNLOCKED", "The data has been unlocked"),

    DATASET_DATA_OTHERS_ANNOTATING("DATASET_DATA_OTHERS_ANNOTATING", "Select data that others are annotating"),

    DATASET_DATA_UNLOCK_ID_ERROR("DATASET__DATA__UNLOCK_ID_ERROR", "Please pass in the correct unlock id"),

    DATASET_DATA_EXIST_ANNOTATE("DATASET_DATA_EXIST_ANNOTATE", "The selected data has been annotated by others"),

    DATASET_DATA_EXIST_OTHER_TYPE_ANNOTATE("DATASET_DATA_EXIST_OTHER_TYPE_ANNOTATE", "Other types of annotations exist"),

    MODEL_DOES_NOT_EXIST("MODEL_DOES_NOT_EXIST", "Model does not exist"),
    
    POINT_CLOUD_COMPRESSED_FILE_ERROR("POINT_CLOUD_COMPRESSED_FILE_ERROR", "The format of the compressed package is incorrect. It must contain lidar_point_cloud_"),

    IMAGE_COMPRESSED_FILE_ERROR("IMAGE_COMPRESSED_FILE_ERROR", "The format of the compressed package is incorrect. It must contain image_"),

    COMPRESSED_FILE_ERROR("COMPRESSED_FILE_ERROR", "The format of the compressed package is incorrect"),
    COMPRESSED_PACKAGE_EMPTY("COMPRESSED_PACKAGE_EMPTY", "Compressed package is empty"),
    DATASET_CLASS_CLASSIFICATION_EMPTY("DATASET_CLASS_CLASSIFICATION_EMPTY", "You don't have any class or classification for your  annotation,do you want to create them at first?"),

    DATASET_DATA_SCENARIO_NOT_FOUND("DATASET_DATA_SCENARIO_NOT_FOUND", "No data has been found"),

    DATASET_DATA_ALL_EMPTY("DATASET_DATA_ALL_EMPTY", "DatasetId and dataIds cannot be empty at the same time"),

    DATASET__NOT_EXIST_DATA("DATASET__NOT_EXIST_DATA", "dataset have not data"),

    DATASET__MODEL_NOT_EXIST("DATASET__MODEL_NOT_EXIT", "model is not exist"),

    DATASET__MODEL_RUN_RECORD_NOT_EXIST("DATASET__MODEL_RUN_RECORD_NOT_EXIST","run record not exist"),

    DATASET__MODEL_RERUN_ERROR("DATASET__MODEL_RERUN_ERROR","model rerun error"),
    DATASET__NOT_EXIST("DATASET__NOT_EXIST", "dataset not exist"),

    MODEL__MISS_FILED("MODEL__MISS_FILED", "We were unable to locate the %s key(s) in your response JSON. As a result, Xtreme1 cannot process your response successfully. Please verify your API.");




    // Storage

    /**
     * code
     */
    private String code;

    /**
     * description
     */
    private String message;

    UsecaseCode(String code, String message) {
        this.code = code;
        this.message = message;
    }

    public static UsecaseCode getUsecaseCode(String code) {
        for (UsecaseCode usecaseCode : UsecaseCode.values()) {
            if (usecaseCode.getCode() == code) {
                return usecaseCode;
            }
        }
        throw new IllegalArgumentException("Param val mismatch.");
    }


    public String getMessage() {
        return message;
    }

    public String getCode() {
        return code;
    }

}
