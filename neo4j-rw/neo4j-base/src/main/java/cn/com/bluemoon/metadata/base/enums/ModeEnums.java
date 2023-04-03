package cn.com.bluemoon.metadata.base.enums;

import lombok.Getter;

/**
 * 
 * @Date 2021/1/6 11:21
 * @Version 1.0
 */
@Getter
public enum ModeEnums {

    /** 比较后，如果有新的，创建一个新的 */
    COMPARE("COMPARE"),
    /** 比较后如果有新的进行合并 */
    MERGE("MERGE"),
    /** 不进行比较，直接创建 */
    CREATE("CREATE");

    private String code;

    private ModeEnums(String code) {
        this.code = code;
    }

}
