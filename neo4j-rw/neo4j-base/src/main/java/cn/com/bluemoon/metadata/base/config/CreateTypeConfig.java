package cn.com.bluemoon.metadata.base.config;

import lombok.Data;

import java.util.List;

/**
 * 
 * @Date 2021/1/6 10:26
 * @Version 1.0
 */
@Data
public class CreateTypeConfig {

    /** 节点名称 */
    private String nodeName;

    /** 标签 */
    private String label;

    /** 是同一个实体的比较字段 */
    private String compareId;

    private List<String> columns;

    private String createType;

    /** 当前同步模式 */
    private String mode;

    /** --------------同步关系配置------------------ */
    /** 源头方向 */
    private String srcDegreeDir = "-";

    /** 目标方向 */
    private String targetDegreeDir = "->";

    /** 源的标签 */
    private String srcLabel;

    /** 目标的标签 */
    private String targetLabel;

    // 同步关系类型
    private String relationType;

    /** 关系的标签 */
    private String relLabel;

    /** 元数据采集的时间 */
    private Long collectTime;

    /** 同步的策略 0 : 保守 1 : 积极 */
    private String incrementMode;

    /** 数据字典NODE的中文名 */
    private String catalogCN;

    /** 环境中文名 */
    private String envCn;

    private Integer nodeTotalCount = 0;

    private Integer relTotalCount = 0;

}
