package com.hpe.pamirs.schedule.hpeschedule.zk;

/**
 * 配置信息
 * 
 * @author gaojy
 *
 */
public class ConfigNode {

  private String rootPath;
  private String name;
  private String configType;
  private String value;

  public ConfigNode() {}

  public ConfigNode(String rootPath, String configType, String name) {
    this.rootPath = rootPath;
    this.configType = configType;
    this.name = name;
  }

  public String getRootPath() {
    return rootPath;
  }

  public void setRootPath(String rootPath) {
    this.rootPath = rootPath;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getConfigType() {
    return configType;
  }

  public void setConfigType(String configType) {
    this.configType = configType;
  }

  public String getValue() {
    return value;
  }

  public void setValue(String value) {
    this.value = value;
  }

  public String toString() {
    StringBuffer buffer = new StringBuffer();
    buffer.append("任务名称：").append(this.name).append("\n");
    buffer.append("配置类型：").append(this.configType).append("\n");
    buffer.append("配置跟目录：").append(this.rootPath).append("\n");
    buffer.append("配置的值：").append(this.value).append("\n");

    return buffer.toString();
  }

}
