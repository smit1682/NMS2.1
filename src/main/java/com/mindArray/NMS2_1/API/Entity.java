package com.mindArray.NMS2_1.API;

public enum Entity {
  DISCOVERY("/discovery","discovery.id"),
  CREDENTIAL("/credential","credential.id"),
  MONITOR("/monitor","monitor.id"),
  METRIC_GROUP("metricGroup","metric.group.id");

  private final String path;
  private final String id;
  Entity(String path,String id) {
    this.path = path;
    this.id =id;
  }

  public String getPath() {
    return path;
  }
  public String getId(){return id;}
}
