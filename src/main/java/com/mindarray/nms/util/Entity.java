package com.mindarray.nms.util;

public enum Entity
{
  DISCOVERY("/discovery","discovery.id"),

  CREDENTIAL("/credential","credential.id"),

  MONITOR("/monitor","monitor.id"),

  METRIC("/metric","monitor.id");

  private final String path;

  private final String id;

  Entity(String path,String id)
  {
    this.path = path;

    this.id =id;
  }

  public String getPath()
  {
    return path;
  }
  public String getId(){return id;}
}
