package com.mindarray.nms.store;

import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;

public interface CrudStore
{

  void create(JsonObject jsonObject, Promise<Object> databaseHandler);

  void read(JsonObject jsonObject,Promise<Object> databaseHandler);

  void readAll(JsonObject jsonObject,Promise<Object> databaseHandler);

  void update(JsonObject jsonObject,Promise<Object> databaseHandler);

  void delete(JsonObject jsonObject,Promise<Object> databaseHandler);

}
