package com.mindArray.NMS2_1.Repository;

import io.vertx.core.json.JsonObject;

public interface Crudable {
  JsonObject create(JsonObject jsonObject);
  JsonObject read(JsonObject jsonObject);
  JsonObject readAll(JsonObject jsonObject);
  JsonObject update(JsonObject jsonObject);
  JsonObject delete(JsonObject jsonObject);
}
