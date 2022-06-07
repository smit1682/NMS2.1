package com.mindarray.nms.util;

import com.mindarray.nms.Bootstrap;
import com.mindarray.nms.api.Monitor;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;




public class Util
{
  private static final Vertx vertx = Bootstrap.getVertex();

  private static final Logger LOGGER = LoggerFactory.getLogger(Util.class);

  public static Future<JsonObject> validateId(String id, Entity table)
  {
    Promise<JsonObject> promise = Promise.promise();

    JsonObject entries = new JsonObject().put(Constant.ID, id).put(Constant.TABLE_NAME, table).put(Constant.IDENTITY, Constant.VALIDATE_ID);

    vertx.eventBus().<JsonObject>request(Constant.DATABASE_HANDLER, entries, messageAsyncResult -> {

      if (messageAsyncResult.succeeded() && messageAsyncResult.result().body().containsKey(Constant.METRIC_TYPE_VALIDATION) )
      {
        promise.complete(messageAsyncResult.result().body());
      }
      else if (messageAsyncResult.succeeded() && messageAsyncResult.result().body().containsKey(Constant.PROTOCOL_VALIDATION))
      {
        promise.complete(messageAsyncResult.result().body());
      }
      else if (messageAsyncResult.succeeded())
      {
        promise.complete();
      }
      else
      {
        promise.fail(Constant.NOT_VALID);
      }
    });

    return promise.future();
  }

  public static void init()
  {

    Promise<Void> promise = Promise.promise();

    vertx.eventBus().<JsonObject>request(Constant.DATABASE_HANDLER, new JsonObject().put(Constant.IDENTITY, Constant.CREDENTIAL_READ_ALL), replyHandler -> {

      if (replyHandler.succeeded() && replyHandler.result().body() != null && replyHandler.result().body().containsKey(Constant.RESULT))
      {

        for(int index = 0 ; index < replyHandler.result().body().getJsonArray(Constant.RESULT).size(); index ++)
        {
          vertx.eventBus().send(Constant.STORE_INITIAL_MAP, replyHandler.result().body().getJsonArray(Constant.RESULT).getJsonObject(index));
        }

        promise.complete();
      }
      else
      {
      promise.fail("FAIL");
      }

    });

    promise.future().onComplete(handler -> {

      if (handler.succeeded())
      {
        vertx.eventBus().<JsonArray>request(Constant.DATABASE_HANDLER, new JsonObject().put(Constant.IDENTITY, Constant.PICK_UP_DATA_INITAL), messageAsyncResult -> {

          if (messageAsyncResult.succeeded())
          {
            new Monitor(messageAsyncResult.result().body());
          }
        });
      }
    });
  }
}
