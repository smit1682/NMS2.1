package com.mindArray.NMS2_1.API;

import com.mindArray.NMS2_1.Bootstrap;
import com.mindArray.NMS2_1.Constant;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Monitor extends RestAPI {
  private static final Logger LOGGER = LoggerFactory.getLogger(Monitor.class);

  private final Vertx vertx = Bootstrap.getVertex();
  public Monitor(Router router) {
    super(router);
    router.post(Constant.PATH_PROVISION_WITH_ID).setName("provision").handler(this::validateDiscoveryStatus).handler(this::createMonitor);
  }
  public Monitor(JsonArray jsonArray){   //will trigger at start of the Application
    for(Object data : jsonArray)
    {
      divideAndSchedule((JsonObject) data);
    }
  }

  private void createMonitor(RoutingContext routingContext) {


    vertx.eventBus().<JsonObject>request(Constant.CREATE_MONITOR,routingContext.getBodyAsJson(), messageAsyncResult -> {
      if(messageAsyncResult.succeeded())
      {

        divideAndSchedule(messageAsyncResult.result().body()).onComplete(event->{
          routingContext.response().end(messageAsyncResult.result().body().toString());
        });                      //add future

      }
      else {
        routingContext.response().end(messageAsyncResult.cause().getMessage());
      }
    });


  }

  private void validateDiscoveryStatus(RoutingContext routingContext) {

    JsonObject provisionData = new JsonObject().put(Constant.DISCOVERY_ID,routingContext.pathParam(Constant.ID));

    vertx.eventBus().<JsonObject>request(Constant.PROVISION_VALIDATION,provisionData,replyMessage->{

      if(replyMessage.succeeded()){

        routingContext.setBody(replyMessage.result().body().toBuffer());

        routingContext.next();

      }
      else {

        routingContext.response().end(new JsonObject().put(Constant.STATUS,Constant.ERROR).put(Constant.ERROR,"not discovered yet").toString());
      }

    });
  }

  @Override
  protected Entity getEntity() {
    return Entity.MONITOR;
  }

  public Future<Void> divideAndSchedule(JsonObject jsonObject){          //add future

    Promise<Void> promise = Promise.promise();
    if (Constant.NETWORK_DEVICE.equals(jsonObject.getString(Constant.JSON_KEY_METRIC_TYPE))) {
      jsonObject.put(Constant.TIME, jsonObject.getInteger(Constant.INTERFACE))
        .put(Constant.JSON_KEY_METRIC_GROUP, Constant.INTERFACE)
        .put(Constant.DEFAULT_TIME, jsonObject.getInteger(Constant.INTERFACE));
      callScheduler(jsonObject);
    } else {

      jsonObject.put(Constant.TIME,jsonObject.getInteger(Constant.CPU))
        .put(Constant.JSON_KEY_METRIC_GROUP,Constant.CPU)
        .put(Constant.DEFAULT_TIME,jsonObject.getInteger(Constant.CPU));
      callScheduler(jsonObject);

      jsonObject.put(Constant.TIME,jsonObject.getInteger(Constant.MEMORY))
        .put(Constant.JSON_KEY_METRIC_GROUP,Constant.MEMORY)
        .put(Constant.DEFAULT_TIME,jsonObject.getInteger(Constant.MEMORY));
      callScheduler(jsonObject);

      jsonObject.put(Constant.TIME,jsonObject.getInteger(Constant.DISK))
        .put(Constant.JSON_KEY_METRIC_GROUP,Constant.DISK)
        .put(Constant.DEFAULT_TIME,jsonObject.getInteger(Constant.DISK));
      callScheduler(jsonObject);

      jsonObject.put(Constant.TIME, jsonObject.getInteger(Constant.PROCESS))
        .put(Constant.JSON_KEY_METRIC_GROUP, Constant.PROCESS)
        .put(Constant.DEFAULT_TIME, jsonObject.getInteger(Constant.PROCESS));
      callScheduler(jsonObject);

    }
    jsonObject.put(Constant.TIME,jsonObject.getInteger(Constant.SYSTEM))
      .put(Constant.JSON_KEY_METRIC_GROUP,Constant.SYSTEM)
      .put(Constant.DEFAULT_TIME,jsonObject.getInteger(Constant.SYSTEM));
    callScheduler(jsonObject);
promise.complete();
return promise.future();

  }

  private void callScheduler(JsonObject jsonObject) {           //add future
    System.out.println("callScheduler ---" + jsonObject);
    vertx.eventBus().send(Constant.EA_SCHEDULING,jsonObject);


  }

}

