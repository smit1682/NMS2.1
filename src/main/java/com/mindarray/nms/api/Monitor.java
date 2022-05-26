package com.mindarray.nms.api;

import com.mindarray.nms.Bootstrap;
import com.mindarray.nms.util.Constant;
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

  public Monitor(JsonArray monitorArray){   //will trigger at start of the Application

    for(Object data : monitorArray)
    {
      divideAndSchedule((JsonObject) data);
    }

  }

  private void createMonitor(RoutingContext routingContext) {


    vertx.eventBus().<JsonObject>request(Constant.INSERT_TO_DATABASE,routingContext.getBodyAsJson().put(Constant.IDENTITY,Constant.CREATE_MONITOR), messageAsyncResult -> {

      if(messageAsyncResult.succeeded())
      {

        divideAndSchedule(messageAsyncResult.result().body()).onComplete(event->{
          routingContext.response().end(messageAsyncResult.result().body().encodePrettily());
        });

      }
      else
      {

        LOGGER.error(messageAsyncResult.cause().getMessage());

        routingContext.response().end(messageAsyncResult.cause().getMessage());

      }

    });


  }

  private void validateDiscoveryStatus(RoutingContext routingContext) {

    JsonObject provisionData = new JsonObject().put(Constant.DISCOVERY_ID,routingContext.pathParam(Constant.ID)).put(Constant.IDENTITY,Constant.PROVISION_VALIDATION);

    vertx.eventBus().<JsonObject>request(Constant.INSERT_TO_DATABASE,provisionData,replyMessage->{

      if(replyMessage.succeeded()){

        routingContext.setBody(replyMessage.result().body().toBuffer());

        routingContext.next();

      }
      else {

        LOGGER.error(Constant.NOT_DISCOVERED);

        routingContext.response().end(new JsonObject().put(Constant.STATUS,Constant.ERROR).put(Constant.ERROR,Constant.NOT_DISCOVERED).encodePrettily());

      }

    });
  }

  @Override
  protected Entity getEntity() {
    return Entity.MONITOR;
  }

  public Future<Void> divideAndSchedule(JsonObject jsonObject){

    Promise<Void> promise = Promise.promise();

    if (Constant.NETWORK_DEVICE.equals(jsonObject.getString(Constant.JSON_KEY_METRIC_TYPE)))
    {

      jsonObject.put(Constant.TIME, jsonObject.getInteger(Constant.INTERFACE))
        .put(Constant.METRIC_GROUP, Constant.INTERFACE)
        .put(Constant.DEFAULT_TIME, jsonObject.getInteger(Constant.INTERFACE));

      callScheduler(jsonObject);

    } else {

      jsonObject.put(Constant.TIME,jsonObject.getInteger(Constant.CPU))
        .put(Constant.METRIC_GROUP,Constant.CPU)
        .put(Constant.DEFAULT_TIME,jsonObject.getInteger(Constant.CPU));

      callScheduler(jsonObject);

      jsonObject.put(Constant.TIME,jsonObject.getInteger(Constant.MEMORY))
        .put(Constant.METRIC_GROUP,Constant.MEMORY)
        .put(Constant.DEFAULT_TIME,jsonObject.getInteger(Constant.MEMORY));

      callScheduler(jsonObject);

      jsonObject.put(Constant.TIME,jsonObject.getInteger(Constant.DISK))
        .put(Constant.METRIC_GROUP,Constant.DISK)
        .put(Constant.DEFAULT_TIME,jsonObject.getInteger(Constant.DISK));

      callScheduler(jsonObject);

      jsonObject.put(Constant.TIME, jsonObject.getInteger(Constant.PROCESS))
        .put(Constant.METRIC_GROUP, Constant.PROCESS)
        .put(Constant.DEFAULT_TIME, jsonObject.getInteger(Constant.PROCESS));

      callScheduler(jsonObject);

    }
    jsonObject.put(Constant.TIME,jsonObject.getInteger(Constant.SYSTEM))
      .put(Constant.METRIC_GROUP,Constant.SYSTEM)
      .put(Constant.DEFAULT_TIME,jsonObject.getInteger(Constant.SYSTEM));

    callScheduler(jsonObject);

    jsonObject.put(Constant.TIME,0)
      .put(Constant.METRIC_GROUP,Constant.PING)
      .put(Constant.DEFAULT_TIME,60);
    callScheduler(jsonObject);

promise.complete();
return promise.future();

  }

  private void callScheduler(JsonObject jsonObject) {

    LOGGER.info("Trigger Scheduler -> {} ",jsonObject);
    vertx.eventBus().send(Constant.EA_SCHEDULING,jsonObject);


  }

}

