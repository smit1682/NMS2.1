package com.mindarray.nms.api;

import com.mindarray.nms.Bootstrap;
import com.mindarray.nms.util.Constant;

import com.mindarray.nms.util.Entity;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Monitor extends RestAPI
{

  private static final Logger LOGGER = LoggerFactory.getLogger(Monitor.class);

  private final Vertx vertx = Bootstrap.getVertex();

  public Monitor(Router router)
  {

    super(router);

    router.post(Constant.PATH_PROVISION_WITH_ID).setName("provision").handler(this::validateDiscoveryStatus).handler(this::createMonitor);

  }

  public Monitor(JsonArray monitorArray)   //will trigger at start of the Application
  {

      callScheduler( monitorArray);

  }

  private void createMonitor(RoutingContext routingContext)
  {

    vertx.eventBus().<JsonArray>request(Constant.INSERT_TO_DATABASE,routingContext.getBodyAsJson().put(Constant.IDENTITY,Constant.CREATE_MONITOR), messageAsyncResult -> {

      if(messageAsyncResult.succeeded())
      {
          callScheduler(messageAsyncResult.result().body());

          routingContext.response().end(messageAsyncResult.result().body().encodePrettily());

      }
      else
      {

        LOGGER.error("hello error {}",messageAsyncResult.cause().getMessage());

        routingContext.response().end(messageAsyncResult.cause().getMessage());

      }

    });


  }

  private void validateDiscoveryStatus(RoutingContext routingContext)
  {

    JsonObject provisionData = new JsonObject().put(Constant.DISCOVERY_ID,routingContext.pathParam(Constant.ID)).put(Constant.IDENTITY,Constant.PROVISION_VALIDATION);

    vertx.eventBus().<JsonObject>request(Constant.INSERT_TO_DATABASE,provisionData,replyMessage->{

      if(replyMessage.succeeded() && replyMessage.result().body() != null)
      {

        routingContext.setBody(replyMessage.result().body().toBuffer());

        routingContext.next();

      }
      else
      {

        LOGGER.error(Constant.NOT_DISCOVERED);

        routingContext.response().end(new JsonObject().put(Constant.STATUS,Constant.ERROR).put(Constant.ERROR,Constant.NOT_DISCOVERED).encodePrettily());

      }

    });
  }

  @Override
  protected Entity getEntity() {
    return Entity.MONITOR;
  }


  private void callScheduler(JsonArray array)
  {
    try {


      for (Object data : array) {
        LOGGER.info("Trigger Scheduler -> {} ", data);
        JsonObject metricData = (JsonObject) data;

        metricData.put(Constant.CREDENTIAL_ID, Integer.parseInt(metricData.getString(Constant.CREDENTIAL_ID)));

        if (metricData.getString(Constant.METRIC_TIME) != null) {

          metricData.put(Constant.TIME, Integer.parseInt(metricData.getString(Constant.METRIC_TIME)));

          metricData.put(Constant.DEFAULT_TIME, Integer.parseInt(metricData.getString(Constant.METRIC_TIME)));

        }


        vertx.eventBus().send(Constant.EA_SCHEDULING, metricData);
      }
    }
    catch (Exception exception)
    {
      LOGGER.error(exception.getMessage());

    }

  }

}

