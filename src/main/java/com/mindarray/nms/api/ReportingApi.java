package com.mindarray.nms.api;

import com.mindarray.nms.Bootstrap;
import com.mindarray.nms.util.Constant;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;

public class ReportingApi
{
  private final Vertx vertx = Bootstrap.getVertex();

  public ReportingApi(Router router)
  {

    router.get("/metric/topFive/:metricGroup").handler(this::getTop);

    router.get("/metric/:metricGroup/:id").handler(this::getLastInstance);

  }

  private void getLastInstance(RoutingContext routingContext)
  {

    vertx.eventBus().<JsonObject>request(Constant.INSERT_TO_DATABASE,new JsonObject().put(Constant.IDENTITY,Constant.GET_LAST_INSTANCE).put(Constant.METRIC_GROUP,routingContext.pathParam("metricGroup")).put(Constant.ID,routingContext.pathParam(Constant.ID)), replyMessage->{

      if(replyMessage.succeeded() && replyMessage.result().body() != null)
      {

        routingContext.response().end(replyMessage.result().body().encodePrettily());

      }
      else
      {

        routingContext.response().end(replyMessage.cause().getMessage());

      }

    });

  }



  private void getTop(RoutingContext routingContext)
  {

    vertx.eventBus().<JsonObject>request(Constant.INSERT_TO_DATABASE,new JsonObject().put(Constant.IDENTITY,Constant.TOP_FIVE).put(Constant.METRIC_GROUP,routingContext.pathParam("metricGroup")), replyHandler->{

      if(replyHandler.succeeded() && replyHandler.result().body()!= null)
      {

        routingContext.response().end(replyHandler.result().body().getJsonArray(Constant.DATA).encodePrettily());

      }
      else
      {

        routingContext.response().end(replyHandler.cause().getMessage());

      }

    });

  }

}
