package com.mindarray.nms.api;

import com.mindarray.nms.Bootstrap;
import com.mindarray.nms.util.Constant;
import com.mindarray.nms.util.Entity;
import com.mindarray.nms.util.Util;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReportingApi
{
  private final Vertx vertx = Bootstrap.getVertex();

  private static final Logger LOGGER = LoggerFactory.getLogger(ReportingApi.class);

  public ReportingApi(Router router)
  {
    router.get("/report/topFive/:metricGroup").handler(this::getTop);

    router.get("/report/:metricGroup/:id").handler(this::validateID).handler(this::getLastInstance);
  }

  private void validateID(RoutingContext routingContext)
  {
    Util.validateId(routingContext.pathParam("id"), Entity.METRIC).onComplete(event -> {
      if(event.succeeded())
      {
        routingContext.next();
      }
      else
      {
        LOGGER.error(Constant.NOT_VALID);

        routingContext.response().putHeader(Constant.CONTENT_TYPE,Constant.APPLICATION_JSON).setStatusCode(Constant.BAD_REQUEST).end(new JsonObject().put(Constant.STATUS,Constant.FAIL).put(Constant.STATUS_CODE,Constant.BAD_REQUEST).put(Constant.ERROR, Constant.NOT_VALID).encodePrettily());
      }
    });
  }

  private void getLastInstance(RoutingContext routingContext)
  {
    vertx.eventBus().<JsonObject>request(Constant.DATABASE_HANDLER,new JsonObject().put(Constant.IDENTITY,Constant.GET_LAST_INSTANCE).put(Constant.METRIC_GROUP,routingContext.pathParam("metricGroup")).put(Constant.ID,routingContext.pathParam(Constant.ID)), replyMessage->{

      try
      {
        if (replyMessage.succeeded() && replyMessage.result().body() != null)
        {
          routingContext.response().putHeader(Constant.CONTENT_TYPE, Constant.APPLICATION_JSON).end(replyMessage.result().body().encodePrettily());
        }
        else
        {
          routingContext.response().putHeader(Constant.CONTENT_TYPE, Constant.APPLICATION_JSON).setStatusCode(Constant.BAD_REQUEST).end(replyMessage.cause().getMessage());
        }
      }
      catch (Exception exception)
      {
        LOGGER.error(exception.getMessage(),exception);

        routingContext.response().putHeader(Constant.CONTENT_TYPE, Constant.APPLICATION_JSON).setStatusCode(Constant.INTERNAL_SERVER_ERROR).end(exception.getMessage());
      }
    });
  }

  private void getTop(RoutingContext routingContext)
  {
    vertx.eventBus().<JsonObject>request(Constant.DATABASE_HANDLER,new JsonObject().put(Constant.IDENTITY,Constant.TOP_FIVE).put(Constant.METRIC_GROUP,routingContext.pathParam("metricGroup")), replyHandler->{

      try
      {
        if(replyHandler.succeeded() && replyHandler.result().body()!= null)
        {
          routingContext.response().putHeader(Constant.CONTENT_TYPE,Constant.APPLICATION_JSON).end(replyHandler.result().body().encodePrettily());
        }
        else
        {
          routingContext.response().putHeader(Constant.CONTENT_TYPE,Constant.APPLICATION_JSON).setStatusCode(Constant.BAD_REQUEST).end(replyHandler.cause().getMessage());
        }
      }
      catch (Exception exception)
      {
        LOGGER.error(exception.getMessage(),exception);

        routingContext.response().putHeader(Constant.CONTENT_TYPE, Constant.APPLICATION_JSON).setStatusCode(Constant.INTERNAL_SERVER_ERROR).end(exception.getMessage());
      }
    });
  }
}
