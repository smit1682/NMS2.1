package com.mindarray.nms.api;

import com.mindarray.nms.Bootstrap;
import com.mindarray.nms.util.Constant;

import com.mindarray.nms.util.Entity;
import io.vertx.core.Vertx;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;


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
    vertx.eventBus().<JsonArray>request(Constant.DATABASE_HANDLER,routingContext.getBodyAsJson().put(Constant.IDENTITY,Constant.CREATE_MONITOR), messageAsyncResult -> {

      try
      {
        if (messageAsyncResult.succeeded() && messageAsyncResult.result().body() != null)
        {
          callScheduler(messageAsyncResult.result().body());

          routingContext.response().putHeader(Constant.CONTENT_TYPE, Constant.APPLICATION_JSON).end(new JsonObject().put(Constant.STATUS, Constant.SUCCESS).put(Constant.STATUS_CODE, Constant.OK).put(Constant.RESULT, messageAsyncResult.result().body()).encodePrettily());
        }
        else
        {
          LOGGER.error(messageAsyncResult.cause().getMessage());

          routingContext.response().putHeader(Constant.CONTENT_TYPE, Constant.APPLICATION_JSON).end(messageAsyncResult.cause().getMessage());
        }
      }
      catch (Exception exception)
      {
        LOGGER.error(exception.getMessage(),exception);

        routingContext.response().putHeader(Constant.CONTENT_TYPE, Constant.APPLICATION_JSON).setStatusCode(Constant.INTERNAL_SERVER_ERROR).end(new JsonObject().put(Constant.STATUS,Constant.FAIL).put(Constant.STATUS_CODE,Constant.INTERNAL_SERVER_ERROR).encodePrettily());
      }
    });

  }

  private void validateDiscoveryStatus(RoutingContext routingContext)
  {
    JsonObject provisionData = new JsonObject().put(Constant.DISCOVERY_ID,routingContext.pathParam(Constant.ID)).put(Constant.IDENTITY,Constant.PROVISION_VALIDATION);

    vertx.eventBus().<JsonObject>request(Constant.DATABASE_HANDLER,provisionData,replyMessage->{
      try
      {
        if (replyMessage.succeeded() && replyMessage.result().body() != null)
        {
          routingContext.setBody(replyMessage.result().body().toBuffer());

          routingContext.next();
        }
        else
        {
          LOGGER.error(Constant.NOT_DISCOVERED);

          routingContext.response().putHeader(Constant.CONTENT_TYPE, Constant.APPLICATION_JSON).setStatusCode(Constant.BAD_REQUEST).end(new JsonObject().put(Constant.STATUS, Constant.FAIL).put(Constant.ERROR, Constant.NOT_DISCOVERED).put(Constant.STATUS_CODE,Constant.BAD_REQUEST).encodePrettily());
        }
      }
      catch (Exception exception)
      {
        routingContext.response().putHeader(Constant.CONTENT_TYPE, Constant.APPLICATION_JSON).setStatusCode(Constant.INTERNAL_SERVER_ERROR).end(new JsonObject().put(Constant.STATUS,Constant.FAIL).put(Constant.STATUS_CODE,Constant.INTERNAL_SERVER_ERROR).encodePrettily());
      }

    });
  }

  @Override
  protected Entity getEntity()
  {
    return Entity.MONITOR;
  }

  @Override
  protected void validate(RoutingContext routingContext)
  {
    try
    {
      JsonObject rawData = routingContext.getBodyAsJson();

      if (Constant.HTTP_PUT.equals(routingContext.currentRoute().getName()))
      {
        if (rawData == null)
        {
          routingContext.response().setStatusCode(Constant.BAD_REQUEST).end(new JsonObject().put(Constant.STATUS,Constant.FAIL).put(Constant.ERROR, Constant.NO_INPUT).put(Constant.STATUS_CODE, Constant.BAD_REQUEST).encodePrettily());
        }
        else
        {
          for (Map.Entry<String, Object> data : rawData)
          {
            if (data.getValue() instanceof String)
            {
              rawData.put(data.getKey(), ((String) data.getValue()).trim());
            }
          }
          {
              System.out.println("raw data in port" + rawData);

              if(rawData.containsKey(Constant.JSON_KEY_PORT) && rawData.containsKey(Constant.CREDENTIAL_ID))
              {
                if(rawData.getInteger(Constant.JSON_KEY_PORT) <= 65535 && rawData.getInteger(Constant.CREDENTIAL_ID) >= 0)
                {
                  routingContext.next();
                }
                else
                {
                  routingContext.response().putHeader(Constant.CONTENT_TYPE,Constant.APPLICATION_JSON).setStatusCode(Constant.BAD_REQUEST).end(new JsonObject().put(Constant.STATUS,Constant.FAIL).put(Constant.STATUS_CODE,Constant.BAD_REQUEST).put(Constant.ERROR,Constant.DO_NOT_UPDATE).encodePrettily());

                }

              }
              else if (rawData.containsKey(Constant.JSON_KEY_PORT) && rawData.getInteger(Constant.JSON_KEY_PORT) <= 65535 && rawData.size() == 1)
              {
                routingContext.next();
              }
              else if (rawData.containsKey(Constant.CREDENTIAL_ID) && rawData.getInteger(Constant.CREDENTIAL_ID) >= 0 && rawData.size() == 1)
              {
                routingContext.next();
              }
              else
              {
                routingContext.response().putHeader(Constant.CONTENT_TYPE,Constant.APPLICATION_JSON).setStatusCode(Constant.BAD_REQUEST).end(new JsonObject().put(Constant.STATUS,Constant.FAIL).put(Constant.STATUS_CODE,Constant.BAD_REQUEST).put(Constant.ERROR, Constant.DO_NOT_UPDATE).encodePrettily());
              }
          }
        }
      }
      else if(Constant.HTTP_GET_ALL.equals(routingContext.currentRoute().getName()))
      {
        routingContext.next();
      }
      else
      {
        routingContext.response().putHeader(Constant.CONTENT_TYPE,Constant.APPLICATION_JSON).setStatusCode(Constant.NOT_FOUND).end("PAGE NOT FOUND");
      }
    }
    catch (DecodeException decodeException)
    {
      LOGGER.error(decodeException.getMessage());

      routingContext.response().putHeader(Constant.CONTENT_TYPE,Constant.APPLICATION_JSON).setStatusCode(Constant.INTERNAL_SERVER_ERROR).end(new JsonObject().put(Constant.STATUS,Constant.FAIL).put(Constant.ERROR, decodeException.getMessage()).put(Constant.STATUS_CODE, Constant.INTERNAL_SERVER_ERROR).encodePrettily());
    }
    catch (ClassCastException classCastException)
    {
      LOGGER.error("Invalid value : {}",classCastException.getMessage());

      routingContext.response().putHeader(Constant.CONTENT_TYPE,Constant.APPLICATION_JSON).setStatusCode(Constant.INTERNAL_SERVER_ERROR).end(new JsonObject().put(Constant.STATUS,Constant.FAIL).put(Constant.ERROR, Constant.INVALID_INPUT).put(Constant.STATUS_CODE,Constant.BAD_REQUEST).encodePrettily());
    }
    catch (Exception exception)
    {
      LOGGER.error(exception.getMessage(),exception);

      routingContext.response().putHeader(Constant.CONTENT_TYPE,Constant.APPLICATION_JSON).setStatusCode(Constant.INTERNAL_SERVER_ERROR).end(new JsonObject().put(Constant.STATUS,Constant.FAIL).put(Constant.ERROR, exception.getMessage()).put(Constant.STATUS_CODE, Constant.INTERNAL_SERVER_ERROR).encodePrettily());
    }
  }


  private void callScheduler(JsonArray array)
  {
    try
    {
      for(int index = 0; index<array.size();index++)
      {
        LOGGER.info("Trigger Scheduler -> {} ", array.getJsonObject(index));

        if ( array.getJsonObject(index).containsKey(Constant.CREDENTIAL_ID) && array.getJsonObject(index).containsKey(Constant.DEFAULT_TIME))
        {
          vertx.eventBus().send(Constant.EA_SCHEDULING, array.getJsonObject(index));
        }
        else
        {
          LOGGER.error("NULL Metric Group Info {}",array.getJsonObject(index));
        }
      }
    }
    catch (Exception exception)
    {
      LOGGER.error(exception.getMessage(),exception);
    }

  }

}

