package com.mindarray.nms.api;

import com.mindarray.nms.Bootstrap;
import com.mindarray.nms.util.Constant;
import com.mindarray.nms.util.Entity;
import io.vertx.core.Vertx;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

public class Discovery extends RestAPI
{

  private static final Logger LOGGER = LoggerFactory.getLogger(Discovery.class);

  private static final Set<String> discoveryFields = Set.of("discovery.name", "host", "port", "metric.type", "credential.id", "metric.typeValidation");


  private final Vertx vertx = Bootstrap.getVertex();

  public Discovery(Router router)
  {
    super(router);

    router.post(Constant.PATH_DISCOVERY_WITH_ID).setName("RunDiscovery").handler(this::validateId).handler(this::createApi);
  }

  @Override
  protected Entity getEntity() {
    return Entity.DISCOVERY;
  }

  @Override
  protected void validate(RoutingContext routingContext)
  {
    try
    {
      JsonObject rawData = routingContext.getBodyAsJson();

      if (routingContext.currentRoute().getName().equals("post"))
      {
        if (rawData == null)
        {
          routingContext.response().putHeader(Constant.CONTENT_TYPE,Constant.APPLICATION_JSON).setStatusCode(Constant.BAD_REQUEST).end(new JsonObject().put(Constant.STATUS,Constant.FAIL).put(Constant.ERROR, Constant.NO_INPUT).put(Constant.STATUS_CODE, Constant.BAD_REQUEST).encodePrettily());
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
          routingContext.setBody(rawData.toBuffer());

          {
              if (!rawData.containsKey(Constant.DISCOVERY_NAME) || !rawData.containsKey(Constant.JSON_KEY_HOST) || !rawData.containsKey(Constant.JSON_KEY_PORT) || !rawData.containsKey(Constant.JSON_KEY_METRIC_TYPE) || !rawData.containsKey(Constant.CREDENTIAL_ID))
              { //contains validation
                routingContext.response().putHeader(Constant.CONTENT_TYPE,Constant.APPLICATION_JSON).setStatusCode(Constant.BAD_REQUEST).end(new JsonObject().put(Constant.STATUS,Constant.FAIL).put(Constant.ERROR, Constant.INVALID_INPUT).put(Constant.STATUS_CODE,Constant.BAD_REQUEST).encodePrettily());
              }
              else if (!(rawData.getInteger(Constant.JSON_KEY_PORT) <= 65535) || !(rawData.getInteger(Constant.CREDENTIAL_ID) >= 0))
              { //datatype validation
                routingContext.response().putHeader(Constant.CONTENT_TYPE,Constant.APPLICATION_JSON).setStatusCode(Constant.BAD_REQUEST).end(new JsonObject().put(Constant.STATUS,Constant.FAIL).put(Constant.ERROR, Constant.INVALID_INPUT).put(Constant.STATUS_CODE,Constant.BAD_REQUEST).encodePrettily());
              }
              else if (rawData.getString(Constant.DISCOVERY_NAME).isEmpty() || rawData.getString(Constant.JSON_KEY_HOST).isEmpty() || rawData.getString(Constant.JSON_KEY_METRIC_TYPE).isEmpty())
              { //isEmpty validation
                routingContext.response().putHeader(Constant.CONTENT_TYPE,Constant.APPLICATION_JSON).setStatusCode(Constant.BAD_REQUEST).end(new JsonObject().put(Constant.STATUS,Constant.FAIL).put(Constant.ERROR, Constant.INVALID_INPUT).put(Constant.STATUS_CODE,Constant.BAD_REQUEST).encodePrettily());
              }
              else
              {
                routingContext.next();
              }
          }
        }
      }
      else if (routingContext.currentRoute().getName().equals("put"))
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
            if (!discoveryFields.contains(data.getKey()))
            {
              routingContext.response().putHeader(Constant.CONTENT_TYPE,Constant.APPLICATION_JSON).setStatusCode(Constant.BAD_REQUEST).end(new JsonObject().put(Constant.STATUS,Constant.FAIL).put(Constant.STATUS_CODE,Constant.BAD_REQUEST).put(Constant.ERROR, Constant.REMOVE_EXTRA_FIELD).encodePrettily());

              return;
            }
          }
          {
              if(rawData.isEmpty())
              {
                routingContext.response().putHeader(Constant.CONTENT_TYPE,Constant.APPLICATION_JSON).setStatusCode(Constant.BAD_REQUEST).end(new JsonObject().put(Constant.STATUS,Constant.FAIL).put(Constant.STATUS_CODE,Constant.BAD_REQUEST).put(Constant.ERROR, Constant.NO_INPUT).encodePrettily());
                return;
              }

              if (rawData.containsKey(Constant.DISCOVERY_ID) || rawData.containsKey(Constant.JSON_KEY_METRIC_TYPE))
              {
                routingContext.response().putHeader(Constant.CONTENT_TYPE,Constant.APPLICATION_JSON).setStatusCode(Constant.BAD_REQUEST).end(new JsonObject().put(Constant.STATUS,Constant.FAIL).put(Constant.ERROR, Constant.INVALID_INPUT).put(Constant.STATUS_CODE,Constant.BAD_REQUEST).encodePrettily());

                return;
              }

              if (rawData.containsKey(Constant.DISCOVERY_ID))
              {
                routingContext.response().putHeader(Constant.CONTENT_TYPE,Constant.APPLICATION_JSON).setStatusCode(Constant.BAD_REQUEST).end(new JsonObject().put(Constant.STATUS,Constant.FAIL).put(Constant.ERROR, Constant.INVALID_INPUT).put(Constant.STATUS_CODE,Constant.BAD_REQUEST).encodePrettily());
              }
              else if (rawData.containsKey(Constant.JSON_KEY_PORT) && (rawData.getInteger(Constant.JSON_KEY_PORT) >= 65535))
              {
                routingContext.response().putHeader(Constant.CONTENT_TYPE,Constant.APPLICATION_JSON).setStatusCode(Constant.BAD_REQUEST).end(new JsonObject().put(Constant.STATUS,Constant.FAIL).put(Constant.ERROR, Constant.INVALID_INPUT).put(Constant.STATUS_CODE,Constant.BAD_REQUEST).encodePrettily());
              }
              else if (rawData.containsKey(Constant.CREDENTIAL_ID) && rawData.getInteger(Constant.CREDENTIAL_ID) <= 0)
              {
                routingContext.response().putHeader(Constant.CONTENT_TYPE,Constant.APPLICATION_JSON).setStatusCode(Constant.BAD_REQUEST).end(new JsonObject().put(Constant.STATUS,Constant.FAIL).put(Constant.ERROR, Constant.INVALID_INPUT).put(Constant.STATUS_CODE,Constant.BAD_REQUEST).encodePrettily());
              }
              else
              {
                routingContext.next();
              }

            }
        }
      }
      else if(routingContext.currentRoute().getName().equals("getAll"))
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

      routingContext.response().putHeader(Constant.CONTENT_TYPE,Constant.APPLICATION_JSON).setStatusCode(Constant.BAD_REQUEST).end(new JsonObject().put(Constant.STATUS,Constant.FAIL).put(Constant.ERROR, decodeException.getMessage()).put(Constant.STATUS_CODE, Constant.BAD_REQUEST).encodePrettily());
    }
    catch (ClassCastException classCastException)
    {
      LOGGER.error("Invalid value : {}",classCastException.getMessage());

      routingContext.response().putHeader(Constant.CONTENT_TYPE,Constant.APPLICATION_JSON).setStatusCode(Constant.BAD_REQUEST).end(new JsonObject().put(Constant.STATUS,Constant.FAIL).put(Constant.ERROR, Constant.INVALID_INPUT).put(Constant.STATUS_CODE,Constant.BAD_REQUEST).encodePrettily());
    }
    catch (Exception exception)
    {
      LOGGER.error(exception.getMessage(),exception);

      routingContext.response().putHeader(Constant.CONTENT_TYPE,Constant.APPLICATION_JSON).setStatusCode(Constant.INTERNAL_SERVER_ERROR).end(new JsonObject().put(Constant.STATUS,Constant.FAIL).put(Constant.ERROR, exception.getMessage()).put(Constant.STATUS_CODE, Constant.INTERNAL_SERVER_ERROR).encodePrettily());
    }
  }

  private void createApi(RoutingContext routingContext)
  {
    JsonObject runDiscoveryData = new JsonObject().put(Constant.DISCOVERY_ID, routingContext.pathParam(Constant.ID));

    vertx.eventBus().<JsonObject>request(Constant.DATABASE_HANDLER, runDiscoveryData.put(Constant.IDENTITY, Constant.RUN_DISCOVERY_DATA_COLLECT), replyMessage -> {

      try
      {
        if (replyMessage.succeeded())
        {
          vertx.eventBus().<JsonObject>request(Constant.EVENTBUS_ADDRESS_DISCOVERY, replyMessage.result().body(), messageAsyncResult -> {

            if (messageAsyncResult.succeeded())
            {
              routingContext.response().putHeader(Constant.CONTENT_TYPE,Constant.APPLICATION_JSON).end(messageAsyncResult.result().body().encodePrettily());
            }
            else
            {
              routingContext.response().putHeader(Constant.CONTENT_TYPE,Constant.APPLICATION_JSON).end(messageAsyncResult.cause().getMessage());
            }
          });
        }
        else
        {
          LOGGER.error(replyMessage.cause().getMessage());

          routingContext.response().putHeader(Constant.CONTENT_TYPE,Constant.APPLICATION_JSON).end(replyMessage.cause().getMessage());
        }
      }
      catch (Exception exception)
      {
        LOGGER.error(exception.getMessage(),exception);

        routingContext.response().putHeader(Constant.CONTENT_TYPE,Constant.APPLICATION_JSON).setStatusCode(Constant.INTERNAL_SERVER_ERROR).end(exception.getMessage());
      }
      });
  }

}
