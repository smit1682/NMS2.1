package com.mindarray.nms.util;

import com.mindarray.nms.Bootstrap;
import com.mindarray.nms.api.Monitor;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.Map;
import java.util.Set;


public class Util
{
  /*private static final Set<String> snmpMetricFields = Set.of("interface", "system", "ping");
  private static final Set<String> otherMetricFields = Set.of("cpu", "disk", "process", "memory", "system", "ping");
  private static final Set<String> credentialFields = Set.of("credential.name", "protocol", "username", "password", "community", "version", "protocolValidation");
  private static final Set<String> discoveryFields = Set.of("discovery.name", "host", "port", "metric.type", "credential.id", "metric.typeValidation");
*/

  private static final Vertx vertx = Bootstrap.getVertex();

  private static final Logger LOGGER = LoggerFactory.getLogger(Util.class);

  /*
  public static void validate(RoutingContext routingContext, Entity entity)
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

          switch (entity)
          {
            case CREDENTIAL:
            {
              if (!rawData.containsKey(Constant.CREDENTIAL_NAME) || !rawData.containsKey(Constant.PROTOCOL))
              {   //contains validation
                routingContext.response().putHeader(Constant.CONTENT_TYPE,Constant.APPLICATION_JSON).setStatusCode(Constant.BAD_REQUEST).end(new JsonObject().put(Constant.STATUS,Constant.FAIL).put(Constant.ERROR, Constant.INVALID_INPUT).put(Constant.STATUS_CODE,Constant.BAD_REQUEST).encodePrettily());
              }
              else if (rawData.getString(Constant.CREDENTIAL_NAME).isEmpty() || rawData.getString(Constant.PROTOCOL).isEmpty())
              {   //isEmpty validation
                routingContext.response().putHeader(Constant.CONTENT_TYPE,Constant.APPLICATION_JSON).setStatusCode(Constant.BAD_REQUEST).end(new JsonObject().put(Constant.STATUS,Constant.FAIL).put(Constant.ERROR, Constant.INVALID_INPUT).put(Constant.STATUS_CODE,Constant.BAD_REQUEST).encodePrettily());
              }
              else if (rawData.getString(Constant.PROTOCOL).equalsIgnoreCase(Constant.SNMP))
              { //protocol validation
                if (rawData.containsKey(Constant.JSON_KEY_VERSION) && rawData.containsKey(Constant.COMMUNITY))
                {
                  routingContext.next();
                }
                else
                {
                  routingContext.response().putHeader(Constant.CONTENT_TYPE,Constant.APPLICATION_JSON).setStatusCode(Constant.BAD_REQUEST).end(new JsonObject().put(Constant.STATUS,Constant.FAIL).put(Constant.ERROR, Constant.INVALID_INPUT).put(Constant.STATUS_CODE,Constant.BAD_REQUEST).encodePrettily());
                }

              }
              else if (!rawData.getString(Constant.PROTOCOL).equals(Constant.SNMP))
              {
                if (rawData.containsKey(Constant.JSON_KEY_USERNAME) && !(rawData.getString(Constant.JSON_KEY_USERNAME).isEmpty()) && rawData.containsKey(Constant.JSON_KEY_PASSWORD) && !rawData.getString(Constant.JSON_KEY_PASSWORD).isEmpty())
                {
                  routingContext.next();
                }
                else
                {
                  routingContext.response().putHeader(Constant.CONTENT_TYPE,Constant.APPLICATION_JSON).setStatusCode(Constant.BAD_REQUEST).end(new JsonObject().put(Constant.STATUS,Constant.FAIL).put(Constant.ERROR, Constant.INVALID_INPUT).put(Constant.STATUS_CODE,Constant.BAD_REQUEST).encodePrettily());
                }

              }
              else
              {
                routingContext.next();
              }
              break;
            }
            case DISCOVERY: {




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
              break;

            }
            default:{
              routingContext.response().putHeader(Constant.CONTENT_TYPE,Constant.APPLICATION_JSON).setStatusCode(Constant.NOT_FOUND).end("PAGE NOT FOUND");
            }

          }


        }

      } else if (routingContext.currentRoute().getName().equals("put")) {
        if (rawData == null) {

          routingContext.response().setStatusCode(Constant.BAD_REQUEST).end(new JsonObject().put(Constant.STATUS,Constant.FAIL).put(Constant.ERROR, Constant.NO_INPUT).put(Constant.STATUS_CODE, Constant.BAD_REQUEST).encodePrettily());

        } else {
          for (Map.Entry<String, Object> data : rawData) {

            if (data.getValue() instanceof String) {

              rawData.put(data.getKey(), ((String) data.getValue()).trim());

            }

          }
          switch (entity) {
            case CREDENTIAL: {
              if(rawData.size()==1)
              {
                routingContext.response().putHeader(Constant.CONTENT_TYPE,Constant.APPLICATION_JSON).setStatusCode(Constant.BAD_REQUEST).end(new JsonObject().put(Constant.STATUS,Constant.FAIL).put(Constant.STATUS_CODE,Constant.BAD_REQUEST).put(Constant.ERROR, Constant.NO_INPUT).encodePrettily());
                return;
              }
              System.out.println("raw data: " + rawData);
              for (Map.Entry<String, Object> data : rawData)
              {

                if (data.getValue() instanceof String) {
                  rawData.put(data.getKey(), ((String) data.getValue()).trim());
                }
                if (!credentialFields.contains(data.getKey()))
                {
                  routingContext.response().putHeader(Constant.CONTENT_TYPE,Constant.APPLICATION_JSON).setStatusCode(Constant.BAD_REQUEST).end(new JsonObject().put(Constant.STATUS,Constant.FAIL).put(Constant.STATUS_CODE,Constant.BAD_REQUEST).put(Constant.ERROR, Constant.REMOVE_EXTRA_FIELD).encodePrettily());
                  return;
                }

              }

              if (rawData.containsKey(Constant.CREDENTIAL_ID) || rawData.containsKey(Constant.PROTOCOL))
              {
                routingContext.response().putHeader(Constant.CONTENT_TYPE,Constant.APPLICATION_JSON).setStatusCode(Constant.BAD_REQUEST).end(new JsonObject().put(Constant.STATUS,Constant.FAIL).put(Constant.ERROR, Constant.INVALID_INPUT).put(Constant.STATUS_CODE,Constant.BAD_REQUEST).encodePrettily());
                return;
              }
              if (rawData.getString(Constant.PROTOCOL_VALIDATION).equalsIgnoreCase(Constant.SNMP))
              {
                if (rawData.containsKey(Constant.JSON_KEY_PASSWORD) || rawData.containsKey(Constant.JSON_KEY_USERNAME))
                {
                  routingContext.response().putHeader(Constant.CONTENT_TYPE,Constant.APPLICATION_JSON).setStatusCode(Constant.BAD_REQUEST).end(new JsonObject().put(Constant.STATUS,Constant.FAIL).put(Constant.ERROR, Constant.INVALID_INPUT).put(Constant.STATUS_CODE,Constant.BAD_REQUEST).encodePrettily());

                }
                else
                {
                  rawData.remove(Constant.PROTOCOL_VALIDATION);

                  routingContext.setBody(rawData.toBuffer());

                  routingContext.next();
                }
              }
              else
              {
                if (rawData.containsKey(Constant.JSON_KEY_VERSION) || rawData.containsKey(Constant.COMMUNITY))
                {
                  routingContext.response().putHeader(Constant.CONTENT_TYPE,Constant.APPLICATION_JSON).setStatusCode(Constant.BAD_REQUEST).end(new JsonObject().put(Constant.STATUS,Constant.FAIL).put(Constant.ERROR, Constant.INVALID_INPUT).put(Constant.STATUS_CODE,Constant.BAD_REQUEST).encodePrettily());
                }
                else
                {
                  rawData.remove(Constant.PROTOCOL_VALIDATION);

                  routingContext.setBody(rawData.toBuffer());

                  routingContext.next();
                }
              }

              break;
            }
            case DISCOVERY:
            {
              if(rawData.isEmpty())
              {
                routingContext.response().putHeader(Constant.CONTENT_TYPE,Constant.APPLICATION_JSON).setStatusCode(Constant.BAD_REQUEST).end(new JsonObject().put(Constant.STATUS,Constant.FAIL).put(Constant.STATUS_CODE,Constant.BAD_REQUEST).put(Constant.ERROR, Constant.NO_INPUT).encodePrettily());
                return;
              }
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
              break;
            }
            case METRIC:
            {

              if (rawData.size() != 3 || !(rawData.containsKey(Constant.METRIC_GROUP) && rawData.containsKey(Constant.TIME) && rawData.containsKey(Constant.METRIC_TYPE_VALIDATION)))
              {
                routingContext.response().putHeader(Constant.CONTENT_TYPE,Constant.APPLICATION_JSON).setStatusCode(Constant.BAD_REQUEST).end(new JsonObject().put(Constant.STATUS,Constant.FAIL).put(Constant.ERROR, Constant.INVALID_INPUT).put(Constant.STATUS_CODE,Constant.BAD_REQUEST).encodePrettily());

                return;
              }
              if (rawData.getString(Constant.METRIC_TYPE_VALIDATION).equals(Constant.NETWORK_DEVICE))
              {
                if (snmpMetricFields.contains(rawData.getString(Constant.METRIC_GROUP)) && numberValidate(rawData.getInteger(Constant.TIME)))
                {
                  routingContext.next();
                }
                else
                {
                  routingContext.response().putHeader(Constant.CONTENT_TYPE,Constant.APPLICATION_JSON).setStatusCode(Constant.BAD_REQUEST).end(new JsonObject().put(Constant.STATUS,Constant.FAIL).put(Constant.ERROR, Constant.INVALID_INPUT).put(Constant.STATUS_CODE,Constant.BAD_REQUEST).encodePrettily());
                }
              }
              else
              {
                if (otherMetricFields.contains(rawData.getString(Constant.METRIC_GROUP)) && numberValidate(rawData.getInteger(Constant.TIME)))
                {
                  routingContext.next();
                }
                else
                {
                  routingContext.response().putHeader(Constant.CONTENT_TYPE,Constant.APPLICATION_JSON).setStatusCode(Constant.BAD_REQUEST).end(new JsonObject().put(Constant.STATUS,Constant.FAIL).put(Constant.ERROR, Constant.INVALID_INPUT).put(Constant.STATUS_CODE,Constant.BAD_REQUEST).encodePrettily());
                }
              }
              break;
            }
            case MONITOR:

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
      else
      {
        routingContext.next();
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
  }*/

  public static Future<JsonObject> validateId(String id, Entity table)
  {
    Promise<JsonObject> promise = Promise.promise();

    JsonObject jsonObject = new JsonObject().put(Constant.ID, id).put(Constant.TABLE_NAME, table).put(Constant.IDENTITY, Constant.VALIDATE_ID);

    vertx.eventBus().<JsonObject>request(Constant.DATABASE_HANDLER, jsonObject, messageAsyncResult -> {

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

  /*private static boolean numberValidate(Integer data)
  {
    try
    {
      if (data % 10 != 0)
      {
        return false;
      }
    }
    catch (ClassCastException exception)
    {
      return false;
    }

    return true;
  }*/

}
