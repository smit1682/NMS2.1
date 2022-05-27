package com.mindarray.nms.api;

import com.mindarray.nms.Bootstrap;
import com.mindarray.nms.util.Constant;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.HashMap;
import java.util.Map;


public class Util
{
  private static final Vertx vertx = Bootstrap.getVertex();

  private static final Logger LOGGER = LoggerFactory.getLogger(Util.class);

  public static void validate(RoutingContext routingContext,Entity entity)
  {

    try
    {

      JsonObject rawData = routingContext.getBodyAsJson();

      if (routingContext.currentRoute().getName().equals("post"))
      {
        if(rawData == null)
        {
          routingContext.response().setStatusCode(Constant.BAD_REQUEST).end(new JsonObject().put(Constant.STATUS, Constant.ERROR).put(Constant.ERROR, Constant.NO_INPUT).put(Constant.STATUS_CODE,Constant.BAD_REQUEST).toString());
        }
        else
        {
          for(Map.Entry<String ,Object> data : rawData)
          {

            if(data.getValue() instanceof String)
            {
              rawData.put(data.getKey(),( (String) data.getValue()).trim());
            }

          }
          routingContext.setBody(rawData.toBuffer());

          switch (entity)
          {

            case CREDENTIAL:
            {
              if (!rawData.containsKey(Constant.CREDENTIAL_NAME) || !rawData.containsKey(Constant.PROTOCOL) || !rawData.containsKey(Constant.JSON_KEY_PASSWORD)) {   //contains validation
                routingContext.response().end(new JsonObject().put(Constant.STATUS, Constant.ERROR).put(Constant.ERROR, Constant.INVALID_INPUT).encodePrettily());
              } else if (rawData.getString(Constant.CREDENTIAL_NAME).isEmpty() || rawData.getString(Constant.PROTOCOL).isEmpty()) {   //isEmpty validation
                routingContext.response().end(new JsonObject().put(Constant.STATUS, Constant.ERROR).put(Constant.ERROR, Constant.INVALID_INPUT).encodePrettily());
              } else if (rawData.getString(Constant.PROTOCOL).equalsIgnoreCase(Constant.SNMP)) { //protocol validation
                if (rawData.containsKey(Constant.JSON_KEY_VERSION) && !(rawData.getString(Constant.JSON_KEY_VERSION).isEmpty())) {
                  routingContext.next();
                } else {
                  routingContext.response().end(new JsonObject().put(Constant.STATUS, Constant.ERROR).put(Constant.ERROR, Constant.INVALID_INPUT).encodePrettily());

                }
                //protocol wrong
              } else if (!rawData.getString(Constant.PROTOCOL).equals(Constant.SNMP)) {
                if (rawData.containsKey(Constant.JSON_KEY_USERNAME) && !(rawData.getString(Constant.JSON_KEY_USERNAME).isEmpty())) {
                  routingContext.next();
                } else {
                  routingContext.response().end(new JsonObject().put(Constant.STATUS, Constant.ERROR).put(Constant.ERROR, Constant.INVALID_INPUT).encodePrettily());
                }

              } else {
                routingContext.next();
              }
              break;
            }
            case DISCOVERY:
            {
              if (!rawData.containsKey(Constant.DISCOVERY_NAME) || !rawData.containsKey(Constant.JSON_KEY_HOST) || !rawData.containsKey(Constant.JSON_KEY_PORT) || !rawData.containsKey(Constant.JSON_KEY_METRIC_TYPE) || !rawData.containsKey(Constant.CREDENTIAL_ID)) { //contains validation
                routingContext.response().end(new JsonObject().put(Constant.STATUS, Constant.ERROR).put(Constant.ERROR, Constant.INVALID_INPUT).encodePrettily());
              } else if (!(rawData.getInteger(Constant.JSON_KEY_PORT) <= 65535) || !(rawData.getInteger(Constant.CREDENTIAL_ID) >= 0)) { //datatype validation
                routingContext.response().end(new JsonObject().put(Constant.STATUS, Constant.ERROR).put(Constant.ERROR, Constant.INVALID_INPUT).encodePrettily());
              } else if (rawData.getString(Constant.DISCOVERY_NAME).isEmpty() || rawData.getString(Constant.JSON_KEY_HOST).isEmpty() || rawData.getString(Constant.JSON_KEY_METRIC_TYPE).isEmpty()) { //isEmpty validation
                routingContext.response().end(new JsonObject().put(Constant.STATUS, Constant.ERROR).put(Constant.ERROR, Constant.INVALID_INPUT).encodePrettily());
              } else {
                routingContext.next();
              }
              break;

            }

          }


        }

      }
      else if (routingContext.currentRoute().getName().equals("put"))
      {
        if(rawData == null)
        {
          routingContext.response().setStatusCode(Constant.BAD_REQUEST).end(new JsonObject().put(Constant.STATUS, Constant.ERROR).put(Constant.ERROR, Constant.NO_INPUT).put(Constant.STATUS_CODE,Constant.BAD_REQUEST).toString());

        }
        else
        {
          for(Map.Entry<String ,Object> data : rawData)
          {

            if(data.getValue() instanceof String)
            {
              rawData.put(data.getKey(),( (String) data.getValue()).trim());

            }

          }

          switch (entity)
          {
            case CREDENTIAL:
            {
              if (rawData.containsKey("credential.id") || rawData.containsKey("protocol")) {
                routingContext.response().end(new JsonObject().put(Constant.STATUS, Constant.ERROR).put(Constant.ERROR, Constant.INVALID_INPUT).encodePrettily());
              } else {
                routingContext.next();
              }
              break;
            }
            case DISCOVERY:
            {
              if (rawData.containsKey("discovery.id") || rawData.containsKey("credential.id")) {

                routingContext.response().end(new JsonObject().put(Constant.STATUS, Constant.ERROR).put(Constant.ERROR, Constant.INVALID_INPUT).encodePrettily());

              } else if (rawData.containsKey("port") && (rawData.getInteger("port") >= 65535)) {

                routingContext.response().end(new JsonObject().put(Constant.STATUS, Constant.ERROR).put(Constant.ERROR, Constant.INVALID_INPUT).encodePrettily());

              } else {
                routingContext.next();
              }
              break;
            }
            case MONITOR:
            {

              if (isString(rawData) || rawData.containsKey("monitor.id"))
              {
                routingContext.response().end(new JsonObject().put(Constant.STATUS, Constant.ERROR).put(Constant.ERROR, Constant.MUST_BE_INTEGER).encodePrettily());
              }
              else
              {
                routingContext.next();
              }

            }
             // if(!rawData.containsKey("cpu") || !rawData.containsKey("memory") )
          }

        }
      }
      else
      {

        routingContext.next();

      }


    }
    catch (DecodeException exception)
    {

      routingContext.response().setStatusCode(Constant.BAD_REQUEST).end(new JsonObject().put(Constant.STATUS, Constant.ERROR).put(Constant.ERROR, exception.getMessage()).put(Constant.STATUS_CODE,Constant.BAD_REQUEST).toString());

      System.out.println("exception vro: " + exception.getMessage());
    }
    catch (ClassCastException classCastException)
    {

      routingContext.response().end(new JsonObject().put(Constant.STATUS, Constant.ERROR).put(Constant.ERROR, classCastException.getMessage()).put(Constant.STATUS_CODE,Constant.BAD_REQUEST).toString());

    }

    }

  public static Future<Void> validateId(String id,Entity table)
  {
    Promise<Void> promise = Promise.promise();

    JsonObject jsonObject = new JsonObject().put(Constant.ID,id).put(Constant.TABLE_NAME,table).put(Constant.IDENTITY,Constant.VALIDATE_ID);

    vertx.eventBus().request(Constant.INSERT_TO_DATABASE,jsonObject,messageAsyncResult -> {

      if( messageAsyncResult.succeeded())
      {
       promise.complete();
      }
      else
      {
        promise.fail("not available");
      }
    });

    return promise.future();
  }

  public static void init() {

    Promise<Void> promise = Promise.promise();


    vertx.eventBus().<JsonObject>request(Constant.INSERT_TO_DATABASE,new JsonObject().put(Constant.IDENTITY,Constant.CREDENTIAL_READ_ALL),replyHandler -> {

      if(replyHandler.succeeded() && replyHandler.result().body() != null && replyHandler.result().body().containsKey("credential") )
      {

        for( Object data : replyHandler.result().body().getJsonArray("credential"))
        {

          JsonObject cred =  (JsonObject)data;

          vertx.eventBus().send(Constant.STORE_INITIAL_MAP,cred);

        }

        promise.complete();
      }

    });

    promise.future().onComplete(handler->{

      if(handler.succeeded())
      {

        vertx.eventBus().<JsonArray>request(Constant.INSERT_TO_DATABASE,new JsonObject().put(Constant.IDENTITY,Constant.PICK_UP_DATA_INITAL), messageAsyncResult -> {
          if(messageAsyncResult.succeeded())
          {

            new Monitor(messageAsyncResult.result().body());

          }
        });

      }



    });


  }

  private static boolean isString(JsonObject rawData) {
    try {
      for (Map.Entry<String, Object> data : rawData)
      {

        if (data.getValue() instanceof String)
        {
          return true;
        }
        else if ((Integer) data.getValue() % 10 != 0)
        {
          return true;
        }

      }
    }
    catch (ClassCastException exception)
    {
      return true;
    }

      return false;
  }

}
