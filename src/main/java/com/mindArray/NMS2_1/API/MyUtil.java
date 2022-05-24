package com.mindArray.NMS2_1.API;

import com.mindArray.NMS2_1.Bootstrap;
import com.mindArray.NMS2_1.Constant;
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


public class MyUtil {
private static final Vertx vertx = Bootstrap.getVertex();
  private static final Logger LOGGER = LoggerFactory.getLogger(MyUtil.class);

// private static final Logger LOGGER = LoggerFactory.getLogger(MyUtil.class);
  public static void validate(RoutingContext routingContext,Entity entity){


    try {
      JsonObject jsonObject = routingContext.getBodyAsJson();




      if (routingContext.currentRoute().getName().equals("post")) {
        if(jsonObject == null)
        {
          routingContext.response().setStatusCode(Constant.BAD_REQUEST).end(new JsonObject().put(Constant.STATUS, Constant.ERROR).put(Constant.ERROR, Constant.NO_INPUT).put(Constant.STATUS_CODE,Constant.BAD_REQUEST).toString());
        }
        else
        {
          for(Map.Entry<String ,Object> data : jsonObject){

            if(data.getValue() instanceof String){
              jsonObject.put(data.getKey(),( (String) data.getValue()).trim());

            }
          }
          routingContext.setBody(jsonObject.toBuffer());

          switch (entity){
            case CREDENTIAL:
              if(!jsonObject.containsKey("credential.name") || ! jsonObject.containsKey("protocol") || ! jsonObject.containsKey("password") || jsonObject.getString("credential.name").isEmpty() || jsonObject.getString("protocol").isEmpty())
              {
                  routingContext.response().end("invalid");
              }
              else if(!( jsonObject.getString("protocol").equals("snmp") && jsonObject.containsKey("version") && !jsonObject.getString("version").isEmpty()) || !((!jsonObject.getString("protocol").equals("snmp")) && jsonObject.containsKey("username") && !jsonObject.getString("username").isEmpty()))
              {
                //protocol wrong
                routingContext.response().end("invalid");
              }
              else
              {
                routingContext.next();
              }
            case DISCOVERY:

              if(!jsonObject.containsKey("discovery.name") || ! jsonObject.containsKey("host") || ! jsonObject.containsKey("port") || ! jsonObject.containsKey("metric.type") || ! jsonObject.containsKey("credential.id") )
              { //contains validation
                routingContext.response().end("invalid");
              }
              else if(! (jsonObject.getValue("discovery.name") instanceof String )|| !(jsonObject.getValue("host")instanceof String ) ||! (jsonObject.getValue("port") instanceof Integer) || ! (jsonObject.getValue("metric.type") instanceof String) || ! (jsonObject.getValue("credential.id") instanceof Integer))
              { //datatype validation
                routingContext.response().end("invalid");
              }
              else if(jsonObject.getString("discovery.name").isEmpty() || jsonObject.getString("host").isEmpty() || jsonObject.getString("metric.type").isEmpty())
              { //isEmpty validation
                routingContext.response().end("invalid");
              }
              else
              {
                routingContext.next();
              }
            case MONITOR:
          }


        }

      } else if (routingContext.currentRoute().getName().equals("put")) {
        if(jsonObject == null)
        {
          routingContext.response().setStatusCode(Constant.BAD_REQUEST).end(new JsonObject().put(Constant.STATUS, Constant.ERROR).put(Constant.ERROR, Constant.NO_INPUT).put(Constant.STATUS_CODE,Constant.BAD_REQUEST).toString());

        }
        else{
          for(Map.Entry<String ,Object> data : jsonObject){

            if(data.getValue() instanceof String){
              jsonObject.put(data.getKey(),( (String) data.getValue()).trim());

            }

          }
          routingContext.next();
        }
      } else if (routingContext.currentRoute().getName().equals("delete")) {

        routingContext.next();
      } else {

        routingContext.next();
      }


    }catch (DecodeException exception){

      routingContext.response().setStatusCode(Constant.BAD_REQUEST).end(new JsonObject().put(Constant.STATUS, Constant.ERROR).put(Constant.ERROR, exception.getMessage()).put(Constant.STATUS_CODE,Constant.BAD_REQUEST).toString());

      System.out.println("exception vro" + exception.getMessage());
    }
    }

  public static Future<Void> validateId(String id,Entity table) {
    Promise<Void> promise = Promise.promise();
    JsonObject jsonObject = new JsonObject().put("id",id).put("identity",table);
  // AtomicReference<Boolean> status = new AtomicReference<>(true);
    vertx.eventBus().request(Constant.EVENTBUS_ADDRESS_CHECK_IP,jsonObject,messageAsyncResult -> {
      //return messageAsyncResult.succeeded();

      if( messageAsyncResult.succeeded())
      {
       promise.complete();
      }
      else {
        promise.fail("not available");
      }
    });

    return promise.future();
  }

  public static void init() {

    vertx.eventBus().<JsonArray>request("pickupData",new JsonObject(), messageAsyncResult -> {
      if(messageAsyncResult.succeeded())
      {
        Monitor monitor = new Monitor(messageAsyncResult.result().body());

      }
    });

  }
}
