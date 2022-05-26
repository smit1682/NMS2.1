package com.mindarray.nms.discovery;

import com.mindarray.nms.util.Constant;
import com.mindarray.nms.util.UtilMethod;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class DiscoveryEngine extends AbstractVerticle {

  private static final Logger LOGGER = LoggerFactory.getLogger(DiscoveryEngine.class);

  @Override
  public void start(Promise<Void> startPromise) {


    vertx.eventBus().<JsonObject>consumer(Constant.EVENTBUS_ADDRESS_DISCOVERY, message -> {

      try {

        JsonObject discoveryData = message.body();  //may throw nullPointerException or DecodeException

        String ipAddress = discoveryData.getString(Constant.JSON_KEY_HOST);

        vertx.executeBlocking(pingEvent -> {

          if (UtilMethod.pingStatus(ipAddress))
          {

            pingEvent.complete(Constant.PING_UP);

          }
          else
          {

            pingEvent.fail(Constant.PING_DOWN);

          }
        }, pingResult -> {

          if (pingResult.succeeded()) {

            LOGGER.debug(Constant.PING_UP);

            vertx.executeBlocking(event -> UtilMethod.pluginEngine(discoveryData.put(Constant.CATEGORY,Constant.DISCOVERY)).onComplete(discoveryEvent->{

              if(discoveryEvent.succeeded())
              {

                event.complete(discoveryData.mergeIn(discoveryEvent.result().getJsonObject("data")).put(Constant.IDENTITY,Constant.UPDATE_AFTER_RUN_DISCOVERY));

              }
              else
              {

                event.fail(discoveryEvent.cause().getMessage());

              }
            }), discoveryAsyncResult -> {

              if (discoveryAsyncResult.succeeded()) {


                vertx.eventBus().request(Constant.INSERT_TO_DATABASE, discoveryAsyncResult.result(), databaseReply -> {

                  if (databaseReply.succeeded()) {

                    JsonObject databaseReplyJson = new JsonObject(databaseReply.result().body().toString());
                    if (databaseReplyJson.getString(Constant.STATUS).equals(Constant.SUCCESS)) {

                      LOGGER.info("Discovery happened and stored in database");
                      System.out.println("Discovery happened and stored in database");

                      message.reply(databaseReplyJson.put(Constant.STATUS, Constant.DISCOVERY_AND_DATABASE_SUCCESS));

                    } else {

                      LOGGER.debug("Discovery happened but did not stored in database");
                      System.out.println("Discovery happened but did not stored in database");
                      message.reply(databaseReplyJson.put(Constant.STATUS, Constant.DISCOVERY_SUCCESS_DATABASE_FAILED));

                    }

                  } else {
                    LOGGER.debug("Database eventbus replay does not returned");
                    System.out.println("db eventbus not returned");
                  }
                });

              } else {

                message.fail(696, discoveryAsyncResult.cause().getMessage());
              }
            });


          } else {
            LOGGER.debug("Ping Down");
            System.out.println("ping down");
            message.fail(400, new JsonObject().put(Constant.STATUS, Constant.ERROR).put(Constant.ERROR, pingResult.cause().getMessage()).put(Constant.STATUS_CODE, Constant.BAD_REQUEST).toString());
          }
        });


      }
      catch (Exception exception)
      {
          message.fail(500,new JsonObject().put(Constant.STATUS, Constant.ERROR).put(Constant.ERROR, exception.getMessage()).toString());
      }
    });


    startPromise.complete();


  }

}
