package com.mindarray.nms.discovery;

import com.mindarray.nms.util.Constant;
import com.mindarray.nms.util.UtilPlugin;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class DiscoveryEngine extends AbstractVerticle {

  private static final Logger LOGGER = LoggerFactory.getLogger(DiscoveryEngine.class);

  @Override
  public void start(Promise<Void> startPromise) {


    vertx.eventBus().<JsonObject>localConsumer(Constant.EVENTBUS_ADDRESS_DISCOVERY, message -> {

      try {

        JsonObject discoveryData = message.body();  //may throw nullPointerException or DecodeException

        String ipAddress = discoveryData.getString(Constant.JSON_KEY_HOST);

        vertx.executeBlocking(pingEvent -> {

          if (UtilPlugin.pingStatus(ipAddress))
          {

            pingEvent.complete(Constant.PING_UP);

          }
          else
          {

            pingEvent.fail(Constant.PING_DOWN);
            vertx.eventBus().send(Constant.INSERT_TO_DATABASE,discoveryData.put("result",Constant.PING_DOWN).put(Constant.IDENTITY,Constant.UPDATE_AFTER_RUN_DISCOVERY).put(Constant.STATUS,Constant.ERROR));


          }
        }, pingResult -> {

          if (pingResult.succeeded()) {

            LOGGER.debug(Constant.PING_UP);

            vertx.executeBlocking(event -> UtilPlugin.pluginEngine(discoveryData.put(Constant.CATEGORY,Constant.DISCOVERY)).onComplete(discoveryEvent->{

              if(discoveryEvent.succeeded() && discoveryEvent.result().getString(Constant.STATUS).equals(Constant.SUCCESS))
              {

                event.complete(discoveryData.mergeIn(discoveryEvent.result()).put(Constant.IDENTITY,Constant.UPDATE_AFTER_RUN_DISCOVERY));

              }
              else
              {

                event.fail(discoveryData.put("result",discoveryEvent.result()).put(Constant.IDENTITY,Constant.UPDATE_AFTER_RUN_DISCOVERY).encodePrettily());
              //  discoveryData.mergeIn(discoveryEvent.result().getJsonObject("data")).put(Constant.IDENTITY,Constant.UPDATE_AFTER_RUN_DISCOVERY);
                vertx.eventBus().send(Constant.INSERT_TO_DATABASE,discoveryData.put("result",discoveryEvent.result()).put(Constant.STATUS,Constant.ERROR).put(Constant.IDENTITY,Constant.UPDATE_AFTER_RUN_DISCOVERY));
              }
            }), discoveryAsyncResult -> {

              if (discoveryAsyncResult.succeeded()) {

                System.out.println("data is " + discoveryAsyncResult.result());
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

                  LOGGER.error(discoveryAsyncResult.cause().getMessage());

                message.fail(696, discoveryAsyncResult.cause().getMessage());
              }
            });


          } else {
            LOGGER.debug(Constant.PING_DOWN);

            message.fail(400, new JsonObject().put(Constant.STATUS, Constant.ERROR).put(Constant.ERROR, pingResult.cause().getMessage()).put(Constant.STATUS_CODE, Constant.BAD_REQUEST).toString());
          }
        });


      }
      catch (Exception exception)
      {
        LOGGER.error(exception.getMessage());

          message.fail(500,new JsonObject().put(Constant.STATUS, Constant.ERROR).put(Constant.ERROR, exception.getMessage()).toString());
      }
    });


    startPromise.complete();


  }

}
