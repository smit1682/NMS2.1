package com.mindarray.nms.discovery;

import com.mindarray.nms.util.Constant;
import com.mindarray.nms.util.UtilPlugin;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class DiscoveryEngine extends AbstractVerticle
{
  private static final Logger LOGGER = LoggerFactory.getLogger(DiscoveryEngine.class);

  @Override
  public void start(Promise<Void> startPromise)
  {
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
          }

        }, pingEventResult -> {

          if (pingEventResult.succeeded())
          {
            LOGGER.debug(Constant.PING_UP);

            vertx.executeBlocking(pluginEvent -> UtilPlugin.pluginEngine(discoveryData.put(Constant.CATEGORY,Constant.DISCOVERY)).onComplete(discoveryEventResult->{

              if(discoveryEventResult.succeeded() && discoveryEventResult.result().getString(Constant.STATUS).equals(Constant.SUCCESS))
              {
                pluginEvent.complete(discoveryData.mergeIn(discoveryEventResult.result()).put(Constant.IDENTITY,Constant.UPDATE_AFTER_RUN_DISCOVERY));
              }
              else
              {
                pluginEvent.fail(discoveryEventResult.result().encodePrettily());
              }

            }), pluginEventResult -> {

              if (pluginEventResult.succeeded())
              {
                vertx.eventBus().<JsonObject>request(Constant.DATABASE_HANDLER, pluginEventResult.result(), databaseReply -> {

                  if (databaseReply.succeeded())
                  {
                        if((Constant.SUCCESS).equals(databaseReply.result().body().getString(Constant.STATUS)))
                        {
                            LOGGER.info("Discovery happened and stored in database");

                            message.reply(databaseReply.result().body().put(Constant.STATUS, Constant.DISCOVERY_AND_DATABASE_SUCCESS));
                        }
                        else
                        {
                            LOGGER.debug("Discovery happened but did not stored in database");

                            message.reply(databaseReply.result().body().put(Constant.STATUS, Constant.DISCOVERY_SUCCESS_DATABASE_FAILED));
                        }

                  }
                  else
                  {
                    LOGGER.debug("Database eventbus replay does not returned");

                    message.fail(696, databaseReply.cause().getMessage());
                  }
                });

              }
              else
              {

                  LOGGER.error(pluginEventResult.cause().getMessage());

                  vertx.eventBus().send(Constant.DATABASE_HANDLER,discoveryData.put(Constant.RESULT,pluginEventResult.cause().getMessage()).put(Constant.STATUS,Constant.ERROR).put(Constant.IDENTITY,Constant.UPDATE_AFTER_RUN_DISCOVERY));

                  message.fail(696, pluginEventResult.cause().getMessage());
              }
            });


          }
          else
          {
            LOGGER.debug(Constant.PING_DOWN);

            vertx.eventBus().send(Constant.DATABASE_HANDLER,discoveryData.put(Constant.RESULT,Constant.PING_DOWN).put(Constant.IDENTITY,Constant.UPDATE_AFTER_RUN_DISCOVERY).put(Constant.STATUS,Constant.ERROR));

            message.fail(400, new JsonObject().put(Constant.STATUS, Constant.ERROR).put(Constant.ERROR, pingEventResult.cause().getMessage()).put(Constant.STATUS_CODE, Constant.BAD_REQUEST).toString());
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
