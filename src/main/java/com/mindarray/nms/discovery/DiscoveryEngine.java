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

      try
      {
        JsonObject discoveryData = message.body();  //may throw nullPointerException or DecodeException

        String ipAddress = discoveryData.getString(Constant.JSON_KEY_HOST);

        vertx.executeBlocking(discoveryEvent -> {

          if (UtilPlugin.pingStatus(ipAddress))
          {
            LOGGER.debug(Constant.PING_UP);

            UtilPlugin.pluginEngine(discoveryData.put(Constant.CATEGORY,Constant.DISCOVERY)).onComplete(pluginEventResult->{

              if(pluginEventResult.succeeded() && Constant.SUCCESS.equals(pluginEventResult.result().getString(Constant.STATUS)))
              {
                discoveryEvent.complete(discoveryData.mergeIn(pluginEventResult.result()).put(Constant.IDENTITY,Constant.UPDATE_AFTER_RUN_DISCOVERY));
              }
              else
              {
                discoveryEvent.fail(pluginEventResult.result().encodePrettily());
              }
            });
          }
          else
          {
            discoveryEvent.fail(Constant.PING_DOWN);
          }

        }, discoveryEventResult -> {

          if (discoveryEventResult.succeeded())
          {

                vertx.eventBus().<JsonObject>request(Constant.DATABASE_HANDLER, discoveryEventResult.result(), databaseReply -> {

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
                LOGGER.error(discoveryEventResult.cause().getMessage());

                vertx.eventBus().send(Constant.DATABASE_HANDLER,discoveryData.put(Constant.RESULT,discoveryEventResult.cause().getMessage()).put(Constant.STATUS,Constant.FAIL).put(Constant.IDENTITY,Constant.UPDATE_AFTER_RUN_DISCOVERY));

                message.fail(400, new JsonObject().put(Constant.STATUS,Constant.FAIL).put(Constant.ERROR, discoveryEventResult.cause().getMessage()).put(Constant.STATUS_CODE, Constant.BAD_REQUEST).toString());
          }
        });


      }
      catch (Exception exception)
      {
        LOGGER.error(exception.getMessage(),exception);

        message.fail(500,new JsonObject().put(Constant.STATUS,Constant.FAIL).put(Constant.ERROR, exception.getMessage()).toString());
      }
    });

    startPromise.complete();
  }
}
