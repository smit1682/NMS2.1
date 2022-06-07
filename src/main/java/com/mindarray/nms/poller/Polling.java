package com.mindarray.nms.poller;

import com.mindarray.nms.util.Constant;
import com.mindarray.nms.util.UtilPlugin;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.HashMap;

public class Polling extends AbstractVerticle
{
  private static final Logger LOGGER = LoggerFactory.getLogger(Polling.class);

  private final HashMap<String,Boolean> pingAvailability = new HashMap<>();

  @Override

  public void start(Promise<Void> startPromise) throws Exception
  {
    vertx.eventBus().<JsonObject>localConsumer(Constant.EA_POLLING, message -> {
      try
      {
        if (Constant.PING.equals(message.body().getString(Constant.METRIC_GROUP)))
        {
          vertx.executeBlocking(pingEvent -> {

            if (UtilPlugin.pingStatus(message.body().getString(Constant.JSON_KEY_HOST)))
            {
              pingEvent.complete();
            }
            else
            {
              pingEvent.fail(Constant.PING_DOWN);
            }

          }, pingEventResult -> pingAvailability.put(message.body().getString(Constant.JSON_KEY_HOST), pingEventResult.succeeded()));

        }
        else if (pingAvailability.containsKey(message.body().getString(Constant.JSON_KEY_HOST)) && !pingAvailability.get(message.body().getString(Constant.JSON_KEY_HOST)))
        {
          LOGGER.error(Constant.PING_DOWN + " HOST: {}", message.body().getString(Constant.JSON_KEY_HOST));
        }
        else
        {
          vertx.<JsonObject>executeBlocking(pluginEvent -> UtilPlugin.pluginEngine(message.body().put(Constant.CATEGORY, Constant.POLLING)).onComplete(pollingEvent -> {

            if (pollingEvent.succeeded())
            {
              pluginEvent.complete(pollingEvent.result());
            }
            else
            {
              pluginEvent.fail(pollingEvent.cause().getMessage());
            }
          }), pluginEventResult -> {

            if (pluginEventResult.succeeded())
            {
              vertx.eventBus().request(Constant.DATABASE_HANDLER, message.body().put(Constant.DATA, pluginEventResult.result()).put(Constant.IDENTITY, Constant.DUMP_METRIC_DATA), replyHandler -> {

                if (replyHandler.succeeded())
                {
                  LOGGER.info("Polling Data Dumped in DB ,HOST: {} ,metric.group: {} ", message.body().getString(Constant.JSON_KEY_HOST), message.body().getString(Constant.METRIC_GROUP));
                }
                else
                {
                  LOGGER.info("Polling Data Not Dumped in DB ,host: {} ,metric.group: {} ", message.body().getString(Constant.JSON_KEY_HOST), message.body().getString(Constant.METRIC_GROUP));
                }
              });
            }
            else
            {
              LOGGER.error("Polling Failed , HOST: {}   ERROR: {}", message.body().getString(Constant.JSON_KEY_HOST), pluginEventResult.cause().getMessage());
            }
          });
        }
      }
      catch (Exception exception)
      {
        LOGGER.error(exception.getMessage(),exception);
      }
    });
    startPromise.complete();
  }
}
