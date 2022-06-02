package com.mindarray.nms.poller;

import com.mindarray.nms.util.Constant;
import com.mindarray.nms.util.UtilPlugin;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.concurrent.ConcurrentHashMap;

public class Polling extends AbstractVerticle {

  private static final Logger LOGGER = LoggerFactory.getLogger(Polling.class);

  ConcurrentHashMap<String,Boolean> hashMap = new ConcurrentHashMap<>();

  @Override
  public void start(Promise<Void> startPromise) throws Exception {

    vertx.eventBus().<JsonObject>localConsumer(Constant.EA_PULLING, message -> vertx.executeBlocking(event->{


      if(message.body().getString(Constant.METRIC_GROUP).equals(Constant.PING))
      {

          hashMap.put(message.body().getString(Constant.JSON_KEY_HOST), UtilPlugin.pingStatus(message.body().getString(Constant.JSON_KEY_HOST)));

        event.complete();
      }

      else if(!hashMap.get(message.body().getString(Constant.JSON_KEY_HOST)) )
      {

        LOGGER.error(Constant.PING_DOWN);

        event.fail(Constant.PING_DOWN);

      }
      else {


        UtilPlugin.pluginEngine(message.body().put(Constant.CATEGORY, Constant.PULLING)).onComplete(pullingEvent -> {
          if (pullingEvent.succeeded()) {
            vertx.eventBus().request(Constant.INSERT_TO_DATABASE, message.body().put(Constant.DATA,pullingEvent.result()).put(Constant.IDENTITY, Constant.DUMP_METRIC_DATA), replyHandler -> {
              if (replyHandler.succeeded()) {
                LOGGER.info("Pulling Data Dumped in DB ,host: {} ,metric.group:{} ", message.body().getString(Constant.JSON_KEY_HOST), message.body().getString(Constant.METRIC_GROUP));
                event.complete("done pulling");
              }
            });
          } else {
            LOGGER.error("Pulling Fail -> {}", message.body());

            event.fail(Constant.PULLING_FAIL);

          }
        });
      }

    },result->{
      if(result.succeeded())
        message.reply(result.result());
      else
        message.fail(909, result.cause().getMessage());
    }));
    startPromise.complete();
  }

}
