package com.mindarray.nms.poller;

import com.mindarray.nms.util.Constant;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class Scheduler extends AbstractVerticle
{
  private static final Logger LOGGER = LoggerFactory.getLogger(Scheduler.class);

  ConcurrentHashMap<Integer,JsonObject> credentialData  = new ConcurrentHashMap<>();
  private static final int SCHEDULING_TIME_PERIOD_SEC = 10;

  @Override
  public void start(Promise<Void> startPromise) throws Exception
  {
    List<JsonObject> schedulingQueue = Collections.synchronizedList(new ArrayList<>());

    vertx.eventBus().<JsonObject>consumer(Constant.EA_SCHEDULING, message -> schedulingQueue.add(message.body()));

    vertx.eventBus().<JsonObject>consumer(Constant.STORE_INITIAL_MAP,message -> credentialData.put(message.body().getInteger(Constant.CREDENTIAL_ID),message.body()));

    vertx.eventBus().<JsonObject>consumer(Constant.UPDATE_SCHEDULING, message -> {

      for (String metricGroup : message.body().fieldNames())
      {
        for (JsonObject metricData : schedulingQueue)
        {
          if (metricGroup.equals(metricData.getString(Constant.METRIC_GROUP)))
          {
            metricData.put(Constant.DEFAULT_TIME, message.body().getInteger(metricGroup));
          }
        }
      }
    });

    vertx.eventBus().<JsonObject>consumer(Constant.DELETE_SCHEDULING, message -> removeFromQueue(schedulingQueue, message.body())
      .onComplete(event -> {
      for (JsonObject metricData : event.result())
      {
        schedulingQueue.remove(metricData);
      }
    }));


    vertx.setPeriodic(SCHEDULING_TIME_PERIOD_SEC * 1000, id -> {

      for (JsonObject metricData : schedulingQueue)
      {

        int waitTime = metricData.getInteger(Constant.TIME) - SCHEDULING_TIME_PERIOD_SEC;

        if (waitTime <= 0)
        {
          //System.out.println("Going for pulling  monitor.id = " + metricData.getString("monitor.id") + " metric.group= " + metricData.getString("metric.group"));

          metricData.mergeIn(credentialData.get(metricData.getInteger("credential.id")));
          /*vertx.eventBus().<JsonObject>request(Constant.INSERT_TO_DATABASE, metricData.put(Constant.IDENTITY, Constant.CREATE_CONTEXT), replyHandler -> {

            if (replyHandler.succeeded())
            {*/
              vertx.eventBus().send(Constant.EA_PULLING, metricData);
            /*}
            else
            {
              LOGGER.error(replyHandler.cause().getMessage());
            }*/
          //});

          metricData.put(Constant.TIME, metricData.getInteger(Constant.DEFAULT_TIME));

        }
        else
        {
          metricData.put(Constant.TIME, waitTime);
        }

      }
      LOGGER.info("********** waiting for 10 seconds **********");
    });

    startPromise.complete();
  }

  public static Future<List<JsonObject>> removeFromQueue(List<JsonObject> schedulingQueue, JsonObject obj)
  {
    Promise<List<JsonObject>> promise = Promise.promise();
    List<JsonObject> removeingQueue = Collections.synchronizedList(new ArrayList<>());

    for (JsonObject data : schedulingQueue) {
      if (data.getString("monitor.id").equals(obj.getString("id"))) {
        removeingQueue.add(data);
      }
    }

    promise.complete(removeingQueue);
    return promise.future();

  }
}
