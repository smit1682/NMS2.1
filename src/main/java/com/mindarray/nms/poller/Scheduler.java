package com.mindarray.nms.poller;

import com.mindarray.nms.util.Constant;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
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

    vertx.eventBus().<JsonObject>localConsumer(Constant.EA_SCHEDULING, message -> schedulingQueue.add(message.body()));

    vertx.eventBus().<JsonObject>localConsumer(Constant.STORE_INITIAL_MAP,message -> credentialData.put(message.body().getInteger(Constant.CREDENTIAL_ID),message.body()));

    vertx.eventBus().<JsonObject>localConsumer(Constant.UPDATE_SCHEDULING, message -> {

      System.out.println( "update : " + message.body());

      for(JsonObject metricData : schedulingQueue)
      {
        if(metricData.getString(Constant.MONITOR_ID).equals(message.body().getString(Constant.MONITOR_ID)))
        {
           if(message.body().containsKey(Constant.JSON_KEY_PORT) && message.body().containsKey(Constant.CREDENTIAL_ID))
           {
              metricData.put(Constant.JSON_KEY_PORT,message.body().getInteger(Constant.JSON_KEY_PORT));
              metricData.put(Constant.CREDENTIAL_ID,message.body().getInteger(Constant.CREDENTIAL_ID));
           }
           else if(message.body().containsKey(Constant.CREDENTIAL_ID))
           {
             metricData.put(Constant.CREDENTIAL_ID,message.body().getInteger(Constant.CREDENTIAL_ID));

           }
           else if(message.body().containsKey(Constant.JSON_KEY_PORT))
           {
             metricData.put(Constant.JSON_KEY_PORT,message.body().getInteger(Constant.JSON_KEY_PORT));

           }
           else if(message.body().containsKey(Constant.METRIC_GROUP) && message.body().getString(Constant.METRIC_GROUP).equals(metricData.getString(Constant.METRIC_GROUP)))
           {
              metricData.put(Constant.DEFAULT_TIME,message.body().getInteger(Constant.TIME));
              break;
           }
        }
      }


    });

    vertx.eventBus().<JsonObject>localConsumer(Constant.DELETE_SCHEDULING, message -> removeFromQueue(schedulingQueue, message.body())
      .onComplete(event -> {
      for (JsonObject metricData : event.result())
      {
        schedulingQueue.remove(metricData);
      }
    }));


    vertx.setPeriodic(SCHEDULING_TIME_PERIOD_SEC * 1000, id -> {
      System.out.println("container is :  " + schedulingQueue);
      for (JsonObject metricData : schedulingQueue)
      {

        int waitTime = metricData.getInteger(Constant.TIME) - SCHEDULING_TIME_PERIOD_SEC;

        if (waitTime <= 0)
        {

          metricData.mergeIn(credentialData.get(metricData.getInteger(Constant.CREDENTIAL_ID)));

              vertx.eventBus().send(Constant.EA_PULLING, metricData);


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
      if (data.getString(Constant.MONITOR_ID).equals(obj.getString(Constant.ID))) {
        removeingQueue.add(data);
      }
    }

    promise.complete(removeingQueue);
    return promise.future();

  }
}
