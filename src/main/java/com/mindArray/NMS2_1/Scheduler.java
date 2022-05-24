package com.mindArray.NMS2_1;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


public class Scheduler extends AbstractVerticle {
  private static final Logger LOGGER = LoggerFactory.getLogger(Scheduler.class);

  //private ArrayList<JsonObject> schedulingQueue = new ArrayList<>();

  @Override
  public void start(Promise<Void> startPromise) throws Exception {
     List<JsonObject> schedulingQueue = Collections.synchronizedList(new ArrayList<>());


    vertx.eventBus().<JsonObject>consumer(Constant.EA_SCHEDULING,message -> {
      schedulingQueue.add(message.body());
    });

    vertx.eventBus().<JsonObject>consumer(Constant.UPDATE_SCHEDULING,message->{
      for(var metricGroup : message.body().fieldNames()){

        for(JsonObject data : schedulingQueue ){
          if(metricGroup.equals(data.getString(Constant.JSON_KEY_METRIC_GROUP))){
            data.put(Constant.DEFAULT_TIME,message.body().getInteger(metricGroup));
          }
        }

      }

    });

    vertx.eventBus().<JsonObject>consumer(Constant.DELETE_SCHEDULING,message -> {
      addRemoveingQueue(schedulingQueue,message.body()).onComplete(event->{
        for(JsonObject data : event.result()){
          schedulingQueue.remove(data);
        }
      });
    });





    vertx.setPeriodic(10000, id -> {

      for(JsonObject metricData : schedulingQueue){

        int newValue = metricData.getInteger(Constant.TIME) - 10;
        //System.out.println(newValue);
        if(newValue == 0 ){
          //System.out.println("Going for pulling  monitor.id = " + metricData.getString("monitor.id") + " metric.group= " + metricData.getString("metric.group"));
          vertx.eventBus().request(Constant.EA_PULLING,metricData,replyMessage->{
            if(replyMessage.succeeded()){
              System.out.println("success");
            }else {
              System.out.println("fail");
            }
          });
          metricData.put("time",metricData.getInteger("default.time"));

        }
        else {
          metricData.put("time",newValue);
        }

      }
      System.out.println("******************");
    });

  startPromise.complete();

  }

  public static Future<List<JsonObject>> addRemoveingQueue(List<JsonObject> schedulingQueue , JsonObject obj){
    Promise<List<JsonObject>> promise = Promise.promise();
    List<JsonObject> removeingQueue = Collections.synchronizedList(new ArrayList<>());

    for(JsonObject data : schedulingQueue){
      if(data.getString("monitor.id").equals(obj.getString("id"))){
        removeingQueue.add(data);
      }
    }

    promise.complete(removeingQueue);
    return promise.future();

  }
}
