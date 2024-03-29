package com.mindarray.nms.poller;

import com.mindarray.nms.util.Constant;
import com.mindarray.nms.util.UtilMethod;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Base64;
import java.util.concurrent.ConcurrentHashMap;

public class Polling extends AbstractVerticle {

  private static final Logger LOGGER = LoggerFactory.getLogger(Polling.class);

  ConcurrentHashMap<String,Boolean> hashMap = new ConcurrentHashMap<>();

  @Override
  public void start(Promise<Void> startPromise) throws Exception {

    vertx.eventBus().<JsonObject>consumer(Constant.EA_PULLING, message -> {

      vertx.executeBlocking(event->{


        System.out.println("hashMap--" + hashMap);
        if(message.body().getString(Constant.METRIC_GROUP).equals(Constant.PING))
        {

            hashMap.put(message.body().getString("host"),UtilMethod.pingStatus(message.body().getString("host")));

          event.complete();
        }

        else if(!hashMap.get(message.body().getString("host")) )
        {

          LOGGER.error(Constant.PING_DOWN);

          event.fail(Constant.PING_DOWN);

        }
        else {


          UtilMethod.pluginEngine(message.body().put(Constant.CATEGORY, Constant.PULLING)).onComplete(pullingEvent -> {
            if (pullingEvent.succeeded()) {
              vertx.eventBus().request(Constant.INSERT_TO_DATABASE, pullingEvent.result().mergeIn(message.body()).put(Constant.IDENTITY, Constant.DUMP_METRIC_DATA), replyHandler -> {
                if (replyHandler.succeeded()) {
                  LOGGER.info("Pulling Data Dumped in DB ,host: {} ,metric.group:{} ", message.body().getString("host"), message.body().getString("metric.group"));
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
      });


    });
    startPromise.complete();
  }

  private JsonObject pollingFunc(JsonObject jsonObject){

    jsonObject.put("category","pulling");

    String encodedJsonStringARG1 = Base64.getEncoder().encodeToString(jsonObject.toString().getBytes());


    ProcessBuilder processBuilder = new ProcessBuilder().command(Constant.PLUGIN_PATH,encodedJsonStringARG1);

    try {
      Process process = processBuilder.start();

      InputStreamReader inputStreamReader = new InputStreamReader(process.getInputStream()); //read the output

      BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

      String output;
      if ((output = bufferedReader.readLine()) != null) {


        return new JsonObject().put(Constant.STATUS,Constant.SUCCESS).put(Constant.DATA,output);

      }

      process.waitFor();
      bufferedReader.close();
      process.destroy();



    } catch (InterruptedException | IOException e) {
      return new JsonObject().put(Constant.STATUS,Constant.ERROR).put(Constant.ERROR,e.getMessage());
    }


    return new JsonObject().put(Constant.STATUS,Constant.ERROR).put(Constant.ERROR,Constant.PULLING_FAIL);

  }
}
