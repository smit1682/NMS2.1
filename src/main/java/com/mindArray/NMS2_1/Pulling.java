package com.mindArray.NMS2_1;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Base64;

public class Pulling extends AbstractVerticle {

  private static final Logger LOGGER = LoggerFactory.getLogger(Pulling.class);

  @Override
  public void start(Promise<Void> startPromise) throws Exception {

    vertx.eventBus().<JsonObject>consumer("pulling",message -> {
      vertx.executeBlocking(event->{
        JsonObject jsonObject =  message.body();
          String pullingData = pollingFunc(jsonObject);
        if(!pullingData.equals("fail")){
          vertx.eventBus().request("metricDatabase",jsonObject.put("data",pullingData));
          event.complete("done pulling");
        }else
          event.fail("fail");

      },result->{
        if(result.succeeded())
          message.reply(result.result());
        else
          message.fail(909, result.cause().getMessage());
      });


    });
    startPromise.complete();
  }

  private String pollingFunc(JsonObject jsonObject){
jsonObject.put("category","pulling");

    String encodedJsonStringARG1 = Base64.getEncoder().encodeToString(jsonObject.toString().getBytes());


    ProcessBuilder processBuilder = new ProcessBuilder().command(Constant.PLUGIN_PATH,encodedJsonStringARG1);

    try {
      Process process = processBuilder.start();

      InputStreamReader inputStreamReader = new InputStreamReader(process.getInputStream()); //read the output

      BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

      String output;
      while ((output = bufferedReader.readLine()) != null) {
        return output;
        //vertx.eventBus().request("metricDatabase",output,)

      }

      process.waitFor();
      bufferedReader.close();
      process.destroy();



    } catch (InterruptedException | IOException e) {

    }


    return "fail";
    //return true;
  }
}
