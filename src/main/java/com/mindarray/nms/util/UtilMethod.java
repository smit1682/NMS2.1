package com.mindarray.nms.util;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonObject;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Base64;

public class UtilMethod {

  public static Future<JsonObject> pluginEngine(JsonObject dataToDiscover) {

    Promise<JsonObject> promise = Promise.promise();

    String encodedJsonString = Base64.getEncoder().encodeToString(dataToDiscover.toString().getBytes());

    ProcessBuilder processBuilder = new ProcessBuilder().command(Constant.PLUGIN_PATH, encodedJsonString);

    try {
      Process process = processBuilder.start();
      InputStreamReader inputStreamReader;
      if(dataToDiscover.getString(Constant.CATEGORY).equals(Constant.DISCOVERY))
      {
         inputStreamReader = new InputStreamReader(process.getErrorStream()); //read the output
      }
      else
      {
         inputStreamReader = new InputStreamReader(process.getInputStream()); //read the output
      }
     // InputStreamReader inputStreamReader = new InputStreamReader(process.getErrorStream()); //read the output

      BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

      String outputString = bufferedReader.readLine();

      String outputJsonString = new String(Base64.getDecoder().decode(outputString.getBytes())); //IllegalArgumentException

      JsonObject discoveryStatus;

      process.waitFor();

      try {

        discoveryStatus = new JsonObject(outputJsonString);   //DecodeException
        System.out.println("status is here vro :     " + discoveryStatus);
        promise.complete( new JsonObject().put("data",discoveryStatus));

      } catch (DecodeException exception) {
        promise.fail(new JsonObject().put(Constant.STATUS, Constant.ERROR).put(Constant.STATUS, outputJsonString).encodePrettily());
      }

    } catch (Exception exception)
    {
      promise.fail( new JsonObject().put(Constant.STATUS, Constant.ERROR).put(Constant.ERROR, exception.getMessage()).encodePrettily());
    }


    return promise.future();


  }

}
