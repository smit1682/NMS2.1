package com.mindarray.nms.util;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

public class UtilPlugin {
  private static final Logger LOGGER = LoggerFactory.getLogger(UtilPlugin.class);

  public static Future<JsonObject> pluginEngine(JsonObject dataToDiscover)  {

    Promise<JsonObject> promise = Promise.promise();

    String encodedJsonString = Base64.getEncoder().encodeToString(dataToDiscover.toString().getBytes());

    ProcessBuilder processBuilder = new ProcessBuilder().command(Constant.PLUGIN_PATH, encodedJsonString);
    InputStreamReader inputStreamReader = null;
    BufferedReader bufferedReader = null ;
    Process process = null;

    try {
       process = processBuilder.start();

      if(dataToDiscover.getString(Constant.CATEGORY).equals(Constant.DISCOVERY))
      {
         inputStreamReader = new InputStreamReader(process.getErrorStream()); //read the output
      }
      else
      {
         inputStreamReader = new InputStreamReader(process.getInputStream()); //read the output
      }

       bufferedReader = new BufferedReader(inputStreamReader);

      String outputString = bufferedReader.readLine();

      String outputJsonString = new String(Base64.getDecoder().decode(outputString.getBytes())); //IllegalArgumentException

      JsonObject discoveryStatus;

      process.waitFor();

      try {

        discoveryStatus = new JsonObject(outputJsonString);   //DecodeException

        promise.complete( discoveryStatus);

      } catch (DecodeException exception) {
        promise.fail(new JsonObject().put(Constant.STATUS, Constant.ERROR).put(Constant.STATUS, outputJsonString).encodePrettily());
      }

    } catch (Exception exception)
    {
      promise.fail( new JsonObject().put(Constant.STATUS, Constant.ERROR).put(Constant.ERROR, exception.getMessage()).encodePrettily());
    }
    finally
    {
      try
      {
        if(inputStreamReader!=null)
        {
            inputStreamReader.close();
        }
        if(bufferedReader!=null)
        {
          bufferedReader.close();
        }
        if(process != null)
        {
          process.destroy();
        }
      }
      catch (Exception exception)
      {
        LOGGER.error(exception.getMessage());
      }


    }



    return promise.future();


  }

  public static Boolean pingStatus(String host) {
    List<String> listOfCommand = new ArrayList<>();          // list because in future multiple ip can be passed

    listOfCommand.add("fping");
    listOfCommand.add("-q");
    listOfCommand.add("-c");
    listOfCommand.add("3");
    listOfCommand.add("-t");
    listOfCommand.add("3000");
    listOfCommand.add(host);

    ProcessBuilder processBuilder = new ProcessBuilder(listOfCommand);
    Process process = null;
    BufferedReader bufferedReader = null;
    try {

       process = processBuilder.start();

       bufferedReader = new BufferedReader(new InputStreamReader(process.getErrorStream()));

      String output = bufferedReader.readLine();

      String[] parts = output.split(":");

      // vertx.executeBlocking()

      String[] partsOfPats = parts[1].split(" ");

      if (partsOfPats.length == 7) {

        String[] finalParts = partsOfPats[3].split("/");

        return finalParts[0].equals(finalParts[1]);

      } else {
        return false;
      }

    } catch (IOException e) {
      // LOGGER.debug(e.getMessage());
      System.out.println(e.getMessage());
    }
    finally {
      try
      {
        if(bufferedReader!=null)
        {
          bufferedReader.close();
        }
        if(process != null)
        {
          process.destroy();
        }
      }
      catch (Exception exception)
      {
        LOGGER.error(exception.getMessage());
      }
    }

    return false;
  }


}

