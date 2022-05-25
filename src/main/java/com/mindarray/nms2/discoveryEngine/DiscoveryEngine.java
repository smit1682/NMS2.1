package com.mindarray.nms2.discoveryEngine;

import com.mindarray.nms2.util.Constant;
import io.vertx.core.AbstractVerticle;
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


public class DiscoveryEngine extends AbstractVerticle {

  private static final Logger LOGGER = LoggerFactory.getLogger(DiscoveryEngine.class);

  @Override
  public void start(Promise<Void> startPromise) {


    vertx.eventBus().<JsonObject>consumer(Constant.EVENTBUS_ADDRESS_DISCOVERY, message -> {

      try {
        JsonObject discoveryData = message.body();  //may throw nullPointerException or DecodeException

        System.out.println(discoveryData);

        String ipAddress = discoveryData.getString(Constant.JSON_KEY_HOST);

        vertx.executeBlocking(pingEvent -> {

          if (pingDiscovery(ipAddress)) {
            pingEvent.complete(Constant.PING_UP);
          } else {
            pingEvent.fail(Constant.PING_DOWN);
          }
        }, pingResult -> {

          if (pingResult.succeeded()) {

            //LOGGER.debug("Ping is Up");
            System.out.println("Ping is Up");
            vertx.executeBlocking(event -> {

              JsonObject discoveryStatus = discovery(discoveryData);
              System.out.println("dis =" + discoveryData);
              System.out.println("disS =" + discoveryStatus);
              if (discoveryStatus.getString(Constant.STATUS).equals(Constant.SUCCESS)) {
                event.complete(discoveryData.mergeIn(discoveryStatus).put(Constant.IDENTITY,Constant.UPDATE_AFTER_RUN_DISCOVERY));
              } else {
                event.fail(discoveryStatus.toString());
              }

            }, discoveryAsyncResult -> {

              if (discoveryAsyncResult.succeeded()) {


                vertx.eventBus().request(Constant.INSERT_TO_DATABASE, discoveryAsyncResult.result(), databaseReply -> {

                  if (databaseReply.succeeded()) {

                    JsonObject databaseReplyJson = new JsonObject(databaseReply.result().body().toString());
                    if (databaseReplyJson.getString(Constant.STATUS).equals(Constant.SUCCESS)) {

                      //LOGGER.debug("Discovery happened and stored in database");
                      System.out.println("Discovery happened and stored in database");

                      message.reply(databaseReplyJson.put(Constant.STATUS, Constant.DISCOVERY_AND_DATABASE_SUCCESS));

                    } else {

                      //LOGGER.debug("Discovery happened but did not stored in database");
                      System.out.println("Discovery happened but did not stored in database");
                      message.reply(databaseReplyJson.put(Constant.STATUS, Constant.DISCOVERY_SUCCESS_DATABASE_FAILED));

                    }

                  } else {
                    //LOGGER.debug("Database eventbus replay does not returned");
                    System.out.println("db eventbus not returned");
                  }
                });

              } else {

                message.fail(696, discoveryAsyncResult.cause().getMessage());
              }
            });


          } else {
           // LOGGER.debug("Ping Down");
            System.out.println("ping down");
            message.fail(400, new JsonObject().put(Constant.STATUS, Constant.ERROR).put(Constant.ERROR, pingResult.cause().getMessage()).put(Constant.STATUS_CODE, Constant.BAD_REQUEST).toString());
          }
        });


      }
      catch (Exception exception)
      {
          message.fail(500,new JsonObject().put(Constant.STATUS, Constant.ERROR).put(Constant.ERROR, exception.getMessage()).toString());
      }
    });


    startPromise.complete();


  }

  private JsonObject discovery(JsonObject dataToDiscover) {

    dataToDiscover.put("category","discovery");
    String encodedJsonString = Base64.getEncoder().encodeToString(dataToDiscover.toString().getBytes());

    ProcessBuilder processBuilder = new ProcessBuilder().command(Constant.PLUGIN_PATH, encodedJsonString);

    try {
      Process process = processBuilder.start();

      InputStreamReader inputStreamReader = new InputStreamReader(process.getErrorStream()); //read the output

      BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

      String outputString = bufferedReader.readLine();

      String outputJsonString = new String(Base64.getDecoder().decode(outputString.getBytes())); //IllegalArgumentException

      JsonObject discoveryStatus;

      process.waitFor();

      try {

        discoveryStatus = new JsonObject(outputJsonString);   //DecodeException
        return discoveryStatus;

      } catch (DecodeException exception) {
        return new JsonObject().put(Constant.STATUS, Constant.ERROR).put(Constant.STATUS, outputJsonString);
      }

    } catch (Exception exception)
    {
      return new JsonObject().put(Constant.STATUS, Constant.ERROR).put(Constant.ERROR, exception.getMessage());
    }

  }


  Boolean pingDiscovery(String ip) {
    List<String> listOfCommand = new ArrayList<>();          // list because in future multiple ip can be passed

    listOfCommand.add("fping");
    listOfCommand.add("-q");
    listOfCommand.add("-c");
    listOfCommand.add("3");
    listOfCommand.add("-t");
    listOfCommand.add("3000");
    listOfCommand.add(ip);

    ProcessBuilder processBuilder = new ProcessBuilder(listOfCommand);

    try {

      Process process = processBuilder.start();

      BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(process.getErrorStream()));

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

    return false;
  }


}
