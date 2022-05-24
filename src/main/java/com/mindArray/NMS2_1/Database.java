package com.mindArray.NMS2_1;


import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;


public class Database extends AbstractVerticle {

  private static final Logger LOGGER = LoggerFactory.getLogger(Database.class);

  @Override
  public void start(Promise<Void> startPromise) {


    vertx.eventBus().<JsonObject>consumer(Constant.EVENTBUS_ADDRESS_CHECK_IP,
      message -> vertx.executeBlocking(checkIPFuture -> {

       JsonObject checkIpStatus = checkIP(message.body().getString(Constant.ID),message.body().getString(Constant.IDENTITY));

          if ( checkIpStatus.getString(Constant.STATUS).equals(Constant.DISCOVERED))
          {
            checkIPFuture.complete(checkIpStatus);

          } else
          {
            checkIPFuture.fail(checkIpStatus.toString());
          }

        }, checkIpResult -> {

          if (checkIpResult.succeeded())
          {
            message.reply(checkIpResult.result().toString());  //Already discovered, do not run discovery again
          }
          else
          {
            message.fail(600,checkIpResult.cause().getMessage());
          }

        }));


    vertx.eventBus().<JsonObject>consumer(Constant.EVENTBUS_ADDRESS_DISCOVERY_DATABASE, message -> vertx.executeBlocking(insertFuture -> {

        JsonObject queryStatus = updateDisResult( message.body());

        if (queryStatus.getString(Constant.STATUS).equals(Constant.SUCCESS))
        {
          insertFuture.complete(queryStatus);
        }
        else
        {
          insertFuture.fail(queryStatus.toString());
        }

      }, insertResult -> {
        if (insertResult.succeeded())
        {
          message.reply(insertResult.result());
        }
        else
        {
          message.reply(insertResult.cause().getMessage());
        }
      } ));

    vertx.eventBus().<JsonObject>consumer(Constant.PROVISION_VALIDATION,message -> {
      vertx.executeBlocking(statusDiscovery->{
        JsonObject queryStatus = discoveryCheck(message.body());
        System.out.println(queryStatus);
        if (queryStatus.getString("discovery.status").equals("true"))
        {
          statusDiscovery.complete(queryStatus);
        }
        else {
          statusDiscovery.fail(queryStatus.toString());
        }
      },resultStatus->{
        if(resultStatus.succeeded())
        {
          message.reply(resultStatus.result());
        }
        else {
          message.fail(699,resultStatus.cause().getMessage());
        }
      });
    });

    vertx.eventBus().<JsonObject>consumer(Constant.CREATE_MONITOR,message -> {
      vertx.executeBlocking(createMonitor->{
        JsonObject queryStatus = createMonitor(message.body());
        System.out.println("queryy " + queryStatus);
        if (queryStatus.getString(Constant.STATUS).equals(Constant.SUCCESS))
        {
          createMonitor.complete(message.body().mergeIn(queryStatus));
        }
        else {

          createMonitor.fail(queryStatus.toString());
        }
      },resultStatus->{
        if(resultStatus.succeeded())
        {
          message.reply(resultStatus.result());
        }
        else {
          message.fail(999,resultStatus.cause().getMessage());
        }
      });

    });

    vertx.eventBus().<JsonObject>consumer("pickupData",message -> {

      vertx.executeBlocking(statusDiscovery->{
        JsonArray queryStatus = IntialRead(message.body());
        System.out.println(queryStatus);
        if (!queryStatus.isEmpty())
        {
          statusDiscovery.complete(queryStatus);
        }
        else {
          statusDiscovery.fail(queryStatus.toString());
        }
      },resultStatus->{
        if(resultStatus.succeeded())
        {
          message.reply(resultStatus.result());
        }
        else {
          message.fail(699,resultStatus.cause().getMessage());
        }
      });

    });

    vertx.eventBus().<JsonObject>consumer("metricDatabase",message -> {

      vertx.executeBlocking(statusDiscovery->{
        JsonObject queryStatus = dumpInDB(message.body());
        System.out.println(queryStatus);
        if (queryStatus.getString(Constant.STATUS).equals(Constant.SUCCESS))
        {
          statusDiscovery.complete(queryStatus);
        }
        else {
          statusDiscovery.fail(queryStatus.toString());
        }
      },resultStatus->{
        if(resultStatus.succeeded())
        {
          message.reply(resultStatus.result());
        }
        else {
          message.fail(699,resultStatus.cause().getMessage());
        }
      });

    });

    vertx.eventBus().<JsonObject>consumer(Constant.INSERT_TO_DATABASE,message->{

      JsonObject jsonMessage = message.body();

      vertx.executeBlocking(insertFuture->{

        switch (jsonMessage.getString(Constant.IDENTITY))
        {
          case Constant.CREDENTIAL_INSERT:
            jsonMessage.remove(Constant.IDENTITY);
              insertFuture.complete(insertInCred(jsonMessage));

              break;
          case Constant.DISCOVERY_INSERT:
            jsonMessage.remove(Constant.IDENTITY);
            insertFuture.complete(insertInDis(jsonMessage));
              break;
          case Constant.CREDENTIAL_READ_ALL:
            jsonMessage.remove(Constant.IDENTITY);
            insertFuture.complete(realAllCred());

            break;
          case Constant.DISCOVERY_READ_ALL:
            jsonMessage.remove(Constant.IDENTITY);
            insertFuture.complete(realAllDis());
            break;
          case Constant.CREDENTIAL_READ:
            jsonMessage.remove(Constant.IDENTITY);
            insertFuture.complete(readCred(jsonMessage));
            break;
          case Constant.DISCOVERY_READ:
            jsonMessage.remove(Constant.IDENTITY);
            insertFuture.complete(readDis(jsonMessage));
            break;
          case Constant.CREDENTIAL_DELETE:
            jsonMessage.remove(Constant.IDENTITY);
            insertFuture.complete(deleteFromCred(jsonMessage));
            break;
          case Constant.DISCOVERY_DELETE:
            jsonMessage.remove(Constant.IDENTITY);
            insertFuture.complete(deleteFromDis(jsonMessage));
            break;
          case Constant.CREDENTIAL_UPDATE:
            System.out.println(jsonMessage);
            jsonMessage.remove(Constant.IDENTITY);
            insertFuture.complete(updateCred(jsonMessage));
            break;
          case Constant.DISCOVERY_UPDATE:
            jsonMessage.remove(Constant.IDENTITY);
            insertFuture.complete(updateDis(jsonMessage));
            break;
          case Constant.MONITOR_DELETE:

            jsonMessage.remove(Constant.IDENTITY);
            insertFuture.complete(deleteFromMonitor(jsonMessage));
            break;
          case Constant.MONITOR_READ:
            jsonMessage.remove(Constant.IDENTITY);
            insertFuture.complete(readMonitor(jsonMessage));
            break;
          case Constant.MONITOR_READ_ALL:
            jsonMessage.remove(Constant.IDENTITY);
            insertFuture.complete(readAllMonitor());
            break;
          case Constant.MONITOR_UPDATE:
            jsonMessage.remove(Constant.IDENTITY);
            insertFuture.complete(updateMonitor(jsonMessage));
            break;
          case Constant.TOP_FIVE_CPU:
          case Constant.TOP_FIVE_MEMORY:
            insertFuture.complete( topFive(jsonMessage));
            break;
          case Constant.GET_LAST_INSTANCE:
            insertFuture.complete(getLastInstance(jsonMessage));
        }

      },insertResult->{

        if(insertResult.succeeded())
        {
          message.reply(insertResult.result());
        }
        else {
          message.fail(300,insertResult.cause().getMessage());
        }

      });
    });

    vertx.eventBus().<JsonObject>consumer("needData",message -> {
      vertx.executeBlocking(dataFuture->{
        dataFuture.complete(mergeData(message.body()));
      },asyncResult -> {
        if(asyncResult.succeeded())
        {
          message.reply(asyncResult.result());
        }
        else {
          message.fail(300,asyncResult.cause().getMessage());
        }
      });
    });



    startPromise.complete();

  }

  private Object getLastInstance(JsonObject jsonMessage) {
    try(Connection connection = createConnection()){
      PreparedStatement preparedStatement = connection.prepareStatement("select `monitor.id`,`monitor.name`,`metric.group`,`data`,`timestamp` from metric  where `metric.group` =  ?  AND  `monitor.id` = ? order by `metric.id` DESC limit 1;");

      preparedStatement.setString(1,jsonMessage.getString(Constant.JSON_KEY_METRIC_GROUP));
      preparedStatement.setString(2,jsonMessage.getString(Constant.ID));

      ResultSet resultSet = preparedStatement.executeQuery();
      JsonObject lastData = new JsonObject();
      while (resultSet.next()){

        lastData.put(Constant.MONITOR_ID,resultSet.getString("monitor.id"));
        lastData.put(Constant.MONITOR_NAME,resultSet.getString("monitor.name"));
        lastData.put(Constant.DATA,resultSet.getObject("data"));
        lastData.put(Constant.TIME_STAMP,resultSet.getString("timestamp"));

      }
      System.out.println("last data===== " + lastData);
      return lastData;
    }
    catch (Exception e){
      return new JsonObject().put(Constant.STATUS,Constant.ERROR).put(Constant.STATUS_CODE,Constant.INTERNAL_SERVER_ERROR).put(Constant.ERROR,e.getMessage());

    }
  }

  private JsonObject topFive(JsonObject jsonMessage) {

    try (Connection connection = createConnection()){
      String query = "";
      if(jsonMessage.getString(Constant.IDENTITY).equals(Constant.TOP_FIVE_CPU)){
        query = "select  `monitor.name`,max(`data` -> '$.\"cpu.all.user.percentage\"') AS cpuPer from metric group by `monitor.name`  order by cpuPer*1 DESC limit 5";
      }
      else if(jsonMessage.getString(Constant.IDENTITY).equals(Constant.TOP_FIVE_MEMORY)){
        query = "select  `monitor.name`,max(`data` -> '$.\"memory.free.bytes\"') AS `free.memory.bytes`  from metric group by `monitor.name` order by `free.memory.bytes`*1 DESC limit 5 ";
      }

      ResultSet resultSet = connection.createStatement().executeQuery(query);
      JsonArray jsonArray = new JsonArray();
      while (resultSet.next())
      {
        JsonObject data = new JsonObject();

        data.put("monitor.name",resultSet.getString("monitor.name"));
        data.put(resultSet.getMetaData().getColumnName(2),resultSet.getString(resultSet.getMetaData().getColumnName(2)));
        jsonArray.add(data);

      }
      return new JsonObject().put(Constant.STATUS,Constant.SUCCESS).put("data",jsonArray);


    }catch (Exception e){return new JsonObject().put(Constant.STATUS,Constant.ERROR).put(Constant.STATUS_CODE,Constant.INTERNAL_SERVER_ERROR).put(Constant.ERROR,e.getMessage());
    }
  }

  private JsonObject dumpInDB(JsonObject body) {
    try (Connection connection = createConnection()){

      PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO `NMS2.1`.`metric`\n" +
        "(`monitor.id`,\n" +
        "`metric.group`,\n" +
        "`metric.type`,\n" +
        "`data`,\n" +
        "`timestamp`,`monitor.name`) VALUES (?,?,?,?,?,?)");


      preparedStatement.setString(1,body.getString("monitor.id"));
      preparedStatement.setString(2,body.getString("metric.group"));
      preparedStatement.setString(3,body.getString("metric.type"));
      preparedStatement.setString(4,body.getString("data"));
      preparedStatement.setTimestamp(5,new Timestamp(System.currentTimeMillis()));
      preparedStatement.setString(6,body.getString("monitor.name"));

      preparedStatement.executeUpdate();

      return new JsonObject().put(Constant.STATUS,Constant.SUCCESS);


    } catch (SQLException e) {
      return new JsonObject().put(Constant.STATUS,Constant.ERROR).put(Constant.STATUS_CODE,Constant.INTERNAL_SERVER_ERROR).put(Constant.ERROR,e.getMessage());

    }
  }

  private JsonArray IntialRead(JsonObject body) {

    try (Connection connection = createConnection()){
      ResultSet resultSet = connection.createStatement().executeQuery("select * from monitor left join credential on monitor.`credential.id` = credential.`credential.id` ");
      JsonArray jsonArray = new JsonArray();
      while (resultSet.next())
      {
        JsonObject data = new JsonObject();
        data.put("monitor.id",resultSet.getInt("monitor.id"));
        data.put("monitor.name",resultSet.getString("monitor.name"));
        data.put(Constant.JSON_KEY_HOST,resultSet.getString(Constant.JSON_KEY_HOST));
        data.put(Constant.JSON_KEY_PORT,resultSet.getString(Constant.JSON_KEY_PORT));
        data.put(Constant.CREDENTIAL_ID,resultSet.getString(Constant.CREDENTIAL_ID));
        data.put(Constant.JSON_KEY_METRIC_TYPE,resultSet.getString(Constant.JSON_KEY_METRIC_TYPE));
        data.put(Constant.CPU,resultSet.getInt(Constant.CPU));
        data.put(Constant.MEMORY,resultSet.getInt(Constant.MEMORY));
        data.put(Constant.DISK,resultSet.getInt(Constant.DISK));
        data.put(Constant.SYSTEM,resultSet.getInt(Constant.SYSTEM));
        data.put(Constant.PROCESS,resultSet.getInt(Constant.PROCESS));
        data.put(Constant.INTERFACE,resultSet.getInt(Constant.INTERFACE));
        data.put(Constant.JSON_KEY_USERNAME,resultSet.getString(Constant.JSON_KEY_USERNAME));
        data.put(Constant.JSON_KEY_PASSWORD,resultSet.getString(Constant.JSON_KEY_PASSWORD));
        data.put(Constant.JSON_KEY_VERSION,resultSet.getString(Constant.JSON_KEY_VERSION));

        jsonArray.add(data);

      }
      return jsonArray;
    }catch (SQLException exception)
    {


      return new JsonArray();
    }
  }

  private JsonObject updateMonitor(JsonObject jsonObject) {

    try (Connection connection = createConnection()){
      StringBuilder queryInit = new StringBuilder();
      for(var data : jsonObject)
      {
        queryInit.append("`").append(data.getKey()).append("`").append(" = ").append("\"").append(data.getValue()).append("\"").append(",");
      }
      queryInit.deleteCharAt(queryInit.length()-1);
      String query = "UPDATE monitor SET " + queryInit + " WHERE `monitor.id` = " + jsonObject.getString("monitor.id");
      System.out.println(query);
      int affectedRows = connection.createStatement().executeUpdate(query);

      System.out.println("affected rows  "+ affectedRows);

      if(affectedRows==0)throw new SQLException("id not exist");
    }catch (SQLException exception){
      return new JsonObject().put(Constant.STATUS,Constant.ERROR).put(Constant.STATUS_CODE,Constant.INTERNAL_SERVER_ERROR).put(Constant.ERROR,exception.getMessage());

    }

    // System.out.println(query);
    return new JsonObject().put(Constant.STATUS,Constant.SUCCESS).put(Constant.STATUS_CODE,Constant.OK);

  }

  private JsonObject readAllMonitor( ) {
    try (Connection connection = createConnection()){
      ResultSet resultSet = connection.createStatement().executeQuery("select * from monitor");
      JsonArray jsonArray = new JsonArray();
      while (resultSet.next())
      {
        JsonObject data = new JsonObject();
        data.put("monitor.id",resultSet.getInt("monitor.id"));
        data.put("monitor.name",resultSet.getString("monitor.name"));
        data.put(Constant.JSON_KEY_HOST,resultSet.getString(Constant.JSON_KEY_HOST));
        data.put(Constant.JSON_KEY_PORT,resultSet.getString(Constant.JSON_KEY_PORT));
        data.put(Constant.CREDENTIAL_ID,resultSet.getString(Constant.CREDENTIAL_ID));
        data.put(Constant.JSON_KEY_METRIC_TYPE,resultSet.getString(Constant.JSON_KEY_METRIC_TYPE));
        data.put(Constant.CPU,resultSet.getString(Constant.CPU));
        data.put(Constant.MEMORY,resultSet.getString(Constant.MEMORY));
        data.put(Constant.DISK,resultSet.getString(Constant.DISK));
        data.put(Constant.SYSTEM,resultSet.getString(Constant.SYSTEM));
        data.put(Constant.PROCESS,resultSet.getString(Constant.PROCESS));
        data.put(Constant.INTERFACE,resultSet.getString(Constant.INTERFACE));
        jsonArray.add(data);

      }
      return new JsonObject().put("monitor",jsonArray);
    }catch (SQLException exception)
    {


      return new JsonObject().put(Constant.STATUS,Constant.ERROR).put(Constant.STATUS_CODE,Constant.INTERNAL_SERVER_ERROR).put(Constant.ERROR,exception.getMessage());

    }
  }

  private Object readMonitor(JsonObject jsonMessage) {
    try (Connection connection = createConnection()){
      ResultSet resultSet = connection.createStatement().executeQuery("select * from monitor where `monitor.id` = " + jsonMessage.getString("id"));
      JsonObject data = new JsonObject();
      while (resultSet.next())
      {

        data.put("monitor.id",resultSet.getInt("monitor.id"));
        data.put("monitor.name",resultSet.getString("monitor.name"));
        data.put(Constant.JSON_KEY_HOST,resultSet.getString(Constant.JSON_KEY_HOST));
        data.put(Constant.JSON_KEY_PORT,resultSet.getString(Constant.JSON_KEY_PORT));
        data.put(Constant.CREDENTIAL_ID,resultSet.getString(Constant.CREDENTIAL_ID));
        data.put(Constant.JSON_KEY_METRIC_TYPE,resultSet.getString(Constant.JSON_KEY_METRIC_TYPE));
        data.put(Constant.CPU,resultSet.getString(Constant.CPU));
        data.put(Constant.MEMORY,resultSet.getString(Constant.MEMORY));
        data.put(Constant.DISK,resultSet.getString(Constant.DISK));
        data.put(Constant.SYSTEM,resultSet.getString(Constant.SYSTEM));
        data.put(Constant.PROCESS,resultSet.getString(Constant.PROCESS));
        data.put(Constant.INTERFACE,resultSet.getString(Constant.INTERFACE));

      }
      return data;
    }catch (SQLException exception)
    {

      return new JsonObject().put(Constant.STATUS,Constant.ERROR).put(Constant.STATUS_CODE,Constant.INTERNAL_SERVER_ERROR).put(Constant.ERROR,exception.getMessage());

    }

  }

  private Object deleteFromMonitor(JsonObject jsonMessage) {

    try (Connection connection = createConnection()){
      int master = connection.createStatement().executeUpdate("delete from monitor where `monitor.id` = "+jsonMessage.getString("id"));

      return new JsonObject().put(Constant.STATUS,Constant.SUCCESS).put(Constant.STATUS_CODE,Constant.OK);

    }catch (SQLException exception){
      return new JsonObject().put(Constant.STATUS,Constant.ERROR).put(Constant.STATUS_CODE,Constant.INTERNAL_SERVER_ERROR).put(Constant.ERROR,exception.getMessage());

    }
  }

  private JsonObject createMonitor(JsonObject body) {


    try (Connection connection = createConnection()){

      PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO `NMS2.1`.`monitor` (`monitor.name`,`host`,`port`,`credential.id`,`metric.type`) VALUES (?,?,?,?,?)");

      System.out.println(body.getString(Constant.CREDENTIAL_NAME));
     // System.out.println("hello misfortune ..." + jsonObject.getString("smit"));
      preparedStatement.setString(1,body.getString("monitor.name"));
      preparedStatement.setString(2,body.getString("host"));
      preparedStatement.setString(3,body.getString("port"));
      preparedStatement.setInt(4,body.getInteger("credential.id"));
      preparedStatement.setString(5,body.getString("metric.type"));

      preparedStatement.executeUpdate();


      ResultSet resultSet = connection.createStatement().executeQuery("SELECT MAX(`monitor.id`) FROM monitor;");
      int id=0;
      while (resultSet.next()){
        id = resultSet .getInt(1);
      }

      System.out.println(id);
      JsonObject jsonObject = new JsonObject();
      resultSet = connection.createStatement().executeQuery("select `cpu`, `memory`, `disk`, `system`, `process`, `interface` from monitor where `monitor.id` = " + id);
      while (resultSet.next()){
        jsonObject.put("cpu",resultSet.getInt("cpu"))
          .put("memory",resultSet.getInt("memory"))
          .put("disk",resultSet.getInt("disk"))
          .put("system",resultSet.getInt("system"))
          .put("process",resultSet.getInt("process"))
          .put("interface",resultSet.getInt("interface"));
      }
      return jsonObject.put(Constant.STATUS,Constant.SUCCESS).put("monitor.id",id);


    } catch (SQLException e) {
      return new JsonObject().put(Constant.STATUS,Constant.ERROR).put(Constant.STATUS_CODE,Constant.INTERNAL_SERVER_ERROR).put(Constant.ERROR,e.getMessage());

    }

  }

  private JsonObject discoveryCheck(JsonObject body) {
    System.out.println("body" + body);
    try (Connection connection = createConnection()){
      ResultSet resultSet = connection.createStatement().executeQuery("select `discovery.status`,`monitor.name` from discovery where `discovery.id` = "+ body.getString("discovery.id"));
      while (resultSet.next()){
        body.put("discovery.status",resultSet.getString("discovery.status"))
          .put("monitor.name",resultSet.getString("monitor.name"));

      }
      body.mergeIn(mergeData(body));
      System.out.println("after mergeIn= " + body);
      return body;
    }catch (SQLException exception){
      return new JsonObject().put(Constant.STATUS,Constant.ERROR).put(Constant.STATUS_CODE,Constant.INTERNAL_SERVER_ERROR).put(Constant.ERROR,exception.getMessage());

    }
  }

  private JsonObject updateDisResult(JsonObject body) {
    System.out.println(body);
    try (Connection connection = createConnection()){
     PreparedStatement preparedStatement = connection.prepareStatement("UPDATE discovery SET `discovery.status` = ?, `discovery.result` = ?,`monitor.name` = ? where `discovery.id` = ?");
      preparedStatement.setString(1,"true");
      preparedStatement.setString(2,body.toString());
      preparedStatement.setString(3,body.getString("monitor.name"));
      preparedStatement.setString(4,body.getString("discovery.id"));

      preparedStatement.executeUpdate();

    }catch (SQLException exception){
      return new JsonObject().put(Constant.STATUS,Constant.ERROR).put(Constant.STATUS_CODE,Constant.INTERNAL_SERVER_ERROR).put(Constant.ERROR,exception.getMessage());
    }


    return new JsonObject().put(Constant.STATUS,Constant.SUCCESS).put(Constant.STATUS_CODE,Constant.OK);
  }

  private JsonObject mergeData(JsonObject jsonObject) {
    try (Connection connection = createConnection()){
      ResultSet resultSet = connection.createStatement().executeQuery("select * from discovery left join credential on discovery.`credential.id` = credential.`credential.id` where discovery.`discovery.id` = " + jsonObject.getString("discovery.id"));

      while (resultSet.next())
      {

        jsonObject.put("credential.id",resultSet.getInt("credential.id"));
        jsonObject.put("credential.name",resultSet.getString("credential.name"));
        jsonObject.put("protocol",resultSet.getString(Constant.PROTOCOL));
        jsonObject.put(Constant.JSON_KEY_USERNAME,resultSet.getString(Constant.JSON_KEY_USERNAME));
        jsonObject.put(Constant.JSON_KEY_PASSWORD,resultSet.getString(Constant.JSON_KEY_PASSWORD));
        jsonObject.put(Constant.JSON_KEY_VERSION,resultSet.getString(Constant.JSON_KEY_VERSION));

        jsonObject.put(Constant.JSON_KEY_HOST,resultSet.getString(Constant.JSON_KEY_HOST));
        jsonObject.put(Constant.JSON_KEY_PORT,resultSet.getString(Constant.JSON_KEY_PORT));
        jsonObject.put("metric.type",resultSet.getString("metric.type"));



      }
      return jsonObject;
    }catch (SQLException exception)
    {


      return new JsonObject().put(Constant.STATUS,Constant.ERROR).put(Constant.STATUS_CODE,Constant.INTERNAL_SERVER_ERROR).put(Constant.ERROR,exception.getMessage());

    }
  }

  private JsonObject deleteFromCred(JsonObject string) {

    try (Connection connection = createConnection()){
      int master = connection.createStatement().executeUpdate("delete from credential where `credential.id` = "+string.getString("id"));
      System.out.println("delete from credential where credential.id = "+string);
      return new JsonObject().put(Constant.STATUS,Constant.SUCCESS).put(Constant.STATUS_CODE,Constant.OK);

    }catch (SQLException exception){
      return new JsonObject().put(Constant.STATUS,Constant.ERROR).put(Constant.STATUS_CODE,Constant.INTERNAL_SERVER_ERROR).put(Constant.ERROR,exception.getMessage());

    }

  }

  private JsonObject deleteFromDis(JsonObject string) {

    try (Connection connection = createConnection()){
      int master = connection.createStatement().executeUpdate("delete from discovery where `discovery.id` = "+string.getString("id"));
      System.out.println("delete from discovery where id = "+string);
      return new JsonObject().put(Constant.STATUS,Constant.SUCCESS).put(Constant.STATUS_CODE,Constant.OK);

    }catch (SQLException exception){
      return new JsonObject().put(Constant.STATUS,Constant.ERROR).put(Constant.STATUS_CODE,Constant.INTERNAL_SERVER_ERROR).put(Constant.ERROR,exception.getMessage());

    }

  }

  private JsonObject realAllCred()   {


    try (Connection connection = createConnection()){
      ResultSet resultSet = connection.createStatement().executeQuery("select * from credential");
      JsonArray jsonArray = new JsonArray();
      while (resultSet.next())
      {
        JsonObject data = new JsonObject();
        data.put("credential.id",resultSet.getInt("credential.id"));
        data.put("credential.name",resultSet.getString("credential.name"));
        data.put("protocol",resultSet.getString(Constant.PROTOCOL));
        data.put(Constant.JSON_KEY_USERNAME,resultSet.getString(Constant.JSON_KEY_USERNAME));
        data.put(Constant.JSON_KEY_PASSWORD,resultSet.getString(Constant.JSON_KEY_PASSWORD));
        data.put(Constant.JSON_KEY_VERSION,resultSet.getString(Constant.JSON_KEY_VERSION));

        jsonArray.add(data);

      }
      return new JsonObject().put("credential",jsonArray);
    }catch (SQLException exception)
    {


      return new JsonObject().put(Constant.STATUS,Constant.ERROR).put(Constant.STATUS_CODE,Constant.INTERNAL_SERVER_ERROR).put(Constant.ERROR,exception.getMessage());

    }




    }

 private JsonArray realAllDis()   {



    try (Connection connection = createConnection()){
      ResultSet resultSet = connection.createStatement().executeQuery("select * from discovery");
      JsonArray jsonArray = new JsonArray();
      while (resultSet.next())
      {
        JsonObject data = new JsonObject();
        data.put("discovery.id",resultSet.getInt("discovery.id"));
        data.put("discovery.name",resultSet.getString("discovery.name"));
        data.put("host",resultSet.getString("host"));
        data.put("credential.id",resultSet.getInt("credential.id"));
        data.put(Constant.JSON_KEY_PORT,resultSet.getString("port"));
        data.put("metric.type",resultSet.getString("metric.type"));

        jsonArray.add(data);

      }
      return jsonArray;
    }catch (SQLException exception)
    {

      JsonArray jsonArray = new JsonArray();
      return jsonArray.add(new JsonObject().put(Constant.STATUS,Constant.ERROR).put(Constant.STATUS_CODE,Constant.INTERNAL_SERVER_ERROR).put(Constant.ERROR,exception.getMessage()));

    }




    }

  private JsonObject readCred(JsonObject jsonObject)   {


    try (Connection connection = createConnection()){
      ResultSet resultSet = connection.createStatement().executeQuery("select * from credential where `credential.id` = " + jsonObject.getString("id"));
      JsonObject data = new JsonObject();
      while (resultSet.next())
      {

        data.put("credential.id",resultSet.getInt("credential.id"));
        data.put("credential.name",resultSet.getString("credential.name"));
        data.put("protocol",resultSet.getString(Constant.PROTOCOL));
        data.put(Constant.JSON_KEY_USERNAME,resultSet.getString(Constant.JSON_KEY_USERNAME));
        data.put(Constant.JSON_KEY_PASSWORD,resultSet.getString(Constant.JSON_KEY_PASSWORD));
        data.put(Constant.JSON_KEY_VERSION,resultSet.getString(Constant.JSON_KEY_VERSION));

      }
      return data;
    }catch (SQLException exception)
    {

      return new JsonObject().put(Constant.STATUS,Constant.ERROR).put(Constant.STATUS_CODE,Constant.INTERNAL_SERVER_ERROR).put(Constant.ERROR,exception.getMessage());

    }

  }

  private JsonObject readDis(JsonObject jsonObject)   {



    try (Connection connection = createConnection()){
      ResultSet resultSet = connection.createStatement().executeQuery("select * from discovery where `discovery.id` = "+ jsonObject.getString("id"));
      JsonObject data = new JsonObject();
      while (resultSet.next())
      {

        data.put("discovery.id",resultSet.getInt("discovery.id"));
        data.put("discovery.name",resultSet.getString("discovery.name"));
        data.put("host",resultSet.getString("host"));
        data.put("credential.id",resultSet.getInt("credential.id"));
        data.put(Constant.JSON_KEY_PORT,resultSet.getString("port"));
        data.put("metric.type",resultSet.getString("metric.type"));


      }
      return data;
    }catch (SQLException exception)
    {


      return new JsonObject().put(Constant.STATUS,Constant.ERROR).put(Constant.STATUS_CODE,Constant.INTERNAL_SERVER_ERROR).put(Constant.ERROR,exception.getMessage());

    }




  }

  private JsonObject updateCred(JsonObject jsonObject){


    try (Connection connection = createConnection()){
      StringBuilder queryInit = new StringBuilder();
      for(var data : jsonObject)
      {
        queryInit.append("`").append(data.getKey()).append("`").append(" = ").append("\"").append(data.getValue()).append("\"").append(",");
      }
      queryInit.deleteCharAt(queryInit.length()-1);
      String query = "UPDATE credential SET " + queryInit + " WHERE `credential.id` = " + jsonObject.getString("credential.id");
      System.out.println(query);
      int affectedRows = connection.createStatement().executeUpdate(query);

      System.out.println("affected rows  "+ affectedRows);

      if(affectedRows==0)throw new SQLException("id not exist");
    }catch (SQLException exception){
      return new JsonObject().put(Constant.STATUS,Constant.ERROR).put(Constant.STATUS_CODE,Constant.INTERNAL_SERVER_ERROR).put(Constant.ERROR,exception.getMessage());

    }

   // System.out.println(query);
return new JsonObject().put(Constant.STATUS,Constant.SUCCESS).put(Constant.STATUS_CODE,Constant.OK);

  }

  private JsonObject updateDis(JsonObject jsonObject){
StringBuilder queryInit = new StringBuilder();
    for(var data : jsonObject)
    {

     queryInit.append("`").append(data.getKey()).append("`").append(" = ").append("\"").append(data.getValue()).append("\"").append(",");
    }
    queryInit.deleteCharAt(queryInit.length()-1);
   String query = "UPDATE discovery SET " + queryInit + " WHERE `discovery.id` = " + jsonObject.getString("discovery.id");
    System.out.println(query);
    try (Connection connection = createConnection()){
      int affectedRows = connection.createStatement().executeUpdate(query);

      System.out.println("affected rows  "+ affectedRows);

      if(affectedRows==0)throw new SQLException("id not exist");
    }catch (SQLException exception){
      return new JsonObject().put(Constant.STATUS,Constant.ERROR).put(Constant.STATUS_CODE,Constant.INTERNAL_SERVER_ERROR).put(Constant.ERROR,exception.getMessage());

    }

    System.out.println(query);
return new JsonObject().put(Constant.STATUS,Constant.SUCCESS).put(Constant.STATUS_CODE,Constant.OK);

  }

  private static JsonObject checkIP(String ip,String identity) {
    System.out.println(ip);
    boolean output ;

    try (Connection connection = createConnection()){
      PreparedStatement preparedStatement = null;
      switch (identity){
        case "DISCOVERY":
           preparedStatement = connection.prepareStatement("select * from discovery where `discovery.id` = ?");
          preparedStatement.setString(1,ip);
           break;
        case "CREDENTIAL":
          preparedStatement = connection.prepareStatement("select * from credential where `credential.id` = ?");
          preparedStatement.setString(1,ip);
          break;
        case "MONITOR":
          preparedStatement = connection.prepareStatement("select * from monitor where `monitor.id` = ?");
          preparedStatement.setString(1,ip);
          break;

      }
      if(preparedStatement !=null) {
        ResultSet resultSet = preparedStatement.executeQuery();
        output = resultSet.next();

        if (output) {
          return new JsonObject().put(Constant.STATUS, Constant.DISCOVERED).put(Constant.STATUS_CODE, Constant.OK);
        } else {
          return new JsonObject().put(Constant.STATUS, Constant.NOT_DISCOVERED).put(Constant.STATUS_CODE, Constant.OK);
        }
      }else{
        return new JsonObject().put(Constant.STATUS,Constant.ERROR).put(Constant.STATUS_CODE,Constant.INTERNAL_SERVER_ERROR).put(Constant.ERROR,"null");

      }

    } catch (SQLException e) {

      return new JsonObject().put(Constant.STATUS,Constant.ERROR).put(Constant.STATUS_CODE,Constant.INTERNAL_SERVER_ERROR).put(Constant.ERROR,e.getMessage());

    }

  }

private static JsonObject insertInDatabase(JsonObject jsonObject) {


    try (Connection connection = createConnection()){

      PreparedStatement preparedStatement = connection.prepareStatement(Constant.QUERY_INSERT_TO_DISCOVERY_TABLE);
      preparedStatement.setString(1,jsonObject.getString(Constant.JSON_KEY_PORT));
      preparedStatement.setString(2,jsonObject.getString(Constant.JSON_KEY_HOST));
      preparedStatement.setString(3,jsonObject.getString(Constant.JSON_KEY_USERNAME));
      preparedStatement.setString(4,jsonObject.getString(Constant.JSON_KEY_PASSWORD));
      preparedStatement.setString(5,jsonObject.getString(Constant.JSON_KEY_METRIC_TYPE));
      preparedStatement.setString(6,jsonObject.getString(Constant.JSON_KEY_VERSION));

      preparedStatement.executeUpdate();

      return new JsonObject().put(Constant.STATUS,Constant.SUCCESS).put(Constant.STATUS_CODE,Constant.OK);


    } catch (SQLException e) {
      return new JsonObject().put(Constant.STATUS,Constant.ERROR).put(Constant.STATUS_CODE,Constant.INTERNAL_SERVER_ERROR).put(Constant.ERROR,e.getMessage());

    }
  }

  private static JsonObject insertInCred(JsonObject jsonObject){

    String username = null;
    String version = null;
    if (jsonObject.containsKey(Constant.JSON_KEY_USERNAME)) {
      username =  jsonObject.getString(Constant.JSON_KEY_USERNAME) ;
    }
    if (jsonObject.containsKey(Constant.JSON_KEY_VERSION)) {
      version =  jsonObject.getString(Constant.JSON_KEY_VERSION) ;
    }


    try (Connection connection = createConnection()){

      PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO `NMS2.1`.`credential`\n" +
        "(`credential.name`,\n" +
        "`protocol`,\n" +
        "`username`,\n" +
        "`password`,\n" +
        "`version`) VALUES (?,?,?,?,?)");

      System.out.println(jsonObject.getString(Constant.CREDENTIAL_NAME));
      System.out.println("hello misfortune ..." + jsonObject.getString("smit"));
      preparedStatement.setString(1,jsonObject.getString(Constant.CREDENTIAL_NAME));
      preparedStatement.setString(2,jsonObject.getString(Constant.PROTOCOL));
      preparedStatement.setString(3,username);
      preparedStatement.setString(4,jsonObject.getString(Constant.JSON_KEY_PASSWORD));
      preparedStatement.setString(5,version);

      preparedStatement.executeUpdate();


      ResultSet resultSet = connection.createStatement().executeQuery("SELECT MAX(`credential.id`) FROM credential;");
      int id=0;
      while (resultSet.next()){
         id = resultSet .getInt(1);
      }

      System.out.println(id);
      return new JsonObject().put(Constant.STATUS,Constant.SUCCESS).put(Constant.MESSAGE,id);


    } catch (SQLException e) {
      return new JsonObject().put(Constant.STATUS,Constant.ERROR).put(Constant.STATUS_CODE,Constant.INTERNAL_SERVER_ERROR).put(Constant.ERROR,e.getMessage());

    }
  }

private static JsonObject insertInDis(JsonObject jsonObject){


    try (Connection connection = createConnection()){

      PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO `NMS2.1`.`discovery`\n" +
        "(`discovery.name`,\n" +
        "`host`,\n" +
        "`port`,\n" +
        "`metric.type`,\n" +
        "`credential.id`) VALUES (?,?,?,?,?)");
      System.out.println(jsonObject.getString(Constant.DISCOVERY_NAME));
      preparedStatement.setString(1, jsonObject.getString(Constant.DISCOVERY_NAME));
      preparedStatement.setString(2,jsonObject.getString(Constant.JSON_KEY_HOST));
      preparedStatement.setString(3,jsonObject.getString(Constant.JSON_KEY_PORT));
      preparedStatement.setString(4,jsonObject.getString(Constant.JSON_KEY_METRIC_TYPE));
      preparedStatement.setString(5,jsonObject.getString(Constant.CREDENTIAL_ID));


      preparedStatement.executeUpdate();
      ResultSet resultSet = connection.createStatement().executeQuery("SELECT MAX(`discovery.id`) FROM discovery;");
      int id=0;
      while (resultSet.next()){
        id = resultSet .getInt(1);
      }

      return new JsonObject().put(Constant.STATUS,Constant.SUCCESS).put(Constant.MESSAGE,id);


    } catch (SQLException e) {
      return new JsonObject().put(Constant.STATUS,Constant.ERROR).put(Constant.STATUS_CODE,Constant.INTERNAL_SERVER_ERROR).put(Constant.ERROR,e.getMessage());

    }
  }

  private static Connection createConnection() throws SQLException {

    try{
      return DriverManager.getConnection(Constant.DATABASE_CONNECTION_URL, Constant.DATABASE_CONNECTION_USER, Constant.DATABASE_CONNECTION_PASSWORD);
    }

    catch (SQLException sqlException){
    throw new SQLException(Constant.CONNECTION_REFUSED);
    }
  }

}
