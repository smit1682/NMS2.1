package com.mindarray.nms.repository;

import com.mindarray.nms.util.Constant;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.sql.*;

public class UtilStore {

  public  void checkIP(String ip, String identity, Promise<Object> databaseHandler) {
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
          databaseHandler.complete( new JsonObject().put(Constant.STATUS, Constant.DISCOVERED).put(Constant.STATUS_CODE, Constant.OK));
        } else {
          databaseHandler.fail( new JsonObject().put(Constant.STATUS, Constant.NOT_DISCOVERED).put(Constant.STATUS_CODE, Constant.OK).encodePrettily());
        }
      }else{
        databaseHandler.fail( new JsonObject().put(Constant.STATUS,Constant.ERROR).put(Constant.STATUS_CODE,Constant.INTERNAL_SERVER_ERROR).put(Constant.ERROR,"null").encodePrettily());

      }

    } catch (SQLException e) {

      databaseHandler.fail( new JsonObject().put(Constant.STATUS,Constant.ERROR).put(Constant.STATUS_CODE,Constant.INTERNAL_SERVER_ERROR).put(Constant.ERROR,e.getMessage()).encodePrettily());

    }

  }

  public void getLastInstance(JsonObject jsonMessage,Promise<Object> databaseHandler) {
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
      databaseHandler.complete( lastData);
    }
    catch (Exception e){
      databaseHandler.fail(new JsonObject().put(Constant.STATUS,Constant.ERROR).put(Constant.STATUS_CODE,Constant.INTERNAL_SERVER_ERROR).put(Constant.ERROR,e.getMessage()).encodePrettily());

    }
  }

  public void topFive(JsonObject jsonMessage,Promise<Object> databaseHandler)  {

    try (Connection connection = createConnection()){
      String query = "";
      if(jsonMessage.getString(Constant.JSON_KEY_METRIC_GROUP).equals(Constant.CPU)){
        query = "select  `monitor.name`,max(`data` -> '$.\"cpu.all.user.percentage\"') AS `cpu.all.user.percentage` from metric group by `monitor.name`  order by `cpu.all.user.percentage`*1 DESC limit 5";
      }
      else if(jsonMessage.getString(Constant.JSON_KEY_METRIC_GROUP).equals(Constant.MEMORY)){
        query = "select  `monitor.name`,max(`data` -> '$.\"memory.free.bytes\"') AS `free.memory.bytes`  from metric group by `monitor.name` order by `free.memory.bytes`*1 DESC limit 5 ";
      }
      else{
        databaseHandler.fail( new JsonObject().put(Constant.STATUS,Constant.ERROR).put(Constant.STATUS,Constant.INVALID_METRIC_GROUP).encodePrettily());
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
      databaseHandler.complete( new JsonObject().put(Constant.STATUS,Constant.SUCCESS).put("data",jsonArray));


    }catch (Exception e){databaseHandler.fail( new JsonObject().put(Constant.STATUS,Constant.ERROR).put(Constant.STATUS_CODE,Constant.INTERNAL_SERVER_ERROR).put(Constant.ERROR,e.getMessage()).encodePrettily());
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
