package com.mindarray.nms.store;

import com.mindarray.nms.util.Constant;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.sql.*;
import java.util.Map;

public class MonitorStore implements CrudStore {

  @Override
  public void create(JsonObject body, Promise<Object> databaseHandler) {

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
      JsonObject jsonObject = new JsonObject().put("monitor.id",id); //can be removed
      resultSet = connection.createStatement().executeQuery("select `cpu`, `memory`, `disk`, `system`, `process`, `interface` from monitor where `monitor.id` = " + id);
      while (resultSet.next()){
        jsonObject.put("cpu",resultSet.getInt("cpu"))
          .put("memory",resultSet.getInt("memory"))
          .put("disk",resultSet.getInt("disk"))
          .put("system",resultSet.getInt("system"))
          .put("process",resultSet.getInt("process"))
          .put("interface",resultSet.getInt("interface"));
      }

       databaseHandler.complete(jsonObject.put(Constant.STATUS,Constant.SUCCESS).mergeIn(body));


    } catch (SQLException e) {
      databaseHandler.fail( new JsonObject().put(Constant.STATUS,Constant.ERROR).put(Constant.STATUS_CODE,Constant.INTERNAL_SERVER_ERROR).put(Constant.ERROR,e.getMessage()).encodePrettily());

    }

  }

  @Override
  public void read(JsonObject jsonObject, Promise<Object> databaseHandler) {

    try (Connection connection = createConnection()){
      ResultSet resultSet = connection.createStatement().executeQuery("select * from monitor where `monitor.id` = " + jsonObject.getString("id"));
      JsonObject data = new JsonObject();
      while (resultSet.next())
      {

        data.put(Constant.MONITOR_ID,resultSet.getInt(Constant.MONITOR_ID));
        data.put(Constant.MONITOR_NAME,resultSet.getString(Constant.MONITOR_NAME));
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
      databaseHandler.complete( data);
    }catch (SQLException exception)
    {

      databaseHandler.fail( new JsonObject().put(Constant.STATUS,Constant.ERROR).put(Constant.STATUS_CODE,Constant.INTERNAL_SERVER_ERROR).put(Constant.ERROR,exception.getMessage()).encodePrettily());

    }


  }

  @Override
  public void readAll(JsonObject jsonObject, Promise<Object> databaseHandler) {

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
      databaseHandler.complete( new JsonObject().put("monitor",jsonArray));
    }catch (SQLException exception)
    {

databaseHandler.fail(new JsonObject().put(Constant.STATUS,Constant.ERROR).put(Constant.STATUS_CODE,Constant.INTERNAL_SERVER_ERROR).put(Constant.ERROR,exception.getMessage()).encodePrettily());

    }
  }

  @Override
  public void update(JsonObject jsonObject, Promise<Object> databaseHandler) {

    try (Connection connection = createConnection()){
      StringBuilder queryInit = new StringBuilder();
      for(Map.Entry<String,Object> data : jsonObject)
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
      databaseHandler.fail( new JsonObject().put(Constant.STATUS,Constant.ERROR).put(Constant.STATUS_CODE,Constant.INTERNAL_SERVER_ERROR).put(Constant.ERROR,exception.getMessage()).encodePrettily());

    }
    catch (ClassCastException exception){
      databaseHandler.fail( new JsonObject().put(Constant.STATUS,Constant.ERROR).put(Constant.STATUS_CODE,Constant.MUST_BE_INTEGER).put(Constant.ERROR,exception.getMessage()).encodePrettily());

    }

    // System.out.println(query);
    databaseHandler.complete( new JsonObject().put(Constant.STATUS,Constant.SUCCESS).put(Constant.STATUS_CODE,Constant.OK));

  }

  @Override
  public void delete(JsonObject jsonObject, Promise<Object> databaseHandler) {
    try (Connection connection = createConnection()){
      int master = connection.createStatement().executeUpdate("delete from monitor where `monitor.id` = "+jsonObject.getString("id"));

      databaseHandler.complete( new JsonObject().put(Constant.STATUS,Constant.SUCCESS).put(Constant.STATUS_CODE,Constant.OK));

    }catch (SQLException exception){
      databaseHandler.fail( new JsonObject().put(Constant.STATUS,Constant.ERROR).put(Constant.STATUS_CODE,Constant.INTERNAL_SERVER_ERROR).put(Constant.ERROR,exception.getMessage()).encodePrettily());

    }
  }

  public void discoveryCheck(JsonObject body,Promise<Object> databaseHandler) {
    System.out.println("body" + body);
    try (Connection connection = createConnection()){
      ResultSet resultSet = connection.createStatement().executeQuery("select `discovery.status`,`monitor.name`,`metric.type` from discovery where `discovery.id` = "+ body.getString("discovery.id"));
      while (resultSet.next()){
        body.put("discovery.status",resultSet.getString("discovery.status"))
          .put("monitor.name",resultSet.getString("monitor.name"))
          .put("metric.type",resultSet.getString("metric.type"));

      }
      body.mergeIn(mergeData(body));
      System.out.println("after mergeIn= " + body);
      if (body.getString("discovery.status").equals("true"))
      {
        databaseHandler.complete(body);
      }
      else {
        databaseHandler.fail(body.encodePrettily());
      }

    }catch (SQLException exception){
      databaseHandler.fail( new JsonObject().put(Constant.STATUS,Constant.ERROR).put(Constant.STATUS_CODE,Constant.INTERNAL_SERVER_ERROR).put(Constant.ERROR,exception.getMessage()).encodePrettily());

    }
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

  public void dumpInDB(JsonObject body,Promise<Object> databaseHandler) {
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

      databaseHandler.complete( new JsonObject().put(Constant.STATUS,Constant.SUCCESS));


    } catch (SQLException e) {
      databaseHandler.fail( new JsonObject().put(Constant.STATUS,Constant.ERROR).put(Constant.STATUS_CODE,Constant.INTERNAL_SERVER_ERROR).put(Constant.ERROR,e.getMessage()).encodePrettily());

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

  public void createContext(JsonObject dataMessage, Promise<Object> databaseHandler) {

    try(Connection connection = createConnection()){

      PreparedStatement preparedStatement = connection.prepareStatement("select * from monitor left join credential on monitor.`credential.id` =  credential.`credential.id` where `monitor.id` = ? ");
      preparedStatement.setInt(1,dataMessage.getInteger("monitor.id"));

      ResultSet resultSet = preparedStatement.executeQuery();

      while (resultSet.next())
      {

        dataMessage.put(Constant.MONITOR_ID,resultSet.getInt(Constant.MONITOR_ID));
        dataMessage.put(Constant.MONITOR_NAME,resultSet.getString(Constant.MONITOR_NAME));
        dataMessage.put(Constant.JSON_KEY_HOST,resultSet.getString(Constant.JSON_KEY_HOST));
        dataMessage.put(Constant.JSON_KEY_PORT,resultSet.getString(Constant.JSON_KEY_PORT));
        dataMessage.put(Constant.JSON_KEY_METRIC_TYPE,resultSet.getString(Constant.JSON_KEY_METRIC_TYPE));
        dataMessage.put(Constant.JSON_KEY_USERNAME,resultSet.getString(Constant.JSON_KEY_USERNAME));
        dataMessage.put(Constant.JSON_KEY_PASSWORD,resultSet.getString(Constant.JSON_KEY_PASSWORD));
        dataMessage.put(Constant.JSON_KEY_VERSION,resultSet.getString(Constant.JSON_KEY_VERSION));

      }

      databaseHandler.complete(dataMessage);


    }catch (SQLException e){
      databaseHandler.fail( new JsonObject().put(Constant.STATUS,Constant.ERROR).put(Constant.STATUS_CODE,Constant.INTERNAL_SERVER_ERROR).put(Constant.ERROR,e.getMessage()).encodePrettily());

    }
  }
}
