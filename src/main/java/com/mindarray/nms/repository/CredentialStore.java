package com.mindarray.nms.repository;

import com.mindarray.nms.util.Constant;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Map;

public class CredentialStore implements CrudRepository {
  private static final Logger LOGGER = LoggerFactory.getLogger(CredentialStore.class);

  @Override
  public void create(JsonObject jsonObject, Promise<Object> databaseHandler) {


    try (Connection connection = createConnection()){

      PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO `NMS2.1`.`credential`\n" +
        "(`credential.name`,\n" +
        "`protocol`,\n" +
        "`username`,\n" +
        "`password`,\n" +
        "`version`) VALUES (?,?,?,?,?)");

      System.out.println(jsonObject.getString(Constant.CREDENTIAL_NAME));
      //System.out.println("hello misfortune ..." + jsonObject.getString("smit"));
      preparedStatement.setString(1,jsonObject.getString(Constant.CREDENTIAL_NAME));
      preparedStatement.setString(2,jsonObject.getString(Constant.PROTOCOL));
      preparedStatement.setString(3,jsonObject.getString(Constant.JSON_KEY_USERNAME));
      preparedStatement.setString(4,jsonObject.getString(Constant.JSON_KEY_PASSWORD));
      preparedStatement.setString(5,jsonObject.getString(Constant.JSON_KEY_VERSION));

      preparedStatement.executeUpdate();


      ResultSet resultSet = connection.createStatement().executeQuery("SELECT MAX(`credential.id`) FROM credential;");
      int id=0;
      while (resultSet.next()){
        id = resultSet .getInt(1);
      }

      System.out.println(id);
      databaseHandler.complete(new JsonObject().put(Constant.STATUS,Constant.SUCCESS).put(Constant.MESSAGE,id));



    } catch (SQLException e) {
      databaseHandler.fail(new JsonObject().put(Constant.STATUS,Constant.ERROR).put(Constant.STATUS_CODE,Constant.INTERNAL_SERVER_ERROR).put(Constant.ERROR,e.getMessage()).encodePrettily());
     // return new JsonObject().put(Constant.STATUS,Constant.ERROR).put(Constant.STATUS_CODE,Constant.INTERNAL_SERVER_ERROR).put(Constant.ERROR,e.getMessage());

    }

  }

  @Override
  public void read(JsonObject jsonObject,Promise<Object> databaseHandler) {
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
      databaseHandler.complete( data);
    }catch (SQLException exception)
    {

      databaseHandler.fail( new JsonObject().put(Constant.STATUS,Constant.ERROR).put(Constant.STATUS_CODE,Constant.INTERNAL_SERVER_ERROR).put(Constant.ERROR,exception.getMessage()).encodePrettily());
    }
  }

  @Override
  public void readAll(JsonObject jsonObject,Promise<Object> databaseHandler) {
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
      databaseHandler.complete( new JsonObject().put("credential",jsonArray));
    }catch (SQLException exception)
    {


      databaseHandler.fail(new JsonObject().put(Constant.STATUS,Constant.ERROR).put(Constant.STATUS_CODE,Constant.INTERNAL_SERVER_ERROR).put(Constant.ERROR,exception.getMessage()).encodePrettily());

    }
  }

  @Override
  public void update(JsonObject jsonObject,Promise<Object> databaseHandler) {
    try (Connection connection = createConnection()){
      StringBuilder queryInit = new StringBuilder();
      for(Map.Entry<String,Object> data : jsonObject)
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
      databaseHandler.fail( new JsonObject().put(Constant.STATUS,Constant.ERROR).put(Constant.STATUS_CODE,Constant.INTERNAL_SERVER_ERROR).put(Constant.ERROR,exception.getMessage()).encodePrettily());

    }

    // System.out.println(query);
    databaseHandler.complete( new JsonObject().put(Constant.STATUS,Constant.SUCCESS).put(Constant.STATUS_CODE,Constant.OK));

  }

  @Override
  public void delete(JsonObject string,Promise<Object> databaseHandler) {
    try (Connection connection = createConnection()){
      int master = connection.createStatement().executeUpdate("delete from credential where `credential.id` = "+string.getString("id"));
      System.out.println("delete from credential where credential.id = "+string);
      databaseHandler.complete( new JsonObject().put(Constant.STATUS,Constant.SUCCESS).put(Constant.STATUS_CODE,Constant.OK));

    }catch (SQLException exception){
      databaseHandler.fail( new JsonObject().put(Constant.STATUS,Constant.ERROR).put(Constant.STATUS_CODE,Constant.INTERNAL_SERVER_ERROR).put(Constant.ERROR,exception.getMessage()).encodePrettily());

    }
  }

  public void intialRead(JsonObject body,Promise<Object> databaseHandler) {

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

      if (!jsonArray.isEmpty())
      {
        databaseHandler.complete(jsonArray);
      }
      else {
        databaseHandler.fail(jsonArray.encodePrettily());
      }
    }catch (SQLException exception)
    {
      databaseHandler.fail( new JsonObject().put(Constant.STATUS,Constant.ERROR).put(Constant.STATUS_CODE,Constant.INTERNAL_SERVER_ERROR).put(Constant.ERROR,exception.getMessage()).encodePrettily());

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
