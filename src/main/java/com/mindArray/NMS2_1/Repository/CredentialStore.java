package com.mindArray.NMS2_1.Repository;

import com.mindArray.NMS2_1.Constant;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

public class CredentialStore implements Crudable{
  private static final Logger LOGGER = LoggerFactory.getLogger(CredentialStore.class);

  @Override
  public JsonObject create(JsonObject jsonObject) {


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
      return new JsonObject().put(Constant.STATUS,Constant.SUCCESS).put(Constant.MESSAGE,id);


    } catch (SQLException e) {
      return new JsonObject().put(Constant.STATUS,Constant.ERROR).put(Constant.STATUS_CODE,Constant.INTERNAL_SERVER_ERROR).put(Constant.ERROR,e.getMessage());

    }

  }

  @Override
  public JsonObject read(JsonObject jsonObject) {
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

  @Override
  public JsonObject readAll(JsonObject jsonObject) {
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

  @Override
  public JsonObject update(JsonObject jsonObject) {
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

  @Override
  public JsonObject delete(JsonObject string) {
    try (Connection connection = createConnection()){
      int master = connection.createStatement().executeUpdate("delete from credential where `credential.id` = "+string.getString("id"));
      System.out.println("delete from credential where credential.id = "+string);
      return new JsonObject().put(Constant.STATUS,Constant.SUCCESS).put(Constant.STATUS_CODE,Constant.OK);

    }catch (SQLException exception){
      return new JsonObject().put(Constant.STATUS,Constant.ERROR).put(Constant.STATUS_CODE,Constant.INTERNAL_SERVER_ERROR).put(Constant.ERROR,exception.getMessage());

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
