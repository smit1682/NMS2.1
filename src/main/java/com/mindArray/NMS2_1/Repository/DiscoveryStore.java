package com.mindArray.NMS2_1.Repository;

import com.mindArray.NMS2_1.Constant;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.rowset.JdbcRowSet;
import java.sql.*;

public class DiscoveryStore implements Crudable{

  private static final Logger LOGGER = LoggerFactory.getLogger(DiscoveryStore.class);

  @Override
  public JsonObject create(JsonObject jsonObject) {

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

  @Override
  public JsonObject read(JsonObject jsonObject) {
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

  @Override
  public JsonObject readAll(JsonObject jsonObject) {
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
      return new JsonObject().put("discovery",jsonArray);
    }catch (SQLException exception)
    {


      return new JsonObject().put(Constant.STATUS,Constant.ERROR).put(Constant.STATUS_CODE,Constant.INTERNAL_SERVER_ERROR).put(Constant.ERROR,exception.getMessage());

    }
  }

  @Override
  public JsonObject update(JsonObject jsonObject) {
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

  @Override
  public JsonObject delete(JsonObject string) {

    try (Connection connection = createConnection()){
      int master = connection.createStatement().executeUpdate("delete from discovery where `discovery.id` = "+string.getString("id"));
      System.out.println("delete from discovery where id = "+string);
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
