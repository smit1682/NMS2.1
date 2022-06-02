package com.mindarray.nms.store;

import com.mindarray.nms.util.Constant;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Map;

public class DiscoveryStore implements CrudStore {

  private static final Logger LOGGER = LoggerFactory.getLogger(DiscoveryStore.class);

  @Override
  public void create(JsonObject jsonObject, Promise<Object> databaseHandler)
  {

    try (Connection connection = createConnection();
         PreparedStatement preparedStatement = connection.prepareStatement(Constant.QUERY_DISCOVERY_INSERT))
    {

      preparedStatement.setString(1, jsonObject.getString(Constant.DISCOVERY_NAME));
      preparedStatement.setString(2,jsonObject.getString(Constant.JSON_KEY_HOST));
      preparedStatement.setString(3,jsonObject.getString(Constant.JSON_KEY_PORT));
      preparedStatement.setString(4,jsonObject.getString(Constant.JSON_KEY_METRIC_TYPE));
      preparedStatement.setString(5,jsonObject.getString(Constant.CREDENTIAL_ID));

      preparedStatement.executeUpdate();
      int id = 0;
      try(ResultSet resultSet = connection.createStatement().executeQuery(Constant.QUERY_DISCOVERY_ID))
      {
        while (resultSet.next())
        {
          id = resultSet.getInt(1);
        }
      }
      databaseHandler.complete(new JsonObject().put(Constant.STATUS,Constant.SUCCESS).put(Constant.MESSAGE,id));

    }
    catch (Exception e)
    {
      LOGGER.error(e.getMessage());

      databaseHandler.fail(new JsonObject().put(Constant.STATUS,Constant.ERROR).put(Constant.STATUS_CODE,Constant.INTERNAL_SERVER_ERROR).put(Constant.ERROR,e.getMessage()).encodePrettily());

    }
  }

  @Override
  public void read(JsonObject jsonObject,Promise<Object> databaseHandler)
  {

    try (Connection connection = createConnection())
    {

      ResultSet resultSet = connection.createStatement().executeQuery("select * from discovery where `discovery.id` = "+ jsonObject.getString("id"));

      JsonObject data = new JsonObject();

      while (resultSet.next())
      {

        data.put(Constant.DISCOVERY_ID,resultSet.getInt(Constant.DISCOVERY_ID));
        data.put(Constant.DISCOVERY_NAME,resultSet.getString(Constant.DISCOVERY_NAME));
        data.put(Constant.JSON_KEY_HOST,resultSet.getString(Constant.JSON_KEY_HOST));
        data.put(Constant.CREDENTIAL_ID,resultSet.getInt(Constant.CREDENTIAL_ID));
        data.put(Constant.JSON_KEY_PORT,resultSet.getString(Constant.JSON_KEY_PORT));
        data.put(Constant.JSON_KEY_METRIC_TYPE,resultSet.getString(Constant.JSON_KEY_METRIC_TYPE));
        data.put(Constant.DISCOVERY_STATUS,resultSet.getString(Constant.DISCOVERY_STATUS));


      }
      databaseHandler.complete( data);


    }catch (Exception exception)
    {

      LOGGER.error(exception.getMessage());

      databaseHandler.fail( new JsonObject().put(Constant.STATUS,Constant.ERROR).put(Constant.STATUS_CODE,Constant.INTERNAL_SERVER_ERROR).put(Constant.ERROR,exception.getMessage()).encodePrettily());

    }
  }

  @Override
  public void readAll(JsonObject jsonObject, Promise<Object> databaseHandler)
  {

    try (Connection connection = createConnection())
    {

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
        data.put("discovery.status",resultSet.getString("discovery.status"));

        jsonArray.add(data);

      }

      databaseHandler.complete( new JsonObject().put("discovery",jsonArray));

    }
    catch (Exception exception)
    {

      LOGGER.error(exception.getMessage());

      databaseHandler.fail( new JsonObject().put(Constant.STATUS,Constant.ERROR).put(Constant.STATUS_CODE,Constant.INTERNAL_SERVER_ERROR).put(Constant.ERROR,exception.getMessage()).encodePrettily());

    }
  }

  @Override
  public void update(JsonObject jsonObject,Promise<Object> databaseHandler)
  {

    StringBuilder queryInit = new StringBuilder();

    for(Map.Entry<String,Object> data : jsonObject)
    {
      queryInit.append("`").append(data.getKey()).append("`").append(" = ").append("\"").append(data.getValue()).append("\"").append(",");
    }

    queryInit.deleteCharAt(queryInit.length()-1);

    String query = "UPDATE discovery SET " + queryInit + " WHERE `discovery.id` = " + jsonObject.getString("discovery.id");

    try (Connection connection = createConnection())
    {

      int affectedRows = connection.createStatement().executeUpdate(query);

      if(affectedRows==0)throw new SQLException("id not exist");
      else {

        PreparedStatement preparedStatement = connection.prepareStatement("UPDATE discovery SET `discovery.status` = ?, `discovery.result` = ?,`monitor.name` = ? where `discovery.id` = ?");
        preparedStatement.setString(1,"false");
        preparedStatement.setString(2,"Update happened in Discovery");
        preparedStatement.setString(3,null);
        preparedStatement.setString(4,jsonObject.getString("discovery.id"));
        preparedStatement.executeUpdate();

        preparedStatement.close();
      }


    }
    catch (SQLException exception)
    {
      LOGGER.error(exception.getMessage());

      databaseHandler.fail( new JsonObject().put(Constant.STATUS,Constant.ERROR).put(Constant.STATUS_CODE,Constant.INTERNAL_SERVER_ERROR).put(Constant.ERROR,exception.getMessage()).encodePrettily());

    }

    databaseHandler.complete( new JsonObject().put(Constant.STATUS,Constant.SUCCESS).put(Constant.STATUS_CODE,Constant.OK));

  }

  @Override
  public void delete(JsonObject string,Promise<Object> databaseHandler)
  {

    try (Connection connection = createConnection())
    {
       connection.createStatement().executeUpdate("delete from discovery where `discovery.id` = "+string.getString("id"));

      databaseHandler.complete( new JsonObject().put(Constant.STATUS,Constant.SUCCESS).put(Constant.STATUS_CODE,Constant.OK));

    }
    catch (SQLException exception)
    {

      LOGGER.error(exception.getMessage());

      databaseHandler.fail( new JsonObject().put(Constant.STATUS,Constant.ERROR).put(Constant.STATUS_CODE,Constant.INTERNAL_SERVER_ERROR).put(Constant.ERROR,exception.getMessage()).encodePrettily());

    }

  }

  public void updateAfterRunDiscovery(JsonObject body,Promise<Object> databaseHandler)
  {

    try (Connection connection = createConnection())
    {
      PreparedStatement preparedStatement = connection.prepareStatement("UPDATE discovery SET `discovery.status` = ?, `discovery.result` = ?,`monitor.name` = ? where `discovery.id` = ?");

      if(body.getString(Constant.STATUS).equals(Constant.SUCCESS))
      {
        preparedStatement.setString(1,"true");
        preparedStatement.setString(2,body.toString());
        preparedStatement.setString(3,body.getString("monitor.name"));
        preparedStatement.setString(4,body.getString("discovery.id"));
      }
      else
      {
        preparedStatement.setString(1,"false");
        preparedStatement.setString(2,body.getString("result"));
        preparedStatement.setString(3,null);
        preparedStatement.setString(4,body.getString("discovery.id"));
      }



      preparedStatement.executeUpdate();

    preparedStatement.close();
    }
    catch (Exception exception)
    {
      LOGGER.error(exception.getMessage());

      databaseHandler.fail( new JsonObject().put(Constant.STATUS,Constant.ERROR).put(Constant.STATUS_CODE,Constant.INTERNAL_SERVER_ERROR).put(Constant.ERROR,exception.getMessage()).encodePrettily());
    }


    databaseHandler.complete( new JsonObject().put(Constant.STATUS,Constant.SUCCESS).put(Constant.STATUS_CODE,Constant.OK));
  }

  public void mergeData(JsonObject jsonObject,Promise<Object> databaseHandler) {
    try (Connection connection = createConnection()){
      ResultSet resultSet = connection.createStatement().executeQuery("select * from discovery left join credential on discovery.`credential.id` = credential.`credential.id` where discovery.`discovery.id` = " + jsonObject.getString("discovery.id"));

      while (resultSet.next())
      {

        jsonObject.put("credential.id",resultSet.getInt("credential.id"));
        jsonObject.put("credential.name",resultSet.getString("credential.name"));
        jsonObject.put("protocol",resultSet.getString(Constant.PROTOCOL));
        jsonObject.put(Constant.JSON_KEY_USERNAME,resultSet.getString(Constant.JSON_KEY_USERNAME));
        jsonObject.put(Constant.JSON_KEY_PASSWORD,resultSet.getString(Constant.JSON_KEY_PASSWORD));
        jsonObject.put(Constant.COMMUNITY,resultSet.getString(Constant.COMMUNITY));
        jsonObject.put(Constant.JSON_KEY_VERSION,resultSet.getString(Constant.JSON_KEY_VERSION));

        jsonObject.put(Constant.JSON_KEY_HOST,resultSet.getString(Constant.JSON_KEY_HOST));
        jsonObject.put(Constant.JSON_KEY_PORT,resultSet.getString(Constant.JSON_KEY_PORT));
        jsonObject.put("metric.type",resultSet.getString("metric.type"));



      }
      databaseHandler.complete( jsonObject);
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
