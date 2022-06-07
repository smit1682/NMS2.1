package com.mindarray.nms.store;

import com.mindarray.nms.util.Constant;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Map;

public class DiscoveryStore implements CrudStore
{
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

      databaseHandler.complete(new JsonObject().put(Constant.STATUS,Constant.SUCCESS).put(Constant.MESSAGE,id).put(Constant.STATUS_CODE,Constant.OK));

    }
    catch (SQLException sqlException)
    {
      LOGGER.error(sqlException.getMessage(),sqlException);

      databaseHandler.fail(new JsonObject().put(Constant.STATUS,Constant.FAIL)
        .put(Constant.STATUS_CODE,Constant.BAD_REQUEST)
        .put(Constant.ERROR,sqlException.getMessage()).encodePrettily());
    }
    catch (Exception exception)
    {
      LOGGER.error(exception.getMessage(),exception);

      databaseHandler.fail(new JsonObject().put(Constant.STATUS,Constant.FAIL)
        .put(Constant.STATUS_CODE,Constant.INTERNAL_SERVER_ERROR)
        .put(Constant.ERROR,exception.getMessage()).encodePrettily());
    }
  }

  @Override
  public void read(JsonObject jsonObject,Promise<Object> databaseHandler)
  {
    try (Connection connection = createConnection();
         ResultSet resultSet = connection.createStatement().executeQuery(Constant.QUERY_READ_DISCOVERY+ jsonObject.getString("id"))
    )
    {
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
      databaseHandler.complete(new JsonObject().put(Constant.STATUS,Constant.SUCCESS).put(Constant.STATUS_CODE,Constant.OK).put(Constant.RESULT,data));
    }
    catch (SQLException sqlException)
    {
      LOGGER.error(sqlException.getMessage(),sqlException);

      databaseHandler.fail(new JsonObject().put(Constant.STATUS,Constant.FAIL)
        .put(Constant.STATUS_CODE,Constant.BAD_REQUEST)
        .put(Constant.ERROR,sqlException.getMessage()).encodePrettily());
    }
    catch (Exception exception)
    {
      LOGGER.error(exception.getMessage(),exception);

      databaseHandler.fail(new JsonObject().put(Constant.STATUS,Constant.FAIL)
        .put(Constant.STATUS_CODE,Constant.INTERNAL_SERVER_ERROR)
        .put(Constant.ERROR,exception.getMessage()).encodePrettily());
    }
  }

  @Override
  public void readAll(JsonObject jsonObject, Promise<Object> databaseHandler)
  {

    try (Connection connection = createConnection();ResultSet resultSet = connection.createStatement().executeQuery("select * from discovery"))
    {
      JsonArray readAllContainer = new JsonArray();

      while (resultSet.next())
      {
        JsonObject data = new JsonObject();

        data.put(Constant.DISCOVERY_ID,resultSet.getInt(Constant.DISCOVERY_ID));
        data.put(Constant.DISCOVERY_NAME,resultSet.getString(Constant.DISCOVERY_NAME));
        data.put(Constant.JSON_KEY_HOST,resultSet.getString(Constant.JSON_KEY_HOST));
        data.put(Constant.CREDENTIAL_ID,resultSet.getInt(Constant.CREDENTIAL_ID));
        data.put(Constant.JSON_KEY_PORT,resultSet.getString(Constant.JSON_KEY_PORT));
        data.put(Constant.JSON_KEY_METRIC_TYPE,resultSet.getString(Constant.JSON_KEY_METRIC_TYPE));
        data.put(Constant.DISCOVERY_STATUS,resultSet.getString(Constant.DISCOVERY_STATUS));

        readAllContainer.add(data);
      }

      databaseHandler.complete( new JsonObject().put(Constant.STATUS,Constant.SUCCESS).put(Constant.STATUS_CODE,Constant.OK).put(Constant.RESULT,readAllContainer));
    }
    catch (SQLException sqlException)
    {
      LOGGER.error(sqlException.getMessage(),sqlException);

      databaseHandler.fail(new JsonObject().put(Constant.STATUS,Constant.FAIL)
        .put(Constant.STATUS_CODE,Constant.BAD_REQUEST)
        .put(Constant.ERROR,sqlException.getMessage()).encodePrettily());
    }
    catch (Exception exception)
    {
      LOGGER.error(exception.getMessage(),exception);

      databaseHandler.fail(new JsonObject().put(Constant.STATUS,Constant.FAIL)
        .put(Constant.STATUS_CODE,Constant.INTERNAL_SERVER_ERROR)
        .put(Constant.ERROR,exception.getMessage()).encodePrettily());
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

    try (Connection connection = createConnection();Statement statement = connection.createStatement())
    {

      int affectedRows = statement.executeUpdate(query);

      if(affectedRows==0)throw new SQLException("id not exist");
      else
      {
        try(PreparedStatement preparedStatement = connection.prepareStatement("UPDATE discovery SET `discovery.status` = ?, `discovery.result` = ?,`monitor.name` = ? where `discovery.id` = ?")) {
          preparedStatement.setString(1, "false");
          preparedStatement.setString(2, "Update happened in Discovery");
          preparedStatement.setString(3, null);
          preparedStatement.setString(4, jsonObject.getString(Constant.DISCOVERY_ID));
          preparedStatement.executeUpdate();
        }

      }

      databaseHandler.complete( new JsonObject().put(Constant.STATUS,Constant.SUCCESS).put(Constant.STATUS_CODE,Constant.OK));

    }
    catch (SQLException sqlException)
    {
      LOGGER.error(sqlException.getMessage(),sqlException);

      databaseHandler.fail(new JsonObject().put(Constant.STATUS,Constant.FAIL)
        .put(Constant.STATUS_CODE,Constant.BAD_REQUEST)
        .put(Constant.ERROR,sqlException.getMessage()).encodePrettily());
    }
    catch (Exception exception)
    {
      LOGGER.error(exception.getMessage(),exception);

      databaseHandler.fail(new JsonObject().put(Constant.STATUS,Constant.FAIL)
        .put(Constant.STATUS_CODE,Constant.INTERNAL_SERVER_ERROR)
        .put(Constant.ERROR,exception.getMessage()).encodePrettily());
    }
  }

  @Override
  public void delete(JsonObject entries,Promise<Object> databaseHandler)
  {
    try (Connection connection = createConnection();Statement statement = connection.createStatement())
    {
       statement.executeUpdate(Constant.QUERY_DELETE_DISCOVERY + entries.getString(Constant.ID));

      databaseHandler.complete( new JsonObject().put(Constant.STATUS,Constant.SUCCESS).put(Constant.STATUS_CODE,Constant.OK));

      LOGGER.info("DISCOVERY ID {} DELETED",entries.getString(Constant.ID));

    }
    catch (SQLException sqlException)
    {
      LOGGER.error(sqlException.getMessage(),sqlException);

      databaseHandler.fail(new JsonObject().put(Constant.STATUS,Constant.FAIL)
        .put(Constant.STATUS_CODE,Constant.BAD_REQUEST)
        .put(Constant.ERROR,sqlException.getMessage()).encodePrettily());
    }
    catch (Exception exception)
    {
      LOGGER.error(exception.getMessage(),exception);

      databaseHandler.fail(new JsonObject().put(Constant.STATUS,Constant.FAIL)
        .put(Constant.STATUS_CODE,Constant.INTERNAL_SERVER_ERROR)
        .put(Constant.ERROR,exception.getMessage()).encodePrettily());
    }

  }

  public void updateAfterRunDiscovery(JsonObject body,Promise<Object> databaseHandler)
  {

    try (Connection connection = createConnection() ;
         PreparedStatement preparedStatement = connection.prepareStatement("UPDATE discovery SET `discovery.status` = ?, `discovery.result` = ?,`monitor.name` = ? where `discovery.id` = ?")
    )
    {

      if(body.getString(Constant.STATUS).equals(Constant.SUCCESS))
      {
        preparedStatement.setString(1,"true");
        preparedStatement.setString(2,body.toString());
        preparedStatement.setString(3,body.getString(Constant.MONITOR_NAME));
        preparedStatement.setString(4,body.getString(Constant.DISCOVERY_ID));
      }
      else
      {
        preparedStatement.setString(1,"false");
        preparedStatement.setString(2,body.getString(Constant.RESULT));
        preparedStatement.setString(3,null);
        preparedStatement.setString(4,body.getString(Constant.DISCOVERY_ID));
      }



      preparedStatement.executeUpdate();

      databaseHandler.complete( new JsonObject().put(Constant.STATUS,Constant.SUCCESS).put(Constant.STATUS_CODE,Constant.OK));


    }
    catch (SQLException sqlException)
    {
      LOGGER.error(sqlException.getMessage(),sqlException);

      databaseHandler.fail(new JsonObject().put(Constant.STATUS,Constant.FAIL)
        .put(Constant.STATUS_CODE,Constant.BAD_REQUEST)
        .put(Constant.ERROR,sqlException.getMessage()).encodePrettily());
    }
    catch (Exception exception)
    {
      LOGGER.error(exception.getMessage(),exception);

      databaseHandler.fail(new JsonObject().put(Constant.STATUS,Constant.FAIL)
        .put(Constant.STATUS_CODE,Constant.INTERNAL_SERVER_ERROR)
        .put(Constant.ERROR,exception.getMessage()).encodePrettily());
    }


      }

  public void mergeData(JsonObject entries,Promise<Object> databaseHandler)
  {
    try (Connection connection = createConnection();
         ResultSet resultSet = connection.createStatement().executeQuery("select * from discovery left join credential on discovery.`credential.id` = credential.`credential.id` where discovery.`discovery.id` = " + entries.getString("discovery.id"))
    )
    {
      while (resultSet.next())
      {

        entries.put(Constant.CREDENTIAL_ID,resultSet.getInt(Constant.CREDENTIAL_ID));
        entries.put(Constant.CREDENTIAL_NAME,resultSet.getString(Constant.CREDENTIAL_NAME));
        entries.put(Constant.PROTOCOL,resultSet.getString(Constant.PROTOCOL));
        entries.put(Constant.JSON_KEY_USERNAME,resultSet.getString(Constant.JSON_KEY_USERNAME));
        entries.put(Constant.JSON_KEY_PASSWORD,resultSet.getString(Constant.JSON_KEY_PASSWORD));
        entries.put(Constant.COMMUNITY,resultSet.getString(Constant.COMMUNITY));
        entries.put(Constant.JSON_KEY_VERSION,resultSet.getString(Constant.JSON_KEY_VERSION));

        entries.put(Constant.JSON_KEY_HOST,resultSet.getString(Constant.JSON_KEY_HOST));
        entries.put(Constant.JSON_KEY_PORT,resultSet.getString(Constant.JSON_KEY_PORT));
        entries.put(Constant.JSON_KEY_METRIC_TYPE,resultSet.getString(Constant.JSON_KEY_METRIC_TYPE));
      }

      databaseHandler.complete( entries);
    }
    catch (SQLException sqlException)
    {
      LOGGER.error(sqlException.getMessage(),sqlException);

      databaseHandler.fail(new JsonObject().put(Constant.STATUS,Constant.FAIL)
        .put(Constant.STATUS_CODE,Constant.BAD_REQUEST)
        .put(Constant.ERROR,sqlException.getMessage()).encodePrettily());
    }
    catch (Exception exception)
    {
      LOGGER.error(exception.getMessage(),exception);

      databaseHandler.fail(new JsonObject().put(Constant.STATUS,Constant.FAIL)
        .put(Constant.STATUS_CODE,Constant.INTERNAL_SERVER_ERROR)
        .put(Constant.ERROR,exception.getMessage()).encodePrettily());
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
