package com.mindarray.nms.store;

import com.mindarray.nms.Bootstrap;
import com.mindarray.nms.util.Constant;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Map;

public class CredentialStore implements CrudStore {
  private static final Logger LOGGER = LoggerFactory.getLogger(CredentialStore.class);

  @Override
  public void create(JsonObject entries, Promise<Object> databaseHandler) {


    try (Connection connection = createConnection();PreparedStatement preparedStatement = connection.prepareStatement(Constant.QUERY_CREDENTIAL_INSERT))
    {

      preparedStatement.setString(1,entries.getString(Constant.CREDENTIAL_NAME));
      preparedStatement.setString(2,entries.getString(Constant.PROTOCOL));
      preparedStatement.setString(3,entries.getString(Constant.JSON_KEY_USERNAME));
      preparedStatement.setString(4,entries.getString(Constant.JSON_KEY_PASSWORD));
      preparedStatement.setString(5,entries.getString(Constant.JSON_KEY_VERSION));
      preparedStatement.setString(6,entries.getString(Constant.COMMUNITY));

      preparedStatement.executeUpdate();

      int id = 0;
      try(ResultSet resultSet = connection.createStatement().executeQuery(Constant.QUERY_CREDENTIAL_ID)) {

        while (resultSet.next()) {
          id = resultSet.getInt(1);
        }
      }
      databaseHandler.complete(new JsonObject().put(Constant.STATUS,Constant.SUCCESS).put(Constant.MESSAGE,id));

      Bootstrap.getVertex().eventBus().send(Constant.STORE_INITIAL_MAP,entries.put(Constant.CREDENTIAL_ID,id));  // notification for credential Cache in Scheduler


    }
    catch (Exception e)
    {

      LOGGER.error(e.getMessage());

      databaseHandler.fail(new JsonObject().put(Constant.STATUS,Constant.ERROR).put(Constant.STATUS_CODE,Constant.INTERNAL_SERVER_ERROR).put(Constant.ERROR,e.getMessage()).encodePrettily());

    }



  }

  @Override
  public void read(JsonObject entries,Promise<Object> databaseHandler)
  {

    try (Connection connection = createConnection();ResultSet resultSet = connection.createStatement().executeQuery(Constant.QUERY_CREDENTIAL_READ + entries.getString("id")))
    {

      JsonObject data = new JsonObject();

      while (resultSet.next())
      {

        data.put(Constant.CREDENTIAL_ID,resultSet.getInt(Constant.CREDENTIAL_ID));
        data.put(Constant.CREDENTIAL_NAME,resultSet.getString(Constant.CREDENTIAL_NAME));
        data.put(Constant.PROTOCOL,resultSet.getString(Constant.PROTOCOL));
        data.put(Constant.JSON_KEY_USERNAME,resultSet.getString(Constant.JSON_KEY_USERNAME));
        data.put(Constant.JSON_KEY_PASSWORD,resultSet.getString(Constant.JSON_KEY_PASSWORD));
        data.put(Constant.JSON_KEY_VERSION,resultSet.getString(Constant.JSON_KEY_VERSION));
        data.put(Constant.COMMUNITY,resultSet.getString(Constant.COMMUNITY));

      }


      databaseHandler.complete( data );

    }
    catch (Exception exception)
    {

      LOGGER.error(exception.getMessage());

      databaseHandler.fail( new JsonObject().put(Constant.STATUS,Constant.ERROR).put(Constant.STATUS_CODE,Constant.INTERNAL_SERVER_ERROR).put(Constant.ERROR,exception.getMessage()).encodePrettily());

    }
  }

  @Override
  public void readAll(JsonObject entries,Promise<Object> databaseHandler)
  {

    try (Connection connection = createConnection();ResultSet resultSet = connection.createStatement().executeQuery(Constant.QUERY_CREDENTIAL_READ_ALL))
    {

      JsonArray credentialData = new JsonArray();

      while (resultSet.next())
      {

        JsonObject data = new JsonObject();
        data.put(Constant.CREDENTIAL_ID,resultSet.getInt(Constant.CREDENTIAL_ID));
        data.put(Constant.CREDENTIAL_NAME,resultSet.getString(Constant.CREDENTIAL_NAME));
        data.put(Constant.PROTOCOL,resultSet.getString(Constant.PROTOCOL));
        data.put(Constant.JSON_KEY_USERNAME,resultSet.getString(Constant.JSON_KEY_USERNAME));
        data.put(Constant.JSON_KEY_PASSWORD,resultSet.getString(Constant.JSON_KEY_PASSWORD));
        data.put(Constant.JSON_KEY_VERSION,resultSet.getString(Constant.JSON_KEY_VERSION));
        data.put(Constant.COMMUNITY,resultSet.getString(Constant.COMMUNITY));

        credentialData.add(data);

      }

      databaseHandler.complete( new JsonObject().put("credential",credentialData));

    }
    catch (Exception exception)
    {

      LOGGER.error(exception.getMessage());

      databaseHandler.fail(new JsonObject().put(Constant.STATUS,Constant.ERROR).put(Constant.STATUS_CODE,Constant.INTERNAL_SERVER_ERROR).put(Constant.ERROR,exception.getMessage()).encodePrettily());

    }

  }

  @Override
  public void update(JsonObject entries,Promise<Object> databaseHandler)
  {

    JsonObject credentialData = new JsonObject();

    try (Connection connection = createConnection();
          PreparedStatement preparedStatement = connection.prepareStatement("select * from credential where `credential.id` = ?"))
    {

      StringBuilder queryInit = new StringBuilder();

      for(Map.Entry<String,Object> data : entries)
      {
        queryInit.append("`").append(data.getKey()).append("`").append(" = ").append("\"").append(data.getValue()).append("\"").append(",");
      }

      queryInit.deleteCharAt(queryInit.length()-1);

      String query = "UPDATE credential SET " + queryInit + " WHERE `credential.id` = " + entries.getString(Constant.CREDENTIAL_ID);

      int affectedRows = connection.createStatement().executeUpdate(query);

      if(affectedRows==0)throw new SQLException("id not exist");

      preparedStatement.setString(1,entries.getString(Constant.CREDENTIAL_ID));

      try(ResultSet resultSet = preparedStatement.executeQuery()) {

        while (resultSet.next()) {

          credentialData.put(Constant.CREDENTIAL_ID, resultSet.getInt(Constant.CREDENTIAL_ID));
          credentialData.put(Constant.CREDENTIAL_NAME, resultSet.getString(Constant.CREDENTIAL_NAME));
          credentialData.put(Constant.PROTOCOL, resultSet.getString(Constant.PROTOCOL));
          credentialData.put(Constant.JSON_KEY_USERNAME, resultSet.getString(Constant.JSON_KEY_USERNAME));
          credentialData.put(Constant.JSON_KEY_PASSWORD, resultSet.getString(Constant.JSON_KEY_PASSWORD));
          credentialData.put(Constant.JSON_KEY_VERSION, resultSet.getString(Constant.JSON_KEY_VERSION));
          credentialData.put(Constant.COMMUNITY, resultSet.getString(Constant.COMMUNITY));

        }
      }
      Bootstrap.getVertex().eventBus().send(Constant.STORE_INITIAL_MAP,credentialData); // notification for credential Cache in Scheduler

      databaseHandler.complete( credentialData.put(Constant.STATUS,Constant.SUCCESS).put(Constant.STATUS_CODE,Constant.OK));

    }
    catch (SQLException exception)
    {

      LOGGER.error(exception.getMessage());

      databaseHandler.fail( new JsonObject().put(Constant.STATUS,Constant.ERROR).put(Constant.STATUS_CODE,Constant.INTERNAL_SERVER_ERROR).put(Constant.ERROR,exception.getMessage()).encodePrettily());

    }



  }

  @Override
  public void delete(JsonObject entries,Promise<Object> databaseHandler)
  {

    try (Connection connection = createConnection();Statement statement = connection.createStatement())
    {


      statement.executeUpdate("delete from credential where `credential.id` = "+entries.getString("id"));

      databaseHandler.complete( new JsonObject().put(Constant.STATUS,Constant.SUCCESS).put(Constant.STATUS_CODE,Constant.OK));

    }
    catch (Exception exception)
    {

      LOGGER.error(exception.getMessage());

      databaseHandler.fail( new JsonObject().put(Constant.STATUS,Constant.ERROR).put(Constant.STATUS_CODE,Constant.INTERNAL_SERVER_ERROR).put(Constant.ERROR,exception.getMessage()).encodePrettily());

    }
  }

  public void intialRead(Promise<Object> databaseHandler)
  {

    try (Connection connection = createConnection();
         ResultSet resultSet = connection.createStatement().executeQuery("select * from metric left join monitor on metric.`monitor.id` = monitor.`monitor.id`"))
    {


      JsonArray array = new JsonArray();
      while (resultSet.next()){
        JsonObject data = new JsonObject();
        data.put(Constant.MONITOR_ID,resultSet.getInt(Constant.MONITOR_ID))
          .put(Constant.METRIC_GROUP,resultSet.getString(Constant.METRIC_GROUP))
          .put(Constant.DEFAULT_TIME,resultSet.getInt(Constant.METRIC_TIME))
          .put(Constant.JSON_KEY_HOST,resultSet.getString(Constant.JSON_KEY_HOST))
          .put(Constant.JSON_KEY_PORT,resultSet.getString(Constant.JSON_KEY_PORT))
          .put(Constant.JSON_KEY_METRIC_TYPE,resultSet.getString(Constant.JSON_KEY_METRIC_TYPE))
          .put(Constant.CREDENTIAL_ID,resultSet.getInt(Constant.CREDENTIAL_ID))
          .put(Constant.MONITOR_NAME,resultSet.getString(Constant.MONITOR_NAME));

        if(resultSet.getString(Constant.METRIC_GROUP).equals(Constant.PING))
        {
          data.put(Constant.TIME,0);
        }
        else
        {
          data.put(Constant.TIME,resultSet.getInt(Constant.METRIC_TIME));
        }

        array.add(data);

      }

        databaseHandler.complete(array);

    }
    catch (Exception exception)
    {

      LOGGER.error(exception.getMessage());

      databaseHandler.fail( new JsonObject().put(Constant.STATUS,Constant.ERROR).put(Constant.STATUS_CODE,Constant.INTERNAL_SERVER_ERROR).put(Constant.ERROR,exception.getMessage()).encodePrettily());

    }
  }

  private static Connection createConnection() throws SQLException
  {
    try
    {
      return DriverManager.getConnection(Constant.DATABASE_CONNECTION_URL, Constant.DATABASE_CONNECTION_USER, Constant.DATABASE_CONNECTION_PASSWORD);
    }
    catch (SQLException sqlException)
    {
      throw new SQLException(Constant.CONNECTION_REFUSED);
    }

  }


}
