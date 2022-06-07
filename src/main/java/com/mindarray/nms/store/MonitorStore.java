package com.mindarray.nms.store;

import com.mindarray.nms.util.Constant;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

public class MonitorStore implements CrudStore
{
  private static final Logger LOGGER = LoggerFactory.getLogger(MonitorStore.class);

  @Override
  public void create(JsonObject body, Promise<Object> databaseHandler)
  {
    try (Connection connection = createConnection();
         PreparedStatement preparedStatement = connection.prepareStatement(Constant.QUERY_INSERT_MONITOR)
        )
    {
      preparedStatement.setString(1,body.getString(Constant.MONITOR_NAME));
      preparedStatement.setString(2,body.getString(Constant.JSON_KEY_HOST));
      preparedStatement.setString(3,body.getString(Constant.JSON_KEY_PORT));
      preparedStatement.setString(4,body.getString(Constant.JSON_KEY_METRIC_TYPE));
      preparedStatement.setInt(5,body.getInteger(Constant.CREDENTIAL_ID));

      preparedStatement.executeUpdate();

      try(Statement statement = connection.createStatement();
          ResultSet resultSet = statement.executeQuery(Constant.QUERY_MAX_MONITOR_ID)
         )
      {
        int id = 0;
        while (resultSet.next())
        {
          id = resultSet.getInt(1);
        }

        try(ResultSet resultSet1 = connection.createStatement().executeQuery(Constant.QUERY_METRIC_LEFT_JOIN_MONITOR + id))
        {
          JsonArray array = new JsonArray();

          while (resultSet1.next())
          {
            JsonObject entries = new JsonObject();
            entries.put(Constant.MONITOR_ID, resultSet1.getInt(Constant.MONITOR_ID))
              .put(Constant.METRIC_GROUP, resultSet1.getString(Constant.METRIC_GROUP))
              .put(Constant.DEFAULT_TIME, resultSet1.getInt(Constant.METRIC_TIME))
              .put(Constant.JSON_KEY_HOST, resultSet1.getString(Constant.JSON_KEY_HOST))
              .put(Constant.JSON_KEY_PORT, resultSet1.getString(Constant.JSON_KEY_PORT))
              .put(Constant.JSON_KEY_METRIC_TYPE, resultSet1.getString(Constant.JSON_KEY_METRIC_TYPE))
              .put(Constant.CREDENTIAL_ID, resultSet1.getInt(Constant.CREDENTIAL_ID))
              .put(Constant.MONITOR_NAME, resultSet1.getString(Constant.MONITOR_NAME));

            if (resultSet1.getString(Constant.METRIC_GROUP).equals(Constant.PING))
            {
              entries.put(Constant.TIME, 0);
            }
            else
            {
              entries.put(Constant.TIME, resultSet1.getInt(Constant.METRIC_TIME));
            }

            array.add(entries);
          }
          databaseHandler.complete(array);
        }
      }
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
  public void read(JsonObject jsonObject, Promise<Object> databaseHandler)
  {
    try (Connection connection = createConnection();
         ResultSet resultSet = connection.createStatement().executeQuery("select * from monitor where `monitor.id` = " + jsonObject.getString(Constant.ID))
    )
    {
      JsonObject data = new JsonObject();

      while (resultSet.next())
      {

        data.put(Constant.MONITOR_ID,resultSet.getInt(Constant.MONITOR_ID));
        data.put(Constant.MONITOR_NAME,resultSet.getString(Constant.MONITOR_NAME));
        data.put(Constant.JSON_KEY_HOST,resultSet.getString(Constant.JSON_KEY_HOST));
        data.put(Constant.JSON_KEY_PORT,resultSet.getString(Constant.JSON_KEY_PORT));
        data.put(Constant.CREDENTIAL_ID,resultSet.getString(Constant.CREDENTIAL_ID));
        data.put(Constant.JSON_KEY_METRIC_TYPE,resultSet.getString(Constant.JSON_KEY_METRIC_TYPE));

      }
      databaseHandler.complete( new JsonObject().put(Constant.STATUS,Constant.SUCCESS).put(Constant.STATUS_CODE,Constant.OK).put(Constant.RESULT,data));
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
    try (Connection connection = createConnection();
         ResultSet resultSet = connection.createStatement().executeQuery("select * from monitor")
    )
    {
      JsonArray jsonArray = new JsonArray();

      while (resultSet.next())
      {
        JsonObject data = new JsonObject();
        data.put(Constant.MONITOR_ID,resultSet.getInt(Constant.MONITOR_ID));
        data.put(Constant.MONITOR_NAME,resultSet.getString(Constant.MONITOR_NAME));
        data.put(Constant.JSON_KEY_HOST,resultSet.getString(Constant.JSON_KEY_HOST));
        data.put(Constant.JSON_KEY_PORT,resultSet.getString(Constant.JSON_KEY_PORT));
        data.put(Constant.CREDENTIAL_ID,resultSet.getString(Constant.CREDENTIAL_ID));
        data.put(Constant.JSON_KEY_METRIC_TYPE,resultSet.getString(Constant.JSON_KEY_METRIC_TYPE));

        jsonArray.add(data);
      }
      databaseHandler.complete( new JsonObject().put(Constant.STATUS,Constant.SUCCESS).put(Constant.STATUS_CODE,Constant.OK).put(Constant.RESULT,jsonArray));
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
  public void update(JsonObject jsonObject, Promise<Object> databaseHandler)
  {
    String query="";
    if(jsonObject.containsKey(Constant.JSON_KEY_PORT))
    {
       query = "update monitor set `port` = "+jsonObject.getInteger(Constant.JSON_KEY_PORT)+" where `monitor.id` = "+jsonObject.getString(Constant.MONITOR_ID);
    }
    else if(jsonObject.containsKey(Constant.CREDENTIAL_ID))
    {
       query = "update monitor set `credential.id` = "+jsonObject.getInteger(Constant.CREDENTIAL_ID)+" where `monitor.id` = "+jsonObject.getString(Constant.MONITOR_ID);
    }

    try (Connection connection = createConnection();Statement statement = connection.createStatement())
    {
      int affectedRows = statement.executeUpdate(query);

      if(affectedRows==0)throw new SQLException("id not exist");

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
  public void delete(JsonObject jsonObject, Promise<Object> databaseHandler)
  {
    try (Connection connection = createConnection();Statement statement = connection.createStatement())
    {
      statement.executeUpdate(Constant.QUERY_DELETE_MONITOR + jsonObject.getString(Constant.ID));

      databaseHandler.complete( new JsonObject().put(Constant.STATUS,Constant.SUCCESS).put(Constant.STATUS_CODE,Constant.OK));

      LOGGER.info("MONITOR ID {} DELETED",jsonObject.getString(Constant.ID));
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

  public void discoveryCheck(JsonObject body,Promise<Object> databaseHandler)
  {
    try (Connection connection = createConnection();
         ResultSet resultSet = connection.createStatement().executeQuery("select `discovery.status`,`monitor.name`,`metric.type` from discovery where `discovery.id` = "+ body.getString(Constant.DISCOVERY_ID))
        )
    {
      while (resultSet.next())
      {
        body.put(Constant.DISCOVERY_STATUS,resultSet.getString(Constant.DISCOVERY_STATUS))
          .put(Constant.MONITOR_NAME,resultSet.getString(Constant.MONITOR_NAME))
          .put(Constant.JSON_KEY_METRIC_TYPE,resultSet.getString(Constant.JSON_KEY_METRIC_TYPE));
      }
      body.mergeIn(mergeData(body));

      if (body.getString(Constant.DISCOVERY_STATUS).equals("true"))
      {
        databaseHandler.complete(body);
      }
      else
      {
        databaseHandler.fail(body.encodePrettily());
      }
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

  private JsonObject mergeData(JsonObject jsonObject)
  {
    try (Connection connection = createConnection();
         ResultSet resultSet = connection.createStatement().executeQuery(Constant.QUERY_CREDENTIAL_LEFT_JOIN_DISCOVERY + jsonObject.getString(Constant.DISCOVERY_ID))
        )
    {
      while (resultSet.next())
      {

        jsonObject.put(Constant.CREDENTIAL_ID,resultSet.getInt(Constant.CREDENTIAL_ID));
        jsonObject.put(Constant.CREDENTIAL_NAME,resultSet.getString(Constant.CREDENTIAL_NAME));
        jsonObject.put(Constant.PROTOCOL,resultSet.getString(Constant.PROTOCOL));
        jsonObject.put(Constant.JSON_KEY_USERNAME,resultSet.getString(Constant.JSON_KEY_USERNAME));
        jsonObject.put(Constant.JSON_KEY_PASSWORD,resultSet.getString(Constant.JSON_KEY_PASSWORD));
        jsonObject.put(Constant.COMMUNITY,resultSet.getString(Constant.COMMUNITY));
        jsonObject.put(Constant.JSON_KEY_VERSION,resultSet.getString(Constant.JSON_KEY_VERSION));

        jsonObject.put(Constant.JSON_KEY_HOST,resultSet.getString(Constant.JSON_KEY_HOST));
        jsonObject.put(Constant.JSON_KEY_PORT,resultSet.getString(Constant.JSON_KEY_PORT));
        jsonObject.put(Constant.JSON_KEY_METRIC_TYPE,resultSet.getString(Constant.JSON_KEY_METRIC_TYPE));
      }
      return jsonObject;

    }
    catch (SQLException sqlException)
    {
      LOGGER.error(sqlException.getMessage(),sqlException);

      return new JsonObject().put(Constant.STATUS,Constant.FAIL)
        .put(Constant.STATUS_CODE,Constant.BAD_REQUEST)
        .put(Constant.ERROR,sqlException.getMessage());
    }
    catch (Exception exception)
    {
      LOGGER.error(exception.getMessage(),exception);

      return new JsonObject().put(Constant.STATUS,Constant.FAIL)
        .put(Constant.STATUS_CODE,Constant.INTERNAL_SERVER_ERROR)
        .put(Constant.ERROR,exception.getMessage());
    }
  }

  public void dumpInDB(JsonObject body,Promise<Object> databaseHandler)
  {
    try (Connection connection = createConnection();
         PreparedStatement preparedStatement = connection.prepareStatement(Constant.QUERY_DUMP_IN_DB)
    )
    {
      preparedStatement.setString(1,body.getString(Constant.MONITOR_ID));
      preparedStatement.setString(2,body.getString(Constant.METRIC_GROUP));
      preparedStatement.setString(3,body.getString(Constant.JSON_KEY_METRIC_TYPE));
      preparedStatement.setString(4,body.getString(Constant.DATA));
      preparedStatement.setTimestamp(5,new Timestamp(System.currentTimeMillis()));
      preparedStatement.setString(6,body.getString(Constant.MONITOR_NAME));

      preparedStatement.executeUpdate();

      databaseHandler.complete( new JsonObject().put(Constant.STATUS,Constant.SUCCESS));
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
