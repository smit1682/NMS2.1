package com.mindarray.nms.store;

import com.mindarray.nms.util.Constant;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

public class UtilStore
{
  private static final Logger LOGGER = LoggerFactory.getLogger(UtilStore.class);

  public  void checkId(String ip, String identity, Promise<Object> databaseHandler)
  {

    boolean output ;
    PreparedStatement preparedStatement = null;
    try (Connection connection = createConnection())
    {
      switch (identity)
      {
        case "DISCOVERY":
          preparedStatement = connection.prepareStatement("select * from discovery where `discovery.id` = ?");
          preparedStatement.setString(1,ip);
          break;
        case "CREDENTIAL":
          preparedStatement = connection.prepareStatement("select protocol from credential where `credential.id` = ?");
          preparedStatement.setString(1,ip);
          break;
        case "MONITOR":
        case "METRIC":
          preparedStatement = connection.prepareStatement("select `metric.type` from monitor where `monitor.id` = ?");
          preparedStatement.setString(1,ip);
          break;
      }
      if(preparedStatement !=null)
      {
        try (ResultSet resultSet = preparedStatement.executeQuery())
        {
          output = resultSet.next();

          if (output && identity.equals("METRIC"))
          {
            databaseHandler.complete(new JsonObject().put(Constant.METRIC_TYPE_VALIDATION, resultSet.getString(Constant.JSON_KEY_METRIC_TYPE)));
          }
          else if (output && identity.equals("CREDENTIAL"))
          {
            databaseHandler.complete(new JsonObject().put(Constant.PROTOCOL_VALIDATION, resultSet.getString(Constant.PROTOCOL)));
          }
          else if (output)
          {
            databaseHandler.complete(new JsonObject().put(Constant.STATUS, Constant.DISCOVERED).put(Constant.STATUS_CODE, Constant.OK));
          }
          else
          {
            databaseHandler.fail(new JsonObject().put(Constant.STATUS, Constant.NOT_DISCOVERED).put(Constant.STATUS_CODE, Constant.OK).encodePrettily());
          }
        }
      }
      else
      {
        databaseHandler.fail( new JsonObject().put(Constant.STATUS,Constant.FAIL).put(Constant.STATUS_CODE,Constant.INTERNAL_SERVER_ERROR).put(Constant.ERROR,"null").encodePrettily());
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
    finally
    {
      try
      {
        if(preparedStatement != null)
          preparedStatement.close();
      }
      catch (Exception exception)
      {
        LOGGER.error(exception.getMessage(),exception);

        databaseHandler.fail(new JsonObject().put(Constant.STATUS,Constant.FAIL)
          .put(Constant.STATUS_CODE,Constant.INTERNAL_SERVER_ERROR)
          .put(Constant.ERROR,exception.getMessage()).encodePrettily());
      }
    }
  }

  public void getLastInstance(JsonObject jsonMessage,Promise<Object> databaseHandler)
  {
    try(Connection connection = createConnection();
        PreparedStatement preparedStatement = connection.prepareStatement("select `monitor.id`,`monitor.name`,`metric.group`,`data`,`timestamp` from metric_store  where `metric.group` =  ?  AND  `monitor.id` = ? order by `metric.store.id` DESC limit 1;")
       )
    {
      preparedStatement.setString(1,jsonMessage.getString(Constant.METRIC_GROUP));
      preparedStatement.setString(2,jsonMessage.getString(Constant.ID));

      ResultSet resultSet = preparedStatement.executeQuery();
      JsonObject lastData = new JsonObject();
      while (resultSet.next())
      {
        lastData.put(Constant.MONITOR_ID,resultSet.getString("monitor.id"));
        lastData.put(Constant.MONITOR_NAME,resultSet.getString("monitor.name"));
        lastData.put(Constant.DATA,resultSet.getObject("data"));
        lastData.put(Constant.TIME_STAMP,resultSet.getString("timestamp"));
      }

      databaseHandler.complete( new JsonObject().put(Constant.STATUS,Constant.SUCCESS).put(Constant.STATUS_CODE,Constant.OK).put(Constant.RESULT,lastData));
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

  public void topFive(JsonObject jsonMessage,Promise<Object> databaseHandler)
  {
    String query = "";

    if(jsonMessage.getString(Constant.METRIC_GROUP).equals(Constant.CPU))
    {
      query = "select  `monitor.name`,max(`data` -> '$.\"cpu.all.user.percentage\"') AS `cpu.all.user.percentage` from metric_store group by `monitor.name`  order by `cpu.all.user.percentage`*1 DESC limit 5";
    }
    else if(jsonMessage.getString(Constant.METRIC_GROUP).equals(Constant.MEMORY))
    {
      query = "select  `monitor.name`,max(`data` -> '$.\"memory.free.bytes\"') AS `free.memory.bytes`  from metric_store group by `monitor.name` order by `free.memory.bytes`*1 DESC limit 5 ";
    }
    else
    {
      databaseHandler.fail( new JsonObject().put(Constant.STATUS,Constant.FAIL).put(Constant.STATUS_CODE,Constant.BAD_REQUEST).put(Constant.ERROR,Constant.INVALID_METRIC_GROUP).encodePrettily());
    }

    try (Connection connection = createConnection();
         ResultSet resultSet = connection.createStatement().executeQuery(query))
    {
      JsonArray topFiveContainer = new JsonArray();

      while (resultSet.next())
      {
        JsonObject data = new JsonObject();

        data.put("monitor.name",resultSet.getString("monitor.name"));
        data.put(resultSet.getMetaData().getColumnName(2),resultSet.getString(resultSet.getMetaData().getColumnName(2)));
        topFiveContainer.add(data);
      }

      databaseHandler.complete( new JsonObject().put(Constant.STATUS,Constant.SUCCESS).put(Constant.STATUS_CODE,Constant.OK).put(Constant.RESULT,topFiveContainer));
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
