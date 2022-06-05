package com.mindarray.nms.store;

import com.mindarray.nms.api.Metric;
import com.mindarray.nms.util.Constant;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

public class MetricStore implements CrudStore
{
  private static final Logger LOGGER = LoggerFactory.getLogger(Metric.class);
  @Override
  public void create(JsonObject jsonObject, Promise<Object> databaseHandler)
  {
    throw new UnsupportedOperationException("Metric Store Doesn't support Create Operation");
  }

  @Override
  public void read(JsonObject entries, Promise<Object> databaseHandler)
  {
    try (Connection connection = createConnection();
         ResultSet resultSet = connection.createStatement().executeQuery("select * from metric where `monitor.id` = " + entries.getString("id"));
        )
    {
      JsonArray array = new JsonArray();

      while (resultSet.next())
      {
        JsonObject data = new JsonObject();
        data.put(Constant.METRIC_ID,resultSet.getInt(Constant.METRIC_ID));
        data.put(Constant.MONITOR_ID,resultSet.getInt(Constant.MONITOR_ID));
        data.put(Constant.METRIC_GROUP,resultSet.getString(Constant.METRIC_GROUP));
        data.put(Constant.METRIC_TIME,resultSet.getString(Constant.METRIC_TIME));

        array.add(data);
      }
      databaseHandler.complete( new JsonObject().put(Constant.STATUS,Constant.SUCCESS).put(Constant.STATUS_CODE,Constant.OK).put(Constant.RESULT,array));
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
    try (Connection connection = createConnection();ResultSet resultSet = connection.createStatement().executeQuery("select * from metric"))
    {
      JsonArray readAllContainer = new JsonArray();

      while (resultSet.next())
      {
        JsonObject data = new JsonObject();
        data.put(Constant.METRIC_ID,resultSet.getInt(Constant.METRIC_ID));
        data.put(Constant.MONITOR_ID,resultSet.getString(Constant.MONITOR_ID));
        data.put(Constant.METRIC_GROUP,resultSet.getString(Constant.METRIC_GROUP));
        data.put(Constant.METRIC_TIME,resultSet.getInt(Constant.METRIC_TIME));

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
  public void update(JsonObject jsonObject, Promise<Object> databaseHandler)
  {
    try (Connection connection = createConnection(); Statement statement = connection.createStatement())
    {
      String query = "update metric set `metric.time` = "+jsonObject.getInteger(Constant.TIME)+" where `monitor.id` = "+jsonObject.getString(Constant.MONITOR_ID)+"  and `metric.group` = \"" + jsonObject.getString(Constant.METRIC_GROUP)+"\"";

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
  public void delete(JsonObject jsonObject, Promise<Object> databaseHandler) {

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
