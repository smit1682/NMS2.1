package com.mindarray.nms.store;

import com.mindarray.nms.api.Metric;
import com.mindarray.nms.util.Constant;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

public class MetricStore implements CrudStore{
  private static final Logger LOGGER = LoggerFactory.getLogger(Metric.class);
  @Override
  public void create(JsonObject jsonObject, Promise<Object> databaseHandler) {

  }

  @Override
  public void read(JsonObject jsonObject, Promise<Object> databaseHandler) {


    try (Connection connection = createConnection()){
      ResultSet resultSet = connection.createStatement().executeQuery("select * from metric where `monitor.id` = " + jsonObject.getString("id"));
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
      databaseHandler.complete( array);
    }
    catch (Exception exception)
    {

      LOGGER.error(exception.getMessage());

      databaseHandler.fail( new JsonObject().put(Constant.STATUS,Constant.ERROR).put(Constant.STATUS_CODE,Constant.INTERNAL_SERVER_ERROR).put(Constant.ERROR,exception.getMessage()).encodePrettily());

    }


  }

  @Override
  public void readAll(JsonObject jsonObject, Promise<Object> databaseHandler) {

  }

  @Override
  public void update(JsonObject jsonObject, Promise<Object> databaseHandler) {

    try (Connection connection = createConnection()) {
      System.out.println("In store: " + jsonObject);

      String query = "update metric set `metric.time` = "+jsonObject.getInteger(Constant.TIME)+" where `monitor.id` = "+jsonObject.getString(Constant.MONITOR_ID)+"  and `metric.group` = \"" + jsonObject.getString(Constant.METRIC_GROUP)+"\"";
      System.out.println(query);
      int affectedRows = connection.createStatement().executeUpdate(query);
      System.out.println("af--"+ affectedRows);
      if(affectedRows==0)throw new SQLException("id not exist");
      /*PreparedStatement preparedStatement = connection.prepareStatement("update metric set `metric.time` = ? where `monitor.id` = ? and `metric.group` = ? ");
      preparedStatement.setInt(1, jsonObject.getInteger(Constant.TIME));
      preparedStatement.setString(2, jsonObject.getString(Constant.MONITOR_ID));
      preparedStatement.setString(3, jsonObject.getString(Constant.METRIC_GROUP));

       preparedStatement.executeUpdate();*/

    }
    catch (Exception exception)
    {
      databaseHandler.fail( new JsonObject().put(Constant.STATUS,Constant.ERROR).put(Constant.STATUS_CODE,Constant.INTERNAL_SERVER_ERROR).put(Constant.ERROR,exception.getMessage()).encodePrettily());
    }
    databaseHandler.complete( new JsonObject().put(Constant.STATUS,Constant.SUCCESS).put(Constant.STATUS_CODE,Constant.OK));



          /*StringBuilder queryInit = new StringBuilder();
      String monitorId = jsonObject.getString("monitor.id");
      jsonObject.remove("monitor.id");
      for(Map.Entry<String,Object> data : jsonObject)
      {
        //queryInit.append("`").append(data.getKey()).append("`").append(" = ").append("\"").append(data.getValue()).append("\"").append(",");
        queryInit.append("when `metric.group` = \"").append(data.getKey()).append("\" then ").append(data.getValue()).append(" ");

      }
      queryInit.deleteCharAt(queryInit.length()-1);
      String query = "update metric set `metric.time` = case " + queryInit + " else `metric.time`  end where `monitor.id` = " + monitorId;
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
*/
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
