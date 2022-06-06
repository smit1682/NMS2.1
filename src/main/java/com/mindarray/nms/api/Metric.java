package com.mindarray.nms.api;

import com.mindarray.nms.util.Constant;
import com.mindarray.nms.util.Entity;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

public class Metric extends RestAPI
{
  private static final Logger LOGGER = LoggerFactory.getLogger(Metric.class);
  private static final Set<String> snmpMetricFields = Set.of("interface", "system", "ping");
  private static final Set<String> otherMetricFields = Set.of("cpu", "disk", "process", "memory", "system", "ping");

  public Metric(Router router)
  {
    super(router);
  }

  @Override
  protected Entity getEntity()
  {
    return Entity.METRIC;
  }

  @Override
  protected void validate(RoutingContext routingContext)
  {
    try
    {
      JsonObject rawData = routingContext.getBodyAsJson();

      if (routingContext.currentRoute().getName().equals("put"))
      {
        if (rawData == null)
        {
          routingContext.response().setStatusCode(Constant.BAD_REQUEST).end(new JsonObject().put(Constant.STATUS,Constant.FAIL).put(Constant.ERROR, Constant.NO_INPUT).put(Constant.STATUS_CODE, Constant.BAD_REQUEST).encodePrettily());
        }
        else
        {
          for (Map.Entry<String, Object> data : rawData)
          {
            if (data.getValue() instanceof String)
            {
              rawData.put(data.getKey(), ((String) data.getValue()).trim());
            }
          }
          {
              if (rawData.size() != 3 || !(rawData.containsKey(Constant.METRIC_GROUP) && rawData.containsKey(Constant.TIME) && rawData.containsKey(Constant.METRIC_TYPE_VALIDATION)))
              {
                routingContext.response().putHeader(Constant.CONTENT_TYPE,Constant.APPLICATION_JSON).setStatusCode(Constant.BAD_REQUEST).end(new JsonObject().put(Constant.STATUS,Constant.FAIL).put(Constant.ERROR, Constant.INVALID_INPUT).put(Constant.STATUS_CODE,Constant.BAD_REQUEST).encodePrettily());

                return;
              }
              if (rawData.getString(Constant.METRIC_TYPE_VALIDATION).equals(Constant.NETWORK_DEVICE))
              {
                if (snmpMetricFields.contains(rawData.getString(Constant.METRIC_GROUP)) && numberValidate(rawData.getInteger(Constant.TIME)))
                {
                  routingContext.next();
                }
                else
                {
                  routingContext.response().putHeader(Constant.CONTENT_TYPE,Constant.APPLICATION_JSON).setStatusCode(Constant.BAD_REQUEST).end(new JsonObject().put(Constant.STATUS,Constant.FAIL).put(Constant.ERROR, Constant.INVALID_INPUT).put(Constant.STATUS_CODE,Constant.BAD_REQUEST).encodePrettily());
                }
              }
              else
              {
                if (otherMetricFields.contains(rawData.getString(Constant.METRIC_GROUP)) && numberValidate(rawData.getInteger(Constant.TIME)))
                {
                  routingContext.next();
                }
                else
                {
                  routingContext.response().putHeader(Constant.CONTENT_TYPE,Constant.APPLICATION_JSON).setStatusCode(Constant.BAD_REQUEST).end(new JsonObject().put(Constant.STATUS,Constant.FAIL).put(Constant.ERROR, Constant.INVALID_INPUT).put(Constant.STATUS_CODE,Constant.BAD_REQUEST).encodePrettily());
                }
              }

          }
        }
      }
      else if(routingContext.currentRoute().getName().equals("getAll"))
      {
        routingContext.next();
      }
      else
      {
        routingContext.response().putHeader(Constant.CONTENT_TYPE,Constant.APPLICATION_JSON).setStatusCode(Constant.NOT_FOUND).end("PAGE NOT FOUND");
      }
    }
    catch (DecodeException decodeException)
    {
      LOGGER.error(decodeException.getMessage());

      routingContext.response().putHeader(Constant.CONTENT_TYPE,Constant.APPLICATION_JSON).setStatusCode(Constant.INTERNAL_SERVER_ERROR).end(new JsonObject().put(Constant.STATUS,Constant.FAIL).put(Constant.ERROR, decodeException.getMessage()).put(Constant.STATUS_CODE, Constant.INTERNAL_SERVER_ERROR).encodePrettily());
    }
    catch (ClassCastException classCastException)
    {
      LOGGER.error("Invalid value : {}",classCastException.getMessage());

      routingContext.response().putHeader(Constant.CONTENT_TYPE,Constant.APPLICATION_JSON).setStatusCode(Constant.INTERNAL_SERVER_ERROR).end(new JsonObject().put(Constant.STATUS,Constant.FAIL).put(Constant.ERROR, Constant.INVALID_INPUT).put(Constant.STATUS_CODE,Constant.BAD_REQUEST).encodePrettily());
    }
    catch (Exception exception)
    {
      LOGGER.error(exception.getMessage(),exception);

      routingContext.response().putHeader(Constant.CONTENT_TYPE,Constant.APPLICATION_JSON).setStatusCode(Constant.INTERNAL_SERVER_ERROR).end(new JsonObject().put(Constant.STATUS,Constant.FAIL).put(Constant.ERROR, exception.getMessage()).put(Constant.STATUS_CODE, Constant.INTERNAL_SERVER_ERROR).encodePrettily());
    }
  }

  private static boolean numberValidate(Integer data)
  {
    try
    {
      if (data <86400 && data >=10 && data % 10 != 0  )
      {
        return false;
      }
    }
    catch (ClassCastException exception)
    {
      return false;
    }

    return true;
  }
}
