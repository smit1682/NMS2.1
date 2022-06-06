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

public class Credential extends RestAPI
{
  private static final Logger LOGGER = LoggerFactory.getLogger(Credential.class);

  private static final Set<String> credentialFields = Set.of("credential.name", "protocol", "username", "password", "community", "version", "protocolValidation");

  public Credential(Router router)
  {
    super(router);
  }

  @Override
  protected Entity getEntity()
  {
    return Entity.CREDENTIAL;
  }

  @Override
  protected void validate(RoutingContext routingContext)
  {
    try
    {
      JsonObject rawData = routingContext.getBodyAsJson();

      if (routingContext.currentRoute().getName().equals("post"))
      {
        if (rawData == null)
        {
          routingContext.response().putHeader(Constant.CONTENT_TYPE,Constant.APPLICATION_JSON).setStatusCode(Constant.BAD_REQUEST).end(new JsonObject().put(Constant.STATUS,Constant.FAIL).put(Constant.ERROR, Constant.NO_INPUT).put(Constant.STATUS_CODE, Constant.BAD_REQUEST).encodePrettily());
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
          routingContext.setBody(rawData.toBuffer());

          {
              if (!rawData.containsKey(Constant.CREDENTIAL_NAME) || !rawData.containsKey(Constant.PROTOCOL))
              {   //contains validation
                routingContext.response().putHeader(Constant.CONTENT_TYPE,Constant.APPLICATION_JSON).setStatusCode(Constant.BAD_REQUEST).end(new JsonObject().put(Constant.STATUS,Constant.FAIL).put(Constant.ERROR, Constant.INVALID_INPUT).put(Constant.STATUS_CODE,Constant.BAD_REQUEST).encodePrettily());
              }
              else if (rawData.getString(Constant.CREDENTIAL_NAME).isEmpty() || rawData.getString(Constant.PROTOCOL).isEmpty())
              {   //isEmpty validation
                routingContext.response().putHeader(Constant.CONTENT_TYPE,Constant.APPLICATION_JSON).setStatusCode(Constant.BAD_REQUEST).end(new JsonObject().put(Constant.STATUS,Constant.FAIL).put(Constant.ERROR, Constant.INVALID_INPUT).put(Constant.STATUS_CODE,Constant.BAD_REQUEST).encodePrettily());
              }
              else if (rawData.getString(Constant.PROTOCOL).equalsIgnoreCase(Constant.SNMP))
              { //protocol validation
                if (rawData.containsKey(Constant.JSON_KEY_VERSION) && rawData.containsKey(Constant.COMMUNITY))
                {
                  routingContext.next();
                }
                else
                {
                  routingContext.response().putHeader(Constant.CONTENT_TYPE,Constant.APPLICATION_JSON).setStatusCode(Constant.BAD_REQUEST).end(new JsonObject().put(Constant.STATUS,Constant.FAIL).put(Constant.ERROR, Constant.INVALID_INPUT).put(Constant.STATUS_CODE,Constant.BAD_REQUEST).encodePrettily());
                }
              }
              else if (!rawData.getString(Constant.PROTOCOL).equals(Constant.SNMP))
              {
                if (rawData.containsKey(Constant.JSON_KEY_USERNAME) && !(rawData.getString(Constant.JSON_KEY_USERNAME).isEmpty()) && rawData.containsKey(Constant.JSON_KEY_PASSWORD) && !rawData.getString(Constant.JSON_KEY_PASSWORD).isEmpty())
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
                routingContext.next();
              }

          }
        }
      }
      else if (routingContext.currentRoute().getName().equals("put"))
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
              if(rawData.size()==1)
              {
                routingContext.response().putHeader(Constant.CONTENT_TYPE,Constant.APPLICATION_JSON).setStatusCode(Constant.BAD_REQUEST).end(new JsonObject().put(Constant.STATUS,Constant.FAIL).put(Constant.STATUS_CODE,Constant.BAD_REQUEST).put(Constant.ERROR, Constant.NO_INPUT).encodePrettily());
                return;
              }
              System.out.println("raw data: " + rawData);
              for (Map.Entry<String, Object> data : rawData)
              {

                if (data.getValue() instanceof String) {
                  rawData.put(data.getKey(), ((String) data.getValue()).trim());
                }
                if (!credentialFields.contains(data.getKey()))
                {
                  routingContext.response().putHeader(Constant.CONTENT_TYPE,Constant.APPLICATION_JSON).setStatusCode(Constant.BAD_REQUEST).end(new JsonObject().put(Constant.STATUS,Constant.FAIL).put(Constant.STATUS_CODE,Constant.BAD_REQUEST).put(Constant.ERROR, Constant.REMOVE_EXTRA_FIELD).encodePrettily());
                  return;
                }

              }

              if (rawData.containsKey(Constant.CREDENTIAL_ID) || rawData.containsKey(Constant.PROTOCOL))
              {
                routingContext.response().putHeader(Constant.CONTENT_TYPE,Constant.APPLICATION_JSON).setStatusCode(Constant.BAD_REQUEST).end(new JsonObject().put(Constant.STATUS,Constant.FAIL).put(Constant.ERROR, Constant.INVALID_INPUT).put(Constant.STATUS_CODE,Constant.BAD_REQUEST).encodePrettily());
                return;
              }
              if (rawData.getString(Constant.PROTOCOL_VALIDATION).equalsIgnoreCase(Constant.SNMP))
              {
                if (rawData.containsKey(Constant.JSON_KEY_PASSWORD) || rawData.containsKey(Constant.JSON_KEY_USERNAME))
                {
                  routingContext.response().putHeader(Constant.CONTENT_TYPE,Constant.APPLICATION_JSON).setStatusCode(Constant.BAD_REQUEST).end(new JsonObject().put(Constant.STATUS,Constant.FAIL).put(Constant.ERROR, Constant.INVALID_INPUT).put(Constant.STATUS_CODE,Constant.BAD_REQUEST).encodePrettily());

                }
                else
                {
                  rawData.remove(Constant.PROTOCOL_VALIDATION);

                  routingContext.setBody(rawData.toBuffer());

                  routingContext.next();
                }
              }
              else
              {
                if (rawData.containsKey(Constant.JSON_KEY_VERSION) || rawData.containsKey(Constant.COMMUNITY))
                {
                  routingContext.response().putHeader(Constant.CONTENT_TYPE,Constant.APPLICATION_JSON).setStatusCode(Constant.BAD_REQUEST).end(new JsonObject().put(Constant.STATUS,Constant.FAIL).put(Constant.ERROR, Constant.INVALID_INPUT).put(Constant.STATUS_CODE,Constant.BAD_REQUEST).encodePrettily());
                }
                else
                {
                  rawData.remove(Constant.PROTOCOL_VALIDATION);

                  routingContext.setBody(rawData.toBuffer());

                  routingContext.next();
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

      routingContext.response().putHeader(Constant.CONTENT_TYPE,Constant.APPLICATION_JSON).setStatusCode(Constant.BAD_REQUEST).end(new JsonObject().put(Constant.STATUS,Constant.FAIL).put(Constant.ERROR, decodeException.getMessage()).put(Constant.STATUS_CODE, Constant.BAD_REQUEST).encodePrettily());
    }
    catch (ClassCastException classCastException)
    {
      LOGGER.error("Invalid value : {}",classCastException.getMessage());

      routingContext.response().putHeader(Constant.CONTENT_TYPE,Constant.APPLICATION_JSON).setStatusCode(Constant.BAD_REQUEST).end(new JsonObject().put(Constant.STATUS,Constant.FAIL).put(Constant.ERROR, Constant.INVALID_INPUT).put(Constant.STATUS_CODE,Constant.BAD_REQUEST).encodePrettily());
    }
    catch (Exception exception)
    {
      LOGGER.error(exception.getMessage(),exception);

      routingContext.response().putHeader(Constant.CONTENT_TYPE,Constant.APPLICATION_JSON).setStatusCode(Constant.INTERNAL_SERVER_ERROR).end(new JsonObject().put(Constant.STATUS,Constant.FAIL).put(Constant.ERROR, exception.getMessage()).put(Constant.STATUS_CODE, Constant.INTERNAL_SERVER_ERROR).encodePrettily());
    }
  }
}
