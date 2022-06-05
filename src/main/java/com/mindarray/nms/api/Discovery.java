package com.mindarray.nms.api;

import com.mindarray.nms.Bootstrap;
import com.mindarray.nms.util.Constant;
import com.mindarray.nms.util.Entity;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Discovery extends RestAPI
{
  private static final Logger LOGGER = LoggerFactory.getLogger(Discovery.class);

  private final Vertx vertx = Bootstrap.getVertex();

  public Discovery(Router router)
  {
    super(router);

    router.post(Constant.PATH_DISCOVERY_WITH_ID).setName("RunDiscovery").handler(this::validateId).handler(this::createApi);
  }

  @Override
  protected Entity getEntity() {
    return Entity.DISCOVERY;
  }

  private void createApi(RoutingContext routingContext)
  {
    JsonObject runDiscoveryData = new JsonObject().put(Constant.DISCOVERY_ID, routingContext.pathParam(Constant.ID));

    vertx.eventBus().<JsonObject>request(Constant.DATABASE_HANDLER, runDiscoveryData.put(Constant.IDENTITY, Constant.RUN_DISCOVERY_DATA_COLLECT), replyMessage -> {

      try
      {
        if (replyMessage.succeeded())
        {
          vertx.eventBus().<JsonObject>request(Constant.EVENTBUS_ADDRESS_DISCOVERY, replyMessage.result().body(), messageAsyncResult -> {

            if (messageAsyncResult.succeeded())
            {
              routingContext.response().putHeader(Constant.CONTENT_TYPE,Constant.APPLICATION_JSON).end(messageAsyncResult.result().body().encodePrettily());
            }
            else
            {
              routingContext.response().putHeader(Constant.CONTENT_TYPE,Constant.APPLICATION_JSON).end(messageAsyncResult.cause().getMessage());
            }
          });
        }
        else
        {
          LOGGER.error(replyMessage.cause().getMessage());

          routingContext.response().putHeader(Constant.CONTENT_TYPE,Constant.APPLICATION_JSON).end(replyMessage.cause().getMessage());
        }
      }
      catch (Exception exception)
      {
        LOGGER.error(exception.getMessage(),exception);

        routingContext.response().putHeader(Constant.CONTENT_TYPE,Constant.APPLICATION_JSON).setStatusCode(Constant.INTERNAL_SERVER_ERROR).end(exception.getMessage());
      }
      });


  }

}
