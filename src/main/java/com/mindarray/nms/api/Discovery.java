package com.mindarray.nms.api;

import com.mindarray.nms.Bootstrap;
import com.mindarray.nms.util.Constant;
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

   JsonObject runDiscoveryData = new JsonObject().put(Constant.DISCOVERY_ID,routingContext.pathParam(Constant.ID));

    vertx.eventBus().<JsonObject>request(Constant.INSERT_TO_DATABASE,runDiscoveryData.put(Constant.IDENTITY,Constant.RUN_DISCOVERY_DATA_COLLECT),replyMessage->{

      if(replyMessage.succeeded())
          {
            vertx.eventBus().<JsonObject>request(Constant.EVENTBUS_ADDRESS_DISCOVERY,replyMessage.result().body(),messageAsyncResult -> {

              if(messageAsyncResult.succeeded())
              {
                routingContext.response().end(messageAsyncResult.result().body().encodePrettily());
              }
              else {
                routingContext.response().end(messageAsyncResult.cause().getMessage());
              }

            });
          }
      else
      {

        LOGGER.error(replyMessage.cause().getMessage());

        routingContext.response().end(replyMessage.cause().getMessage());

      }
    });


  }

}
