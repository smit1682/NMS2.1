package com.mindArray.NMS2_1.API;

import com.mindArray.NMS2_1.Bootstrap;
import com.mindArray.NMS2_1.Constant;
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
  public Discovery(Router router) {

    super(router);

    router.post("/discovery/:id").setName("runDiscovery").handler(this::validate).handler(this::createApi);

  }

  @Override
  protected Entity getEntity() {
    return Entity.DISCOVERY;
  }

  @Override
  public void validate(RoutingContext routingContext) {
    MyUtil.validate(routingContext,getEntity());
  }

  private void createApi(RoutingContext routingContext) {

   JsonObject runDiscoveryData = new JsonObject().put(Constant.DISCOVERY_ID,routingContext.pathParam(Constant.ID));

    vertx.eventBus().<JsonObject>request(Constant.EA_RUN_DISCOVERY_DATA_COLLECT,runDiscoveryData,replyMessage->{

      if(replyMessage.succeeded())
          {
            vertx.eventBus().<JsonObject>request(Constant.EVENTBUS_ADDRESS_DISCOVERY,replyMessage.result().body(),messageAsyncResult -> {

              if(messageAsyncResult.succeeded())
              {
                routingContext.response().end(messageAsyncResult.result().body().toString());
              }
              else {
                routingContext.response().end(messageAsyncResult.cause().getMessage());
              }

            });
          }
      else
      {
        routingContext.response().end(replyMessage.cause().getMessage());
      }
    });



  }


}
