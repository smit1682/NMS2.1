package com.mindArray.NMS2_1.API;

import com.mindArray.NMS2_1.Bootstrap;
import com.mindArray.NMS2_1.Constant;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;

public class FetchApi {
  private final Vertx vertx = Bootstrap.getVertex();

  public FetchApi(Router router){

    router.get("/metric/topFive/1").handler(this::getCpu);
    router.get("/metric/topFive/2").handler(this::getMemory);

    router.get("/metric/:metricGroup/:id").handler(this::getLastInstance);
  }

  private void getLastInstance(RoutingContext routingContext) {
    vertx.eventBus().<JsonObject>request(Constant.INSERT_TO_DATABASE,new JsonObject().put(Constant.IDENTITY,Constant.GET_LAST_INSTANCE).put(Constant.JSON_KEY_METRIC_GROUP,routingContext.pathParam("metricGroup")).put(Constant.ID,routingContext.pathParam("id")),replyMessage->{
      if(replyMessage.succeeded())
        routingContext.response().end(replyMessage.result().body().encodePrettily());
    });
  }

  private void getMemory(RoutingContext routingContext) {
    vertx.eventBus().<JsonObject>request(Constant.INSERT_TO_DATABASE,new JsonObject().put(Constant.IDENTITY,Constant.TOP_FIVE_MEMORY),replyHandler->{
      if(replyHandler.succeeded())
      {
        routingContext.response().end(replyHandler.result().body().getJsonArray("data").encode());
      }
    });
  }

  private void getCpu(RoutingContext routingContext) {
    vertx.eventBus().<JsonObject>request(Constant.INSERT_TO_DATABASE,new JsonObject().put(Constant.IDENTITY,Constant.TOP_FIVE_CPU), replyHandler->{
      if(replyHandler.succeeded())
      {
        routingContext.response().end(replyHandler.result().body().getJsonArray("data").encode());
      }
    });
  }

  //private void getMemory

 /* private void getData(RoutingContext routingContext) {
    routingContext.pathParam("id");
    vertx.eventBus().request("fetchData",)
  }*/
}
