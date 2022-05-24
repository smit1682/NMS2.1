package com.mindArray.NMS2_1;

import com.mindArray.NMS2_1.API.APIRouter;

import com.mindArray.NMS2_1.API.MyUtil;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Bootstrap {
    private static final Vertx vertex = Vertx.vertx();
  private static final Logger LOGGER = LoggerFactory.getLogger(Bootstrap.class);

 public static Vertx getVertex()
  {
    return Bootstrap.vertex;
  }

  public static void main(String[] args) {

    start(APIRouter.class.getName())

      .compose(future -> start(Database.class.getName()))

      .compose(future -> start(Scheduler.class.getName()))

      .compose(future -> start(Pulling.class.getName()))

      .compose(future -> start(DiscoveryEngine.class.getName()))

      .onComplete(result -> {

        if (result.succeeded())
        {
          //System.out.println("All Verticals are Deployed");
          LOGGER.info("All Verticals are Deployed");
          MyUtil.init();
        }
        else
        {
          //System.out.println("Error occurs while deploying Verticals");
          LOGGER.error("Error occurs while deploying Verticals");
        }

      });

  }

  public static Future<Void> start(String vertical) {

    Promise<Void> promise = Promise.promise();

    vertex.deployVerticle(vertical).onComplete(result -> {

      if (result.succeeded())
      {
        promise.complete();
      }
      else
      {
        promise.fail(result.cause());
      }

    });

    return promise.future();

  }

}
