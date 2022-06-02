package com.mindarray.nms;

import com.mindarray.nms.api.APIRouter;

import com.mindarray.nms.util.Util;
import com.mindarray.nms.discovery.DiscoveryEngine;
import com.mindarray.nms.poller.Polling;
import com.mindarray.nms.store.DataStoreHandler;
import com.mindarray.nms.poller.Scheduler;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Bootstrap
{
  private static final Vertx vertex = Vertx.vertx();

  private static final Logger LOGGER = LoggerFactory.getLogger(Bootstrap.class);

  public static Vertx getVertex()
  {
    return Bootstrap.vertex;
  }

  public static void main(String[] args)
  {
    start(APIRouter.class.getName())

      .compose(future -> start(DataStoreHandler.class.getName()))

      .compose(future -> start(Scheduler.class.getName()))

      .compose(future -> start(Polling.class.getName()))

      .compose(future -> start(DiscoveryEngine.class.getName()))

      .onComplete(result -> {

        if (result.succeeded())
        {
          LOGGER.info("All Verticals are Deployed");

          Util.init();
        }
        else
        {
          LOGGER.error("Error occurs while deploying Verticals");

          System.exit(0);
        }

      });

  }

  public static Future<Void> start(String vertical)
  {

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
