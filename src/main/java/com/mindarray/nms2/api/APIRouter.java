package com.mindarray.nms2.api;

import com.mindarray.nms2.util.Constant;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class APIRouter extends AbstractVerticle {
  private static final Logger LOGGER = LoggerFactory.getLogger(APIRouter.class);

  @Override
  public void start(Promise<Void> startPromise) {

    Router mainRouter = Router.router(vertx);

    Router route = Router.router(vertx);

    route.route().handler(BodyHandler.create());

    mainRouter.mountSubRouter("/api/v1",route);

    new Discovery(route);

    new Credential(route);

    new Monitor(route);

    new FetchApi(route);

    vertx.createHttpServer().requestHandler(mainRouter).listen(Constant.HTTP_PORT).onComplete(http -> {

      if (http.succeeded()) {

        LOGGER.info("HTTP server started on port 8888");

      } else {

        startPromise.fail(http.cause());

      }

    });

    startPromise.complete();

  }

}
