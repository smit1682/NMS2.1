package com.mindArray.NMS2_1.API;

import com.mindArray.NMS2_1.Constant;
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

    Router credentialRoute = Router.router(vertx);
    Router discoveryRoute = Router.router(vertx);
    Router monitorRoute = Router.router(vertx);
    Router fetchRoute = Router.router(vertx);


    credentialRoute.route().handler(BodyHandler.create());
    discoveryRoute.route().handler(BodyHandler.create());
    monitorRoute.route().handler(BodyHandler.create());
    fetchRoute.route().handler(BodyHandler.create());

    mainRouter.mountSubRouter("/api/v1",credentialRoute);
    mainRouter.mountSubRouter("/api/v1",discoveryRoute);
    mainRouter.mountSubRouter("/api/v1",monitorRoute);
    mainRouter.mountSubRouter("/api/v1",fetchRoute);


    Discovery discovery = new Discovery(discoveryRoute);
    Credential credential = new Credential(credentialRoute);
    Monitor monitor = new Monitor(mainRouter);
    FetchApi fetchApi = new FetchApi(fetchRoute);





    vertx.createHttpServer().requestHandler(mainRouter).listen(Constant.HTTP_PORT).onComplete(http -> {

      if (http.succeeded()) {

        System.out.println("HTTP server started on port 8888");

      } else {

        startPromise.fail(http.cause());

      }

    });

    startPromise.complete();
  }

}
