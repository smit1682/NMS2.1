package com.mindarray.nms.api;

import com.mindarray.nms.util.Constant;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class APIRouter extends AbstractVerticle
{

  private static final Logger LOGGER = LoggerFactory.getLogger(APIRouter.class);

  @Override
  public void start(Promise<Void> startPromise)
  {

    Router mainRouter = Router.router(vertx);

    Router subRoute = Router.router(vertx);

    subRoute.route().handler(BodyHandler.create());

    mainRouter.mountSubRouter(Constant.PATH_MOUNT_POINT,subRoute);

    new Discovery(subRoute);

    new Credential(subRoute);

    new Monitor(subRoute);

    new ReportingApi(subRoute);

    new Metric(subRoute);

    vertx.createHttpServer().requestHandler(mainRouter).listen(Constant.HTTP_PORT).onComplete(http -> {

      if (http.succeeded())
      {
        LOGGER.info("HTTP server started on port {}",Constant.HTTP_PORT);
      }
      else
      {
        LOGGER.error("HTTP server not started");
      }

    });

    startPromise.complete();

  }

}
