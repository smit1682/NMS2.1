package com.mindarray.nms.api;

import io.vertx.ext.web.Router;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Credential extends RestAPI
{
  private static final Logger LOGGER = LoggerFactory.getLogger(Credential.class);

  public Credential(Router router) {
    super(router);
  }

  @Override
  protected Entity getEntity() {
    return Entity.CREDENTIAL;
  }
}
