package com.mindarray.nms.api;

import com.mindarray.nms.util.Entity;
import io.vertx.ext.web.Router;

public class Credential extends RestAPI
{

  public Credential(Router router)
  {
    super(router);
  }

  @Override
  protected Entity getEntity()
  {
    return Entity.CREDENTIAL;
  }

}
