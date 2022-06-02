package com.mindarray.nms.api;

import com.mindarray.nms.util.Entity;
import io.vertx.ext.web.Router;

public class Metric extends RestAPI{

  public Metric(Router router)
  {
    super(router);
  }

  @Override
  protected Entity getEntity()
  {
    return Entity.METRIC;
  }

}
