package com.mindarray.nms.api;

import com.mindarray.nms.Bootstrap;
import com.mindarray.nms.util.Constant;
import com.mindarray.nms.util.Entity;
import com.mindarray.nms.util.Util;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class RestAPI
{
  private final Vertx vertx = Bootstrap.getVertex();

  private static final Logger LOGGER = LoggerFactory.getLogger(RestAPI.class);

  protected RestAPI() {}

  protected RestAPI(Router router)
  {
    router.post(getEntity().getPath()).setName("post").handler(this::validate).handler(this::create);

    router.put(getEntity().getPath() + "/:id").setName("put").handler(this::validateId).handler(this::validate).handler(this::update);

    router.get(getEntity().getPath()).setName("getAll").handler(this::validate).handler(this::readAll);

    router.delete(getEntity().getPath() + "/:id").setName("delete").handler(this::validateId).handler(this::delete);

    router.get(getEntity().getPath() + "/:id").setName("get").handler(this::validateId).handler(this::read);
  }

  protected abstract Entity getEntity();

  protected abstract void validate(RoutingContext routingContext);

   public void validateId(RoutingContext routingContext)
   {
     Util.validateId(routingContext.pathParam(Constant.ID), getEntity()).onComplete(result -> {

       try
       {
         if (result.succeeded() && Entity.METRIC.equals(getEntity()) && routingContext.getBodyAsJson() != null)  //for monitor id validation which also fetch metric.type
         {
           routingContext.setBody(routingContext.getBodyAsJson().mergeIn(result.result()).toBuffer());

           routingContext.next();
         }
         else if (result.succeeded() && Entity.CREDENTIAL.equals(getEntity()) && routingContext.getBodyAsJson() != null)    //for credential id validation which also fetch protocol to validate update field
         {
           routingContext.setBody(routingContext.getBodyAsJson().mergeIn(result.result()).toBuffer());

           routingContext.next();
         }
         else if (result.succeeded())   //for get and getAll validation
         {
           routingContext.next();
         }
         else
         {
           LOGGER.error(Constant.NOT_VALID);

           routingContext.response().putHeader(Constant.CONTENT_TYPE,Constant.APPLICATION_JSON).setStatusCode(Constant.BAD_REQUEST).end(new JsonObject().put(Constant.STATUS,Constant.FAIL).put(Constant.STATUS_CODE,Constant.BAD_REQUEST).put(Constant.ERROR, Constant.NOT_VALID).encodePrettily());
         }
       }
       catch (Exception exception)
       {
         LOGGER.error(exception.getMessage());

         routingContext.response().setStatusCode(Constant.BAD_REQUEST).end(new JsonObject().put(Constant.STATUS,Constant.FAIL).put(Constant.ERROR, exception.getMessage()).put(Constant.STATUS_CODE,Constant.BAD_REQUEST).encodePrettily());
       }
     });
   }


   /*public void validate(RoutingContext routingContext)
   {
     Util.validate(routingContext,getEntity());
   }*/


   private void create( RoutingContext routingContext)
   {
     JsonObject insertData = routingContext.getBodyAsJson();

     insertData.put(Constant.IDENTITY, getEntity() + Constant.INSERT);

     routingContext.setBody(insertData.toBuffer());

     eventBusToDB(insertData, routingContext);
   }

   private void readAll(RoutingContext routingContext)
   {
     eventBusToDB(new JsonObject().put(Constant.IDENTITY,getEntity()+ Constant.READ_ALL),routingContext);
   }

   private void read(RoutingContext routingContext)
   {
     JsonObject readData = new JsonObject();

     readData.put(Constant.ID,routingContext.pathParam(Constant.ID)).put(Constant.IDENTITY,getEntity() + Constant.READ);

     routingContext.setBody(readData.toBuffer());

     eventBusToDB(readData,routingContext);
   }

   private void update(RoutingContext routingContext)
   {
     JsonObject updateData = routingContext.getBodyAsJson();

     updateData.put(getEntity().getId(), routingContext.pathParam(Constant.ID));

     updateData.put(Constant.IDENTITY,getEntity() + Constant.UPDATE);

     routingContext.setBody(updateData.toBuffer());

     eventBusToDB(updateData,routingContext);
   }

   private void delete(RoutingContext routingContext)
   {
     JsonObject deleteData = new JsonObject();

     deleteData.put(Constant.ID,routingContext.pathParam(Constant.ID)).put(Constant.IDENTITY,getEntity() + Constant.DELETE);

     routingContext.setBody(deleteData.toBuffer());

     eventBusToDB(deleteData,routingContext);
   }


   private void eventBusToDB(JsonObject data,RoutingContext routingContext)
   {

     vertx.eventBus().<JsonObject>request(Constant.DATABASE_HANDLER ,data,replayMessage->{
      try
      {
        if (replayMessage.succeeded() && replayMessage.result().body() != null)
        {
          if (Constant.METRIC_UPDATE.equals(data.getString(Constant.IDENTITY)) || Constant.MONITOR_UPDATE.equals(data.getString(Constant.IDENTITY)))
          {
            vertx.eventBus().send(Constant.UPDATE_SCHEDULING, data);
          }
          else if (Constant.MONITOR_DELETE.equals(data.getString(Constant.IDENTITY)))
          {
            vertx.eventBus().send(Constant.DELETE_SCHEDULING, data);
          }

          routingContext.response().putHeader(Constant.CONTENT_TYPE, Constant.APPLICATION_JSON).end(replayMessage.result().body().encodePrettily());
        }
        else
        {
          routingContext.response().putHeader(Constant.CONTENT_TYPE, Constant.APPLICATION_JSON).setStatusCode(Constant.BAD_REQUEST).end(replayMessage.cause().getMessage());
        }
      }
      catch (Exception exception)
      {
        LOGGER.error(exception.getMessage());

        routingContext.response().setStatusCode(Constant.INTERNAL_SERVER_ERROR).end(new JsonObject().put(Constant.STATUS,Constant.FAIL).put(Constant.ERROR, exception.getMessage()).put(Constant.STATUS_CODE,Constant.INTERNAL_SERVER_ERROR).encodePrettily());
      }
     });
   }
}
