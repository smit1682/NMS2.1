package com.mindarray.nms2.api;

import com.mindarray.nms2.Bootstrap;
import com.mindarray.nms2.util.Constant;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;

public abstract class RestAPI {

  private final Vertx vertx = Bootstrap.getVertex();

  public RestAPI() {}

   public RestAPI(Router router)
   {

    router.post(getEntity().getPath()).setName("post").handler(this::validate).handler(this::create);

    router.put(getEntity().getPath() + "/:id").setName("put").handler(this::validate).handler(this::update);

    router.get(getEntity().getPath()).setName("getAll").handler(this::validate).handler(this::readAll);

    router.delete(getEntity().getPath() + "/:id").setName("delete").handler(this::validateId).handler(this::delete);

    router.get(getEntity().getPath() + "/:id").setName("get").handler(this::validateId).handler(this::read);

  }

   protected abstract Entity getEntity();

   public void validateId(RoutingContext routingContext)
   {

    Util.validateId(routingContext.pathParam(Constant.ID),getEntity()).onComplete(result->{

      if(result.succeeded())
      {

        routingContext.next();

      }
      else
      {

        routingContext.response().end(new JsonObject().put(Constant.STATUS,Constant.ERROR).put(Constant.ERROR,Constant.NOT_VALID).toString());

      }

    });

  }

   public void validate(RoutingContext routingContext)
   {

   Util.validate(routingContext,getEntity());

 }

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

     updateData.put(Constant.IDENTITY,getEntity() +Constant.UPDATE);

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

     vertx.eventBus().request(Constant.INSERT_TO_DATABASE ,data,replayMessage->{

       if(replayMessage.succeeded())
       {

         if(Constant.MONITOR_UPDATE.equals(data.getString(Constant.IDENTITY)))
         {

           vertx.eventBus().request(Constant.UPDATE_SCHEDULING,data,replayFromScheduler->{

             if(replayFromScheduler.succeeded()){System.out.println("updated in Arraylist");}

           });

         }

         if(Constant.MONITOR_DELETE.equals(data.getString(Constant.IDENTITY)))
         {
           vertx.eventBus().request(Constant.DELETE_SCHEDULING,data,replyMessage->{

             if(replyMessage.succeeded()) System.out.println("deleted in arraylist");
           });

         }

        routingContext.response().end( replayMessage.result().body().toString());

       }
       else
       {

       routingContext.response().end(replayMessage.cause().getMessage());

       }

     });
   }

}
