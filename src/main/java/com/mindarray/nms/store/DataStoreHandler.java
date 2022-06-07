package com.mindarray.nms.store;

import com.mindarray.nms.util.Constant;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;

public class DataStoreHandler extends AbstractVerticle
{
  private final DiscoveryStore discoveryStore = new DiscoveryStore();

  private final MetricStore metricStore = new MetricStore();

  private final CredentialStore credentialStore = new CredentialStore();

  private final MonitorStore monitorStore = new MonitorStore();

  private final UtilStore utilStore = new UtilStore();

  @Override
  public void start(Promise<Void> startPromise)
  {
    vertx.eventBus().<JsonObject>localConsumer(Constant.DATABASE_HANDLER, message->{

      JsonObject dataMessage = message.body();

      vertx.executeBlocking(databaseHandler->{

        switch (dataMessage.getString(Constant.IDENTITY))
        {
          case Constant.CREDENTIAL_INSERT:
              credentialStore.create(dataMessage,databaseHandler);
              break;

          case Constant.DISCOVERY_INSERT:
              discoveryStore.create(dataMessage,databaseHandler);
              break;

          case Constant.CREDENTIAL_READ_ALL:
              credentialStore.readAll(dataMessage,databaseHandler);
              break;

          case Constant.DISCOVERY_READ_ALL:
              discoveryStore.readAll(dataMessage,databaseHandler);
              break;

          case Constant.CREDENTIAL_READ:
              credentialStore.read(dataMessage,databaseHandler);
              break;

          case Constant.DISCOVERY_READ:
              discoveryStore.read(dataMessage,databaseHandler);
              break;

          case Constant.CREDENTIAL_DELETE:
              credentialStore.delete(dataMessage,databaseHandler);
              break;

          case Constant.DISCOVERY_DELETE:
              discoveryStore.delete(dataMessage,databaseHandler);
              break;

          case Constant.CREDENTIAL_UPDATE:
              dataMessage.remove(Constant.IDENTITY);
              credentialStore.update(dataMessage,databaseHandler);
              break;

          case Constant.DISCOVERY_UPDATE:
              dataMessage.remove(Constant.IDENTITY);
              discoveryStore.update(dataMessage,databaseHandler);
              break;

          case Constant.MONITOR_DELETE:
              monitorStore.delete(dataMessage,databaseHandler);
              break;

          case Constant.MONITOR_READ:
              monitorStore.read(dataMessage,databaseHandler);
              break;

          case Constant.METRIC_READ:
            metricStore.read(dataMessage,databaseHandler);
              break;

          case Constant.MONITOR_READ_ALL:
              monitorStore.readAll(dataMessage,databaseHandler);
              break;
          case Constant.METRIC_READ_ALL:
            metricStore.readAll(dataMessage,databaseHandler);

          case Constant.METRIC_UPDATE:
              dataMessage.remove(Constant.IDENTITY);
              metricStore.update(dataMessage,databaseHandler);
              break;

          case Constant.MONITOR_UPDATE:
              dataMessage.remove(Constant.IDENTITY);
              monitorStore.update(dataMessage,databaseHandler);
              break;

          case Constant.TOP_FIVE:
              utilStore.topFive(dataMessage,databaseHandler);
              break;

          case Constant.GET_LAST_INSTANCE:
              utilStore.getLastInstance(dataMessage,databaseHandler);
              break;

          case Constant.VALIDATE_ID:
              utilStore.checkId(dataMessage.getString(Constant.ID),dataMessage.getString(Constant.TABLE_NAME),databaseHandler);
              break;

          case Constant.UPDATE_AFTER_RUN_DISCOVERY:
              discoveryStore.updateAfterRunDiscovery(dataMessage,databaseHandler);
              break;

          case Constant.PROVISION_VALIDATION:
              monitorStore.discoveryCheck(dataMessage,databaseHandler);
              break;

          case Constant.CREATE_MONITOR:
              monitorStore.create(dataMessage,databaseHandler);
              break;

          case Constant.PICK_UP_DATA_INITAL:
              credentialStore.intialRead(databaseHandler);
              break;

          case Constant.DUMP_METRIC_DATA:
              monitorStore.dumpInDB(dataMessage,databaseHandler);
              break;

          case Constant.RUN_DISCOVERY_DATA_COLLECT:
              discoveryStore.mergeData(dataMessage,databaseHandler);
              break;

        }

      }, databaseHandlerResult->{

        if(databaseHandlerResult.succeeded())
        {
          message.reply(databaseHandlerResult.result());
        }
        else
        {
          message.fail(400,databaseHandlerResult.cause().getMessage());
        }

      });
    });

    startPromise.complete();
  }
}
