package com.mindarray.nms2.util;

public class Constant {
 public static final int HTTP_PORT = 8888;
  public static final String ROUTE_DISCOVERY_PATH = "/discovery";
  public static final String EVENTBUS_ADDRESS_DISCOVERY = "discovery.engine";
  public static final String EVENTBUS_ADDRESS_CHECK_IP = "checkIP";
  public static final String EVENTBUS_ADDRESS_DISCOVERY_DATABASE = "discovery.database";

  public static final String JSON_KEY_METRIC_TYPE = "metric.type";
  public static final String JSON_KEY_PASSWORD = "password";
  public static final String JSON_KEY_PORT = "port";
  public static final String JSON_KEY_USERNAME = "username";
  public static final String JSON_KEY_HOST = "host";
  public static final String JSON_KEY_VERSION = "version";

  public static final String STATUS = "status";
  public static final String ERROR = "error";
  public static final String SUCCESS = "success";

  public static final String DISCOVERED = "Already Discovered";

  public static final String NOT_DISCOVERED = "Not Discovered";

  public static final String PLUGIN_PATH = "./plugin.exe";

  public static final String DISCOVERY_AND_DATABASE_SUCCESS = "Discovery Success and stored in database";

  public static final String DISCOVERY_SUCCESS_DATABASE_FAILED = "Discovery Success but did not stored in database";

  public static final String INTERRUPTED_EXCEPTION = "INTERRUPTED_EXCEPTION";

  public static final String IO_EXCEPTION = "IO_EXCEPTION";

  public static final Integer BAD_REQUEST = 400;
  public static final String NO_INPUT = "NO JSON INPUT";
  public static final String INVALID_INPUT = "INVALID JSON INPUT";

  public static final String PING_DOWN = "Ping down";
  public static final String PING_UP = "Ping up";

  public static final String STATUS_CODE = "status.code";

  public static final String LINUX = "linux";
  public static final String WINDOWS = "windows";
  public static final String NETWORK_DEVICE = "network.device";

public static final String ILLEGAL_ARGUMENT_EXCEPTION = "IllegalArgumentException";
public static final Integer OK =200;
public static final Integer INTERNAL_SERVER_ERROR  =500;
public static final Integer ALREADY_AVAILABLE = 600;

public static final String QUERY_INSERT_TO_DISCOVERY_TABLE = "INSERT INTO credentials VALUES (?,?,?,?,?,?)";
public static final String QUERY_CHECK_IP = "select * from credentials where host = ?";
public static final String DATABASE_CONNECTION_URL = "jdbc:mysql://localhost:3306/NMS2.1";
public static final String DATABASE_CONNECTION_USER = "root";
public static final String DATABASE_CONNECTION_PASSWORD = "smit1682";
public static final String CONNECTION_REFUSED = "Connection Refused";

  public static final String zeroTo255 = "(\\d{1,2}|([01])\\" + "d{2}|2[0-4]\\d|25[0-5])";

 public static final String REGEX_IP = zeroTo255 + "\\." + zeroTo255 + "\\." + zeroTo255 + "\\." + zeroTo255;

public static final String VERSION_1 = "v1";
public static final String VERSION_2C = "v2c";

public static final String CREDENTIAL_NAME = "credential.name";
public static final String PROTOCOL = "protocol";


  public static final String MESSAGE = "message";
  public static final String DISCOVERY_NAME = "discovery.name";
  public static final String CREDENTIAL_ID = "credential.id" ;
  //public static final String CREDENTIAL_PROFILE = "credential.i";

  public static final String CREDENTIAL_INSERT = "CREDENTIALInsert";
  public static final String CREDENTIAL_READ = "CREDENTIALRead";
  public static final String CREDENTIAL_READ_ALL = "CREDENTIALReadAll";
  public static final String CREDENTIAL_UPDATE = "CREDENTIALUpdate";
  public static final String CREDENTIAL_DELETE = "CREDENTIALDelete";
  public static final String DISCOVERY_INSERT = "DISCOVERYInsert";
  public static final String DISCOVERY_READ = "DISCOVERYRead";
  public static final String DISCOVERY_READ_ALL = "DISCOVERYReadAll";
  public static final String DISCOVERY_UPDATE = "DISCOVERYUpdate";
  public static final String DISCOVERY_DELETE = "DISCOVERYDelete";

  public static final String MONITOR_DELETE = "MONITORDelete";
  public static final String MONITOR_READ = "MONITORRead";
  public static final String CPU = "cpu";
  public static final String MEMORY ="memory" ;
  public static final String DISK = "disk";
  public static final String SYSTEM = "system";
  public static final String PROCESS = "process";
  public static final String INTERFACE = "interface";
  public static final String MONITOR_READ_ALL = "MONITORReadAll";
  public static final String MONITOR_UPDATE ="MONITORUpdate" ;
  public static final String ID = "id";

  public static final String IDENTITY = "identity";
  public static final String INSERT_TO_DATABASE ="InsertToDatabase" ;
  public static final String INSERT = "Insert";
  public static final String READ_ALL = "ReadAll";
  public static final String READ = "Read";
  public static final String UPDATE = "Update";
  public static final String DELETE ="Delete" ;

  public static final String UPDATE_SCHEDULING = "updateScheduling";
  public static final String DELETE_SCHEDULING = "deleteScheduling";
  public static final String DISCOVERY_ID = "discovery.id";

  public static final String RUN_DISCOVERY_DATA_COLLECT = "needData";
  public static final String CREATE_MONITOR = "createMonitor";
  public static final String PROVISION_VALIDATION = "discoveryStatus";
  public static final String TIME = "time";
  public static final String DEFAULT_TIME = "default.time";
  public static final String JSON_KEY_METRIC_GROUP = "metric.group";
  public static final String PATH_PROVISION_WITH_ID ="/provision/:id" ;
  public static final String EA_SCHEDULING = "scheduling";
  public static final String EA_PULLING = "pulling";
  public static final String TOP_FIVE = "topFive";



  public static final String GET_LAST_INSTANCE = "getLastInstance";
  public static final String MONITOR_ID = "monitor.id";
  public static final String MONITOR_NAME = "monitor.name";
  public static final String DATA = "data";
  public static final String TIME_STAMP = "timestamp";
  public static final String VALIDATE_ID = "validateID";
  public static final String UPDATE_AFTER_RUN_DISCOVERY = "updateAfterRunDiscovery";
  public static final String PICK_UP_DATA_INITAL = "pickupData";
  public static final String DUMP_METRIC_DATA = "dumpMetricData";
  public static final String PULLING_FAIL = "Pulling Failed";

  public static final String INVALID_METRIC_GROUP = "Invalid metric group";
  public static final String SNMP = "snmp";
  public static final String TABLE_NAME = "table.name";

  public static final String PATH_DISCOVERY_WITH_ID = "/discovery/:id";
  public static final String MUST_BE_INTEGER = "All value must be in Integer And multiple of 10";
  public static final String NOT_VALID = "Not Valid ID";
}
