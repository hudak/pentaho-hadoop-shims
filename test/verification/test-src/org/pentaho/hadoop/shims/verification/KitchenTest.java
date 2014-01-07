package org.pentaho.hadoop.shims.verification;

import java.io.File;
import java.io.FileFilter;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.List;
import java.util.Random;

import org.hsqldb.Server;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.logging.CentralLogStore;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.parameters.UnknownParamException;
import org.pentaho.di.job.Job;
import org.pentaho.di.job.JobMeta;

@RunWith(value = Parameterized.class)
public class KitchenTest {
  private static final String HSQL_URL = "hsql.url";

  private static final String HSQL_PORT = "hsql.port";

  private File file;

  @SuppressWarnings("unused")
  private String name;

  private static Server server;

  private static Random random = new Random();

  /**
   * Starts hsqldb using hsql.url and hsql.port, setting them if necessary
   * @throws SocketException
   * @throws SQLException 
   * @throws ClassNotFoundException 
   */
  @BeforeClass
  public static void startHsqldb() throws SocketException, SQLException, ClassNotFoundException {
    server = new Server();
    if (!System.getProperties().containsKey(HSQL_URL)) {
      String hsqlUrl = null;
      Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
      while (interfaces.hasMoreElements() && hsqlUrl == null) {
        NetworkInterface currentInterface = interfaces.nextElement();
        if (currentInterface.isUp() && !currentInterface.isLoopback() && !currentInterface.isVirtual()) {
          Enumeration<InetAddress> addresses = currentInterface.getInetAddresses();
          while (addresses.hasMoreElements() && hsqlUrl == null) {
            InetAddress address = addresses.nextElement();
            if (!address.isLoopbackAddress() && address instanceof Inet4Address) {
              hsqlUrl = address.getHostAddress();
            }
          }
        }
      }
      System.setProperty(HSQL_URL, hsqlUrl);
    }
    server.setAddress(System.getProperty(HSQL_URL));
    if (!System.getProperties().containsKey(HSQL_PORT)) {
      System.setProperty(HSQL_PORT, Integer.toString(random.nextInt(55534) + 10000));
    }
    server.setPort(Integer.parseInt(System.getProperty(HSQL_PORT)));
    server.setDatabasePath(0, "file:mydb");
    server.setDatabaseName(0, "xdb");
    server.start();
    Class.forName(org.hsqldb.jdbcDriver.class.getCanonicalName());
    Connection c = DriverManager.getConnection("jdbc:hsqldb:hsql://" + server.getAddress() + ":" + server.getPort()
        + "/" + server.getDatabaseName(0, true), "SA", "");
    try {
      c.prepareStatement("CREATE USER \"cluster\" PASSWORD 'cluster' ADMIN").execute();
      c.prepareStatement("COMMIT").execute();
    } catch (SQLException e) {
      System.out.println("Cluster user already exists, assuming password correct");
    } finally {
      c.close();
    }
  }
  
  @AfterClass
  public static void shutdownHyperSql() throws SQLException {
    Connection c = DriverManager.getConnection("jdbc:hsqldb:hsql://" + server.getAddress() + ":" + server.getPort()
        + "/" + server.getDatabaseName(0, true), "SA", "");
    try {
      c.prepareStatement("SHUTDOWN COMPACT").execute();
    } catch (SQLTransientConnectionException e) {
      
    } finally {
      c.close();
    }
  }

  public KitchenTest(File file, String name) {
    this.file = file;
    this.name = name;
  }

  @Parameters(name = "{index}: file:{1}")
  public static Collection<Object[]> data() {
    final File dirName = new File(System.getProperty("job.dirname"));
    List<File> files = new ArrayList<File>(Arrays.asList(dirName.listFiles(new FileFilter() {

      @Override
      public boolean accept(File pathname) {
        return pathname.getParentFile().equals(dirName) && (pathname.getName().endsWith(".kjb"));
      }
    })));
    String priorityList = System.getProperty("job.order");
    if (priorityList != null) {
      final List<String> priorities = new ArrayList<String>(Arrays.asList(priorityList.split(",")));
      Collections.sort(files, new Comparator<File>() {

        @Override
        public int compare(File o1, File o2) {
          int indexO1 = priorities.indexOf(o1.getName());
          int indexO2 = priorities.indexOf(o2.getName());
          if (indexO1 == -1 && indexO2 == -1) {
            return o1.getAbsolutePath().compareTo(o2.getAbsolutePath());
          } else if (indexO1 == -1) {
            return 1;
          } else if (indexO2 == -1) {
            return -1;
          } else {
            return indexO1 - indexO2;
          }
        }
      });
    }

    Object[][] data = new Object[files.size()][];
    for (int i = 0; i < files.size(); i++) {
      data[i] = new Object[] { files.get(i), files.get(i).getName() };
    }
    return Arrays.asList(data);
  }

  @Before
  public void setup() throws KettleException {
    KettleEnvironment.init();
  }

  @Test
  public void runTest() throws KettleXMLException, UnknownParamException {
    CentralLogStore.init();
    LogChannelInterface log = new LogChannel("ShimUnittest");
    JobMeta jobMeta = new JobMeta(file.getAbsolutePath(), null, null);
    Job job = new Job(null, jobMeta);
    jobMeta.setArguments(null);
    job.initializeVariablesFrom(null);
    job.setLogLevel(log.getLogLevel());
    jobMeta.setInternalKettleVariables(job);

    for (String parameterName : jobMeta.listParameters()) {
      String systemValue = System.getProperty(parameterName);
      if (systemValue != null) {
        jobMeta.setParameterValue(parameterName, systemValue);
      }
    }

    job.copyParametersFrom(jobMeta);
    job.activateParameters();
    job.start();
    job.waitUntilFinished();
    Assert.assertEquals("Job executed with " + job.getResult().getNrErrors() + ":\n" + job.getResult().getLogText(), 0,
        job.getResult().getNrErrors());
  }
}
