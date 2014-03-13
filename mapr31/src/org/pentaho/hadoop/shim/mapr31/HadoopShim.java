/*******************************************************************************
*
* Pentaho Big Data
*
* Copyright (C) 2002-2013 by Pentaho : http://www.pentaho.com
*
*******************************************************************************
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with
* the License. You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*
******************************************************************************/

package org.pentaho.hadoop.shim.mapr31;

import java.net.URLClassLoader;
import java.util.List;

import org.pentaho.di.core.auth.AuthenticationConsumerPluginType;
import org.pentaho.di.core.auth.AuthenticationPersistenceManager;
import org.pentaho.di.core.auth.core.AuthenticationManager;
import org.pentaho.di.core.auth.core.AuthenticationPerformer;
import org.pentaho.hadoop.shim.HadoopConfiguration;
import org.pentaho.hadoop.shim.HadoopConfigurationFileSystemManager;
import org.pentaho.hadoop.shim.api.Configuration;
import org.pentaho.hadoop.shim.common.CommonHadoopShim;
import org.pentaho.hadoop.shim.mapr31.auth.MapRSuperUserKerberosConsumer.MapRSuperUserKerberosConsumerType;
import org.pentaho.hdfs.vfs.MapRFileProvider;

import com.mapr.fs.proto.Security.TicketAndKey;
import com.mapr.login.client.MapRLoginHttpsClient;

public class HadoopShim extends CommonHadoopShim {
  protected static final String SUPER_USER = "authentication.superuser.provider";
  protected static final String DEFAULT_CLUSTER = "/";
  protected static final String MFS_SCHEME = "maprfs://";
  protected static final String[] EMPTY_CONNECTION_INFO = new String[2];

  static {
    JDBC_DRIVER_MAP.put("hive2",org.apache.hive.jdbc.HiveDriver.class); 
  }

  public HadoopShim() {
    System.setProperty("mapr.library.flatclass", "");
  }
  
  @Override
  public String[] getNamenodeConnectionInfo(Configuration c) {
    return EMPTY_CONNECTION_INFO;
  }
  
  @Override
  public String[] getJobtrackerConnectionInfo(Configuration c) {
    return EMPTY_CONNECTION_INFO;
  }

  @Override
  public void configureConnectionInformation(String namenodeHost, String namenodePort, String jobtrackerHost,
      String jobtrackerPort, Configuration conf, List<String> logMessages) throws Exception {
    if (namenodeHost == null || namenodeHost.length() == 0) {
      namenodeHost = DEFAULT_CLUSTER;
      logMessages.add("Using MapR default cluster for filesystem");
    } else if (namenodePort == null || namenodePort.trim().length() == 0) {
      logMessages.add("Using MapR CLDB named cluster: " + namenodeHost 
          + " for filesystem");
      namenodeHost = "/mapr/" + namenodeHost;
    } else {
      logMessages.add("Using filesystem at " + namenodeHost + ":" + namenodePort);
      namenodeHost = namenodeHost + ":" + namenodePort;
    }
    
    if (jobtrackerHost == null || jobtrackerHost.trim().length() == 0) {
      jobtrackerHost = DEFAULT_CLUSTER;
      logMessages.add("Using MapR default cluster for job tracker");
    } else if (jobtrackerPort == null || jobtrackerPort.trim().length() == 0) {
      logMessages.add("Using MapR CLDB named cluster: " + jobtrackerHost +
      " for job tracker");
      jobtrackerHost = "/mapr/" + jobtrackerHost;
    } else {
      logMessages.add("Using job tracker at " + jobtrackerHost + ":" + jobtrackerPort);
      jobtrackerHost = jobtrackerHost + ":" + jobtrackerPort;
    }
    
    String fsDefaultName = MFS_SCHEME + namenodeHost;
    String jobTracker = MFS_SCHEME + jobtrackerHost;
    conf.set("fs.default.name", fsDefaultName);
    conf.set("mapred.job.tracker", jobTracker);
    conf.set("fs.maprfs.impl", MapRFileProvider.FS_MAPR_IMPL);
  }

  @Override
  public void onLoad( HadoopConfiguration config, HadoopConfigurationFileSystemManager fsm ) throws Exception {
    fsm.addProvider( config, MapRFileProvider.SCHEME, config.getIdentifier(), new MapRFileProvider() );
    AuthenticationConsumerPluginType.getInstance().registerPlugin( (URLClassLoader) getClass().getClassLoader(),
        MapRSuperUserKerberosConsumerType.class );
    if ( config.getConfigProperties().containsKey( SUPER_USER ) ) {
      AuthenticationManager manager = AuthenticationPersistenceManager.getAuthenticationManager();
      AuthenticationPerformer<TicketAndKey, MapRLoginHttpsClient> performer =
          manager.getAuthenticationPerformer( TicketAndKey.class, MapRLoginHttpsClient.class, config
              .getConfigProperties().getProperty( SUPER_USER ) );
      if ( performer == null ) {
        throw new RuntimeException( "Unable to find relevant provider for MapR super user (id of "
            + config.getConfigProperties().getProperty( SUPER_USER ) );
      } else {
        TicketAndKey ticket = performer.perform( new MapRLoginHttpsClient() );
        if ( ticket == null ) {
          throw new RuntimeException( "Unable to get MapR ticket for provider "
              + config.getConfigProperties().getProperty( SUPER_USER ) );
        }
      }
    }
    setDistributedCacheUtil( new MapR3DistributedCacheUtilImpl( config ) );
  }
}
