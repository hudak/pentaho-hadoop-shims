package org.pentaho.hadoop.shim.mapr31.authentication;

import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import org.pentaho.di.core.auth.AuthenticationConsumerPlugin;
import org.pentaho.di.core.auth.AuthenticationConsumerType;
import org.pentaho.di.core.auth.KerberosAuthenticationProvider;
import org.pentaho.di.core.auth.core.AuthenticationConsumer;
import org.pentaho.di.core.auth.core.AuthenticationConsumptionException;
import org.pentaho.di.core.auth.kerberos.KerberosUtil;

import com.mapr.fs.proto.Security.TicketAndKey;
import com.mapr.login.client.MapRLoginHttpsClient;

public class MapRSuperUserKerberosConsumer implements
    AuthenticationConsumer<Void, KerberosAuthenticationProvider> {
  @AuthenticationConsumerPlugin( id = "MapRSuperUserKerberosConsumer", name = "MapRSuperUserKerberosConsumer" )
  public static class MapRSuperUserKerberosConsumerType implements AuthenticationConsumerType {

    @Override
    public String getDisplayName() {
      return "MapRSuperUserKerberosConsumer";
    }

    @Override
    public Class<? extends AuthenticationConsumer<?, ?>> getConsumerClass() {
      return MapRSuperUserKerberosConsumer.class;
    }
  }

  private final KerberosUtil kerberosUtil;

  public MapRSuperUserKerberosConsumer( Void client ) {
    System.setProperty("mapr.library.flatclass", "");
    this.kerberosUtil = new KerberosUtil();
  }

  @Override
  public Void consume( KerberosAuthenticationProvider authenticationProvider )
    throws AuthenticationConsumptionException {
    final LoginContext loginContext;
    try {
      if ( authenticationProvider.isUseExternalCredentials() ) {
        if ( authenticationProvider.isUseKeytab() ) {
          loginContext =
              kerberosUtil.getLoginContextFromKeytab( authenticationProvider.getPrincipal(), authenticationProvider
                  .getKeytabLocation() );
        } else {
          loginContext = kerberosUtil.getLoginContextFromKerberosCache( authenticationProvider.getPrincipal() );
        }
      } else {
        loginContext =
            kerberosUtil.getLoginContextFromUsernamePassword( authenticationProvider.getPrincipal(),
                authenticationProvider.getPassword() );
      }
    } catch ( LoginException e ) {
      throw new AuthenticationConsumptionException( e );
    }
    try {
      loginContext.login();
      Subject.doAs( loginContext.getSubject(), new PrivilegedExceptionAction<TicketAndKey>() {

        @Override
        public TicketAndKey run() throws Exception {
          return new MapRLoginHttpsClient().getMapRCredentialsViaKerberos( 1209600000L );
        }
      } );
      return null;
    } catch ( LoginException e ) {
      throw new AuthenticationConsumptionException( e );
    } catch ( PrivilegedActionException e ) {
      throw new AuthenticationConsumptionException( e );
    }
  }
}
