package org.pentaho.hadoop.shim.mapr31.authentication;

import org.pentaho.di.core.auth.AuthenticationConsumerPlugin;
import org.pentaho.di.core.auth.AuthenticationConsumerType;
import org.pentaho.di.core.auth.NoAuthenticationAuthenticationProvider;
import org.pentaho.di.core.auth.core.AuthenticationConsumer;
import org.pentaho.di.core.auth.core.AuthenticationConsumptionException;

public class MapRSuperUserNoAuthConsumer implements
    AuthenticationConsumer<Void, NoAuthenticationAuthenticationProvider> {
  @AuthenticationConsumerPlugin( id = "MapRSuperUserNoAuthConsumer", name = "MapRSuperUserNoAuthConsumer" )
  public static class MapRSuperUserNoAuthConsumerType implements AuthenticationConsumerType {

    @Override
    public String getDisplayName() {
      return "MapRSuperUserNoAuthConsumer";
    }

    @Override
    public Class<? extends AuthenticationConsumer<?, ?>> getConsumerClass() {
      return MapRSuperUserNoAuthConsumer.class;
    }
  }

  public MapRSuperUserNoAuthConsumer( Void client ) {
    // Noop
  }

  @Override
  public Void consume( NoAuthenticationAuthenticationProvider authenticationProvider )
    throws AuthenticationConsumptionException {
    return null;
  }
}
