package com.jbrisbin.vpc.mapred;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author J. Brisbin <jon@jbrisbin.com>
 */
public class Main {

  static AtomicBoolean wait = new AtomicBoolean( true );

  public static void main( String[] args ) {
    Logger log = LoggerFactory.getLogger( Main.class );
    ApplicationContext context = new FileSystemXmlApplicationContext(
        "etc/groovy-map-reduce.xml" );
    while ( wait.get() ) {
      try {
        Thread.sleep( 500 );
      } catch ( InterruptedException e ) {
        log.error( e.getMessage(), e );
      }
    }
  }
}
