package com.jbrisbin.vpc.mapred;

import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Address;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageCreator;
import org.springframework.amqp.rabbit.connection.SingleConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitMessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;

import java.io.*;

/**
 * @author J. Brisbin <jon@jbrisbin.com>
 */
public class GroovyMapReduceTest {

  Logger log;
  ApplicationContext context;

  @Before
  public void setUp() {
    log = LoggerFactory.getLogger( GroovyMapReduceTest.class );
    context = new FileSystemXmlApplicationContext(
        "etc/groovy-map-reduce.xml" );
  }

  @Test
  public void testMapReduce() throws IOException, InterruptedException {
    ApplicationContext testContext = new AnnotationConfigApplicationContext(
        MapReduceTestConfiguration.class );

    SimpleMessageListenerContainer results = testContext
        .getBean( SimpleMessageListenerContainer.class );

    RabbitTemplate tmpl = new RabbitTemplate(
        testContext.getBean( SingleConnectionFactory.class ) );
    for ( int i = 0; i < 23; i++ ) {
      tmpl.send( "map", "groovy", new MessageCreator() {
        @Override
        public Message createMessage() {
          RabbitMessageProperties props = new RabbitMessageProperties();
          props.setCorrelationId( "test.mapred".getBytes() );
          props.setReplyTo( new Address( "test.mapred.q" ) );
          props.setContentType( "application/json" );
          props.getHeaders().put( "script", "test.groovy" );
          return new Message( "[1,2,3,4,5]".getBytes(), props );
        }
      } );
    }

    results.start();

    int total = 0;
    for (; total < 69; total = ((MapReduceTestConfiguration.ResultsListener) results
        .getMessageListener()).total ) {
      Thread.sleep( 500 );
    }

    log.info( "TOTAL: {}", total );
    assert total == 69;
  }

  @After
  public void tearDown() {
  }
}
