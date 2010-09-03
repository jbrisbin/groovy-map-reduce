package com.jbrisbin.vpc.mapred;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.SingleConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author J. Brisbin <jon@jbrisbin.com>
 */
@Configuration
public class MapReduceTestConfiguration {

  public ResultsListener resultsListener = new ResultsListener();
  SimpleMessageListenerContainer resultsListenerContainer;

  @Bean
  public SingleConnectionFactory connectionFactory() {
    SingleConnectionFactory cf = new SingleConnectionFactory();
    return cf;
  }

  @Bean
  public RabbitAdmin rabbitAdmin() {
    RabbitAdmin ra = new RabbitAdmin( connectionFactory() );
    return ra;
  }

  @Bean
  public Queue resultsQueue() {
    RabbitAdmin admin = rabbitAdmin();
    Queue q = new Queue( "test.mapred.q" );
    admin.declareQueue( q );
    return q;
  }

  @Bean
  public SimpleMessageListenerContainer resultsListenerContainer() {
    if ( null == resultsListenerContainer ) {
      resultsListenerContainer = new SimpleMessageListenerContainer(
          connectionFactory() );
      resultsListenerContainer.setAutoStartup( false );
      resultsListenerContainer.setQueues( resultsQueue() );
      resultsListenerContainer.setMessageListener( resultsListener );
    }
    return resultsListenerContainer;
  }

  class ResultsListener implements MessageListener {
    Logger log = LoggerFactory.getLogger( getClass() );
    int total = 0;

    @Override
    public void onMessage( Message message ) {
      try {
        total += Integer.parseInt( new String( message.getBody() ) );
        if ( total > 66 ) {
          // We're finished
          log.info( "Finished! {}", total );
        }
      } catch ( NumberFormatException e ) {
        log.error( "Got a bad msg: {}", e.getMessage() );
      }
    }

  }
}
