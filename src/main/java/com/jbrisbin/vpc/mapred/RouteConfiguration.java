package com.jbrisbin.vpc.mapred;

import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;

/**
 * @author J. Brisbin <jon@jbrisbin.com>
 */
@Configuration
public class RouteConfiguration {

  @Autowired
  private RabbitAdmin rabbitAdmin;

  // Map
  @Value("${map.exchange.name}")
  private String mapExchangeName;
  @Value("${map.queue.name}")
  private String mapQueueName;
  @Value("${map.route.name}")
  private String mapRouteName;
  // Reduce
  @Value("${reduce.exchange.name}")
  private String reduceExchangeName;

  @Bean
  public TopicExchange mapExchange() {
    TopicExchange exchange = new TopicExchange( mapExchangeName, true, false );
    rabbitAdmin.declareExchange( exchange );
    return exchange;
  }

  @Bean
  public TopicExchange reduceExchange() {
    TopicExchange exchange = new TopicExchange( reduceExchangeName, true, false );
    rabbitAdmin.declareExchange( exchange );
    return exchange;
  }

  @Bean
  public Queue mapQueue() {
    Queue q = new Queue( mapQueueName );
    q.setDurable( true );
    q.setAutoDelete( false );
    rabbitAdmin.declareQueue( q );
    return q;
  }

  @Bean
  public Binding groovyMapBinding() {
    Binding binding = BindingBuilder.from( mapQueue() ).to( mapExchange() )
        .with( mapRouteName );
    rabbitAdmin.declareBinding( binding );
    return binding;
  }

}
