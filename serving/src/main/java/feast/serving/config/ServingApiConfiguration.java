/*
 * Copyright 2018 The Feast Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package feast.serving.config;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import feast.core.StoreProto.Store;
import feast.serving.service.spec.CachedSpecService;
import feast.serving.service.spec.CoreSpecService;
import feast.serving.service.serving.ServingService;
import feast.serving.service.serving.RedisServingService;
import feast.serving.service.spec.SpecService;
import io.opentracing.Tracer;
import io.opentracing.contrib.concurrent.TracedExecutorService;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.protobuf.ProtobufJsonFormatHttpMessageConverter;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Global bean configuration.
 */
@Slf4j
@Configuration
public class ServingApiConfiguration implements WebMvcConfigurer {

  @Autowired
  private ProtobufJsonFormatHttpMessageConverter protobufConverter;

  private ScheduledExecutorService scheduledExecutorService =
      Executors.newSingleThreadScheduledExecutor();

  @Bean
  public AppConfig getAppConfig(
      @Value("${feast.redispool.maxsize}") int redisPoolMaxSize,
      @Value("${feast.redispool.maxidle}") int redisPoolMaxIdle,
      @Value("${feast.maxentity}") int maxEntityPerBatch,
      @Value("${feast.timeout}") int timeout) {
    return AppConfig.builder()
        .maxEntityPerBatch(maxEntityPerBatch)
        .redisMaxPoolSize(redisPoolMaxSize)
        .redisMaxIdleSize(redisPoolMaxIdle)
        .timeout(timeout)
        .build();
  }

  @Bean
  public SpecService getCoreServiceSpecStorage(
      @Value("${feast.store.id}") String storeId,
      @Value("${feast.core.host}") String coreServiceHost,
      @Value("${feast.core.grpc.port}") String coreServicePort,
      @Value("${feast.cacheDurationMinute}") int cacheDurationMinute) {
    final CachedSpecService cachedSpecStorage =
        new CachedSpecService(new CoreSpecService(coreServiceHost, Integer.parseInt(coreServicePort)),
            storeId);

    // reload all specs including new ones periodically
    scheduledExecutorService.schedule(
        cachedSpecStorage::populateCache, cacheDurationMinute, TimeUnit.MINUTES);

    // load all specs during start up
    try {
      cachedSpecStorage.populateCache();
    } catch (Exception e) {
      log.error("Unable to preload feast's spec");
    }
    return cachedSpecStorage;
  }

  @Bean
  public ServingService getFeastServing(
      @Value("${feast.store.id}") String storeId,
      @Value("${feast.core.host}") String coreServiceHost,
      @Value("${feast.core.grpc.port}") String coreServicePort,
      AppConfig appConfig,
      Tracer tracer) {
    CoreSpecService coreSpecService = new CoreSpecService(coreServiceHost, Integer.parseInt(coreServicePort));
    Store store = coreSpecService.getStoreDetails(storeId);

    switch (store.getType()) {
      case REDIS:
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(appConfig.getRedisMaxPoolSize());
        poolConfig.setMaxIdle(appConfig.getRedisMaxIdleSize());
        JedisPool jedisPool = new JedisPool(poolConfig, store.getRedisConfig().getHost(),
            store.getRedisConfig().getPort());
        return new RedisServingService(jedisPool, tracer);
      case BIGQUERY:
        // TODO: Implement connection to BigQuery
        return null;
      default:
        return null;
    }
  }

  @Bean
  public ListeningExecutorService getExecutorService(
      Tracer tracer, @Value("${feast.threadpool.max}") int maxPoolSize) {

    ExecutorService executor = Executors.newFixedThreadPool(maxPoolSize);
    return MoreExecutors.listeningDecorator(new TracedExecutorService(executor, tracer));
  }

  @Bean
  ProtobufJsonFormatHttpMessageConverter protobufHttpMessageConverter() {
    return new ProtobufJsonFormatHttpMessageConverter();
  }

  @Override
  public void configureMessageConverters(List<HttpMessageConverter<?>> converters) {
    converters.add(protobufConverter);
  }
}
