plugins {
  id("otel.library-instrumentation")
}

dependencies {
  implementation(project(":instrumentation:kafka-clients:kafka-clients-common:library"))
  compileOnly("org.apache.kafka:kafka-clients:0.11.0.0")

  testLibrary("org.springframework.kafka:spring-kafka:2.4.0.RELEASE")
  testLibrary("org.springframework.kafka:spring-kafka-test:2.4.0.RELEASE")
  testLibrary("org.springframework:spring-core:5.2.9.RELEASE")
  testImplementation("javax.xml.bind:jaxb-api:2.2.3")

  latestDepTestLibrary("org.apache.kafka:kafka_2.13:+")
}
