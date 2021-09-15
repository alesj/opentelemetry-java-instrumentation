plugins {
  id("otel.java-conventions")
}

val versions: Map<String, String> by project

dependencies {
  api(project(":testing-common"))

  implementation("org.apache.kafka:kafka-clients:0.11.0.0")

  implementation(project(":instrumentation:kafka-clients:kafka-clients-common:library"))

  implementation("org.testcontainers:kafka:${versions["org.testcontainers"]}")
}
