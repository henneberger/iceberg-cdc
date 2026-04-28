import org.gradle.external.javadoc.StandardJavadocDocletOptions

plugins {
    java
    id("com.github.johnrengelman.shadow") version "8.1.1"
    id("com.vanniktech.maven.publish") version "0.36.0"
}

group = "dev.henneberger"
version = providers.gradleProperty("releaseVersion").orElse("0.1.0-SNAPSHOT").get()

repositories {
    mavenCentral()
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(17))
    }
}

mavenPublishing {
    coordinates("dev.henneberger", "iceberg-cdc", version.toString())
    publishToMavenCentral(true)
    signAllPublications()

    pom {
        name.set("iceberg-cdc")
        description.set("Apache Flink source connector that exposes Apache Iceberg tables as CDC streams.")
        url.set("https://github.com/henneberger/iceberg-cdc")

        licenses {
            license {
                name.set("Apache License, Version 2.0")
                url.set("https://www.apache.org/licenses/LICENSE-2.0.txt")
                distribution.set("repo")
            }
        }

        developers {
            developer {
                id.set("henneberger")
                name.set("Daniel Henneberger")
                url.set("https://github.com/henneberger")
            }
        }

        scm {
            connection.set("scm:git:https://github.com/henneberger/iceberg-cdc.git")
            developerConnection.set("scm:git:ssh://git@github.com/henneberger/iceberg-cdc.git")
            url.set("https://github.com/henneberger/iceberg-cdc")
        }
    }
}

val flinkVersion = "2.0.0"
val flinkBinary = "2.0"
val icebergVersion = "1.10.1"
val hadoopVersion = "3.3.6"

dependencies {
    // Flink 2.x table-API + streaming surface (compileOnly: provided in cluster runtime).
    compileOnly("org.apache.flink:flink-streaming-java:$flinkVersion")
    compileOnly("org.apache.flink:flink-clients:$flinkVersion")
    compileOnly("org.apache.flink:flink-connector-base:$flinkVersion")
    compileOnly("org.apache.flink:flink-connector-files:$flinkVersion")
    compileOnly("org.apache.flink:flink-runtime:$flinkVersion")
    compileOnly("org.apache.flink:flink-table-common:$flinkVersion")
    compileOnly("org.apache.flink:flink-table-api-java:$flinkVersion")
    compileOnly("org.apache.flink:flink-table-api-java-bridge:$flinkVersion")
    compileOnly("org.apache.flink:flink-table-runtime:$flinkVersion")
    compileOnly("org.apache.parquet:parquet-column:1.13.1")
    compileOnly("org.apache.parquet:parquet-common:1.13.1")

    // Iceberg 1.10 — full v3 spec maturity, deletion vectors, row lineage.
    implementation("org.apache.iceberg:iceberg-core:$icebergVersion")
    implementation("org.apache.iceberg:iceberg-api:$icebergVersion")
    implementation("org.apache.iceberg:iceberg-data:$icebergVersion")
    implementation("org.apache.iceberg:iceberg-parquet:$icebergVersion")
    implementation("org.apache.iceberg:iceberg-aws:$icebergVersion")
    implementation("org.apache.iceberg:iceberg-aws-bundle:$icebergVersion")
    implementation("org.apache.iceberg:iceberg-common:$icebergVersion")
    // iceberg-flink for the Flink 2.0 line — supplies IcebergSource + delete-vector reader.
    implementation("org.apache.iceberg:iceberg-flink-$flinkBinary:$icebergVersion")

    implementation("org.apache.parquet:parquet-hadoop:1.13.1")
    implementation("org.apache.parquet:parquet-column:1.13.1")
    implementation("org.apache.hadoop:hadoop-common:$hadoopVersion") {
        exclude(group = "org.slf4j")
    }
    implementation("org.apache.hadoop:hadoop-mapreduce-client-core:$hadoopVersion") {
        exclude(group = "org.slf4j")
    }

    compileOnly("org.apache.hadoop:hadoop-common:$hadoopVersion")
    compileOnly("org.apache.hadoop:hadoop-aws:$hadoopVersion")

    implementation("org.slf4j:slf4j-api:2.0.12")

    testImplementation("org.junit.jupiter:junit-jupiter:5.10.2")
    testImplementation("org.apache.flink:flink-test-utils:$flinkVersion")
    testImplementation("org.apache.flink:flink-streaming-java:$flinkVersion")
    testImplementation("org.apache.flink:flink-clients:$flinkVersion")
    testImplementation("org.apache.flink:flink-connector-base:$flinkVersion")
    testImplementation("org.apache.flink:flink-connector-files:$flinkVersion")
    testImplementation("org.apache.flink:flink-runtime:$flinkVersion")
    testImplementation("org.apache.flink:flink-table-common:$flinkVersion")
    testImplementation("org.apache.flink:flink-table-api-java:$flinkVersion")
    testImplementation("org.apache.flink:flink-table-api-java-bridge:$flinkVersion")
    testImplementation("org.apache.flink:flink-table-planner-loader:$flinkVersion")
    testImplementation("org.apache.flink:flink-table-runtime:$flinkVersion")
    testImplementation("org.apache.hadoop:hadoop-common:$hadoopVersion")
    testImplementation("org.apache.hadoop:hadoop-hdfs-client:$hadoopVersion")
    testImplementation("org.apache.parquet:parquet-column:1.13.1")
    testImplementation("org.apache.parquet:parquet-hadoop:1.13.1")
    testImplementation("org.slf4j:slf4j-simple:2.0.12")
}

tasks.test {
    useJUnitPlatform()
    // Flink + Kryo on JDK 17 still needs java.base opens.
    jvmArgs(
            "--add-opens=java.base/java.util=ALL-UNNAMED",
            "--add-opens=java.base/java.lang=ALL-UNNAMED",
            "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
            "--add-opens=java.base/java.nio=ALL-UNNAMED",
            "--add-opens=java.base/java.io=ALL-UNNAMED",
            "--add-opens=java.base/java.net=ALL-UNNAMED",
            "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
            "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
            "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED")
    systemProperty("mat.state.backend", "hashmap")
}

tasks.withType<Javadoc>().configureEach {
    (options as StandardJavadocDocletOptions).addStringOption("Xdoclint:none", "-quiet")
}

tasks.named<com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar>("shadowJar") {
    isZip64 = true
}
