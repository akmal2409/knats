plugins {
    kotlin("jvm")
}

kotlin {
    jvmToolchain(21)
}

group = "io.github.akmal2409.knats"

repositories {
    mavenCentral()
}

dependencies {
    implementation("ch.qos.logback:logback-classic:1.5.3")
    implementation("io.github.oshai:kotlin-logging-jvm:5.1.0")
    testImplementation("org.jetbrains.kotlin:kotlin-test")
}

tasks.test {
    useJUnitPlatform()
}
