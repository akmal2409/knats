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

val kotestVersion = "5.8.0"
val mockkVersion = "1.13.10"

dependencies {
    implementation("ch.qos.logback:logback-classic:1.5.3")
    implementation("io.github.oshai:kotlin-logging-jvm:5.1.0")
    testImplementation("org.jetbrains.kotlin:kotlin-test")
    testImplementation("io.kotest:kotest-assertions-core:$kotestVersion")
    testImplementation("io.kotest:kotest-property:$kotestVersion")
    testImplementation("io.mockk:mockk:$mockkVersion")
}

tasks.test {
    useJUnitPlatform()
}
