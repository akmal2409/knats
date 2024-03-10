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
    testImplementation("org.jetbrains.kotlin:kotlin-test")
}

tasks.test {
    useJUnitPlatform()
}
