repositories {
    gradlePluginPortal()
}

val kotlinVersion = "1.9.23"

plugins {
    `kotlin-dsl`
}

group = "io.github.akmal2409.knats"
version = "0.0.1-SNAPSHOT"


dependencies {
    implementation("org.jetbrains.kotlin.jvm:org.jetbrains.kotlin.jvm.gradle.plugin:$kotlinVersion")
}
