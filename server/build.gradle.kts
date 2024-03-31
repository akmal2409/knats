plugins {
    id("knats.kotlin-conventions")
}

version = "1.0-SNAPSHOT"

val mockkVersion = "1.13.10"
val coroutinesVersion = "1.8.0"

dependencies {
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:$coroutinesVersion")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:$coroutinesVersion")

    testImplementation("io.mockk:mockk:$mockkVersion")
}


