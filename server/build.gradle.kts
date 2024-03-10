plugins {
    id("knats.kotlin-conventions")
}

version = "1.0-SNAPSHOT"

dependencies {
    implementation(platform("io.arrow-kt:arrow-stack:1.2.1"))

    implementation("io.arrow-kt:arrow-core")
    implementation("io.arrow-kt:arrow-fx-coroutines")
}


