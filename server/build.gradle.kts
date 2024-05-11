plugins {
    id("knats.coroutine-reactive-conventions")
}


version = "1.0-SNAPSHOT"


dependencies {
    implementation(project(":transport"))
    implementation(project(":extensions"))
}
