plugins {
    id("knats.coroutine-reactive-conventions")
    id("knats.publish-artifacts")
}


version = "1.0-SNAPSHOT"


dependencies {
    implementation(project(":transport"))
}
