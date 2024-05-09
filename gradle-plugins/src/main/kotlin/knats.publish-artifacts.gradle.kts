plugins {
    `maven-publish`
}

publishing {
    repositories {
        maven {
            name = "GithubPackages"
            url = uri("https://maven.pkg.github.com/OWNER/REPOSITORY")
            credentials {
                username = project.findProperty("gpr.user")?.toString()
                password = project.findProperty("gpr.key")?.toString()
            }
        }
    }

    publications {
        create<MavenPublication>("gpr") {
            pom {
                licenses {
                    license {
                        name = "The Apache License, Version 2.0"
                        url = "http://www.apache.org/licenses/LICENSE-2.0.txt"
                    }
                }

                developers {
                    developer {
                        id = "akmal2409"
                        name = "Akmal Alikhujaev"
                        email = "contact@akmal.engineer"
                    }
                }
            }
        }
    }
}
