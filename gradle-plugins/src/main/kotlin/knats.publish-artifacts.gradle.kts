plugins {
    `maven-publish`
}

publishing {
    repositories {
        maven {
            name = "GithubPackages"
            url = uri("https://maven.pkg.github.com/akmal2409/knats")
            credentials {
                username =
                    project.findProperty("gpr.user")?.toString() ?: System.getenv("CI_USER")
                password =
                    project.findProperty("gpr.key")?.toString() ?: System.getenv("CI_TOKEN")
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
