plugins {
    id("java")
}

group = "rodrigues.igor"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(platform("org.junit:junit-bom:5.9.1"))
    testImplementation("org.junit.jupiter:junit-jupiter")

    // https://mvnrepository.com/artifact/com.opencsv/opencsv
    implementation("com.opencsv:opencsv:5.9")

    // https://mvnrepository.com/artifact/com.mysql/mysql-connector-j
    implementation("com.mysql:mysql-connector-j:8.2.0")
}

tasks.test {
    useJUnitPlatform()
}