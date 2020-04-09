pipeline {
    agent { label "sailfish" }
    tools {
        jdk 'openjdk-1.8u202'
    }
    environment {
        VERSION_MAINTENANCE = """${sh(
                            returnStdout: true,
                            script: 'git rev-list --count HEAD'
                            )}""" //TODO: Calculate revision from a specific tag instead of a root commit
        NEXUS = credentials('docker-user_nexus.exp.exactpro.com_9000')
        NEXUS_URL = 'nexus.exp.exactpro.com:9000'
        GRADLE_SWITCHES = "-Pversion_maintenance=${VERSION_MAINTENANCE}"
    }
    stages {
        stage('Build') {
            steps {
                sh """
                    ./gradlew clean build ${GRADLE_SWITCHES}
                """
            }
        }
    }
}
