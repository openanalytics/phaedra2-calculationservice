pipeline {

    agent {
        kubernetes {
            yamlFile 'kubernetesPod.yaml'
            defaultContainer 'builder'
        }
    }

    options {
        buildDiscarder(logRotator(numToKeepStr: '3'))
    }

    environment {
        REPO_PREFIX = "196229073436.dkr.ecr.eu-west-1.amazonaws.com/openanalytics/"
        ACCOUNTID = "196229073436"
    }
    stages {

        stage('Load maven cache repository from S3') {
            steps {
                container('builder') {
                    sh """
                        aws --region 'eu-west-1' s3 sync s3://oa-phaedra2-jenkins-maven-cache/ /home/jenkins/maven-repository --quiet
                        """
                }
            }
        }

        stage('Prepare environment') {
            steps {
                script {
                    env.GROUP_ID = sh(returnStdout: true, script: "mvn org.apache.maven.plugins:maven-help-plugin:3.2.0:evaluate -Dexpression=project.groupId -q -DforceStdout").trim()
                    env.ARTIFACT_ID = sh(returnStdout: true, script: "mvn org.apache.maven.plugins:maven-help-plugin:3.2.0:evaluate -Dexpression=project.artifactId -q -DforceStdout").trim()
                    env.VERSION = sh(returnStdout: true, script: "mvn org.apache.maven.plugins:maven-help-plugin:3.2.0:evaluate -Dexpression=project.version -q -DforceStdout").trim()
                    env.REPO = "openanalytics/${env.ARTIFACT_ID}-server"
                    env.MVN_ARGS = "-Dmaven.repo.local=/home/jenkins/maven-repository --batch-mode"
                    env.MVN_EXLCUDE_PARENT = ""
//                    env.MVN_EXLCUDE_PARENT = "--projects '!${env.GROUP_ID}:${env.ARTIFACT_ID}'"
                }
            }

        }

        stage('Build') {
            steps {
                container('builder') {

                    configFileProvider([configFile(fileId: 'maven-settings-rsb', variable: 'MAVEN_SETTINGS_RSB')]) {

                        sh "mvn -s \$MAVEN_SETTINGS_RSB -U clean install -DskipTests -Ddockerfile.skip ${env.MVN_ARGS} ${env.MVN_EXLCUDE_PARENT}"

                    }

                }
            }
        }

        stage('Test') {
            steps {
                container('builder') {

                    configFileProvider([configFile(fileId: 'maven-settings-rsb', variable: 'MAVEN_SETTINGS_RSB')]) {

                        sh "mvn -s \$MAVEN_SETTINGS_RSB test -Ddockerfile.skip ${env.MVN_ARGS} ${env.MVN_EXLCUDE_PARENT}"

                    }

                }
            }
        }

        stage("Deploy to Nexus") {
            steps {
                container('builder') {

                    configFileProvider([configFile(fileId: 'maven-settings-rsb', variable: 'MAVEN_SETTINGS_RSB')]) {

                        sh "mvn -s \$MAVEN_SETTINGS_RSB deploy -DskipTests -Ddockerfile.skip ${env.MVN_ARGS} ${env.MVN_EXLCUDE_PARENT}"

                    }

                }
            }
        }

        stage('Build Docker image') {
            steps {
                dir('server') {
                    container('builder') {

                        configFileProvider([configFile(fileId: 'maven-settings-rsb', variable: 'MAVEN_SETTINGS_RSB')]) {

                            sh "mvn -s \$MAVEN_SETTINGS_RSB dockerfile:build -Ddocker.repoPrefix=${env.REPO_PREFIX} ${env.MVN_ARGS}"

                        }

                    }
                }
            }
        }

        stage('Push to OA registry') {
            steps {
                dir('server') {
                    container('builder') {
                        sh "aws --region eu-west-1 ecr describe-repositories --repository-names ${env.REPO} || aws --region eu-west-1 ecr create-repository --repository-name ${env.REPO}"
                        sh "\$(aws ecr get-login --registry-ids '${env.ACCOUNTID}' --region 'eu-west-1' --no-include-email)"

                        configFileProvider([configFile(fileId: 'maven-settings-rsb', variable: 'MAVEN_SETTINGS_RSB')]) {

                            sh "mvn -s \$MAVEN_SETTINGS_RSB dockerfile:push -Ddocker.repoPrefix=${env.REPO_PREFIX} ${env.MVN_ARGS}"
                        }
                    }
                }
            }
        }

        stage('Cache maven repository to S3') {
            steps {
                container('builder') {
                    sh """
                        aws --region 'eu-west-1' s3 sync /home/jenkins/maven-repository s3://oa-phaedra2-jenkins-maven-cache/ --quiet
                        """
                }
            }
        }
    }

    post {
        success {
            step([$class: 'JacocoPublisher',
                  execPattern: '**/target/jacoco.exec',
                  classPattern: '**/target/classes',
                  sourcePattern: '**/src/main/java',
                  exclusionPattern: '**/src/test*'
            ])
        }
    }

}
