pipeline {

    agent {
        kubernetes {
            yamlFile 'kubernetesPod.yaml'
        }
    }

    options {
        buildDiscarder(logRotator(numToKeepStr: '3'))
    }

    environment {
        REPO_PREFIX = "196229073436.dkr.ecr.eu-west-1.amazonaws.com/openanalytics/"
        REPO = "openanalytics/phaedra2-calculationservice"
        ACCOUNTID = "196229073436"
    }

    stages {

        stage('Checkout phaedra2-parent') {
            steps {
                dir('../phaedra2-parent') {
                    checkout([$class: 'GitSCM', branches: [[name: '*/develop']], extensions: [], userRemoteConfigs: [[credentialsId: 'oa-jenkins', url: 'https://scm.openanalytics.eu/git/phaedra2-parent']]])
                }
            }
        }

        stage('Checkout phaedra2-commons') {
            steps {
                dir('../phaedra2-commons') {
                    checkout([$class: 'GitSCM', branches: [[name: '*/develop']], extensions: [], userRemoteConfigs: [[credentialsId: 'oa-jenkins', url: 'https://scm.openanalytics.eu/git/phaedra2-commons']]])
                }
            }
        }

        stage('Load maven cache repository from S3') {
            steps {
                container('builder') {
                    sh  """
                        aws --region 'eu-west-1' s3 sync s3://oa-phaedra2-jenkins-maven-cache/  /home/jenkins/maven-repository --quiet
                        """
                }
            }
        }

        stage('Build Phaedra2 commons') {
            steps {
                dir('../phaedra2-commons') {
                    container('builder') {

                        configFileProvider([configFile(fileId: 'maven-settings-rsb', variable: 'MAVEN_SETTINGS_RSB')]) {

                            sh 'mvn -s $MAVEN_SETTINGS_RSB -U clean install -Dmaven.repo.local=/home/jenkins/maven-repository -DskipTests'

                        }

                    }
                }
            }
        }

        stage('Build') {
            steps {
                container('builder') {

                    configFileProvider([configFile(fileId: 'maven-settings-rsb', variable: 'MAVEN_SETTINGS_RSB')]) {

                        sh 'mvn -s $MAVEN_SETTINGS_RSB -U clean install -DskipTests -Ddockerfile.skip -Dmaven.repo.local=/home/jenkins/maven-repository'

                    }

                }
            }
        }

        stage('Test') {
            steps {
                container('builder') {

                    configFileProvider([configFile(fileId: 'maven-settings-rsb', variable: 'MAVEN_SETTINGS_RSB')]) {

                        sh 'mvn -s $MAVEN_SETTINGS_RSB test -Ddockerfile.skip -Dmaven.repo.local=/home/jenkins/maven-repository'

                    }

                }
            }
        }

        stage('Build Docker image') {
            steps {
                container('builder') {

                    configFileProvider([configFile(fileId: 'maven-settings-rsb', variable: 'MAVEN_SETTINGS_RSB')]) {

                        sh "mvn -s \$MAVEN_SETTINGS_RSB dockerfile:build -Ddocker.repoPrefix=${env.REPO_PREFIX} -Dmaven.repo.local=/home/jenkins/maven-repository"

                    }

                }
            }
        }

        stage('Push to OA registry') {
            steps {
                container('builder') {
                    sh "aws --region eu-west-1 ecr describe-repositories --repository-names ${env.REPO} || aws --region eu-west-1 ecr create-repository --repository-name ${env.REPO}"
                    sh "\$(aws ecr get-login --registry-ids '${env.ACCOUNTID}' --region 'eu-west-1' --no-include-email)"

                    configFileProvider([configFile(fileId: 'maven-settings-rsb', variable: 'MAVEN_SETTINGS_RSB')]) {

                        sh "mvn -s \$MAVEN_SETTINGS_RSB dockerfile:push -Ddocker.repoPrefix=${env.REPO_PREFIX} -Dmaven.repo.local=/home/jenkins/maven-repository"
                    }
                }
            }
        }

        stage('Cache maven repository to S3') {
            steps {
                container('builder') {
                    sh  """
                        aws --region 'eu-west-1' s3 sync /home/jenkins/maven-repository s3://oa-phaedra2-jenkins-maven-cache/ --quiet
                        """
                }
            }
        }

    }

//    post {
//        success {
//            step([$class: 'JacocoPublisher',
//                  execPattern: 'target/jacoco.exec',
//                  classPattern: 'target/classes',
//                  sourcePattern: 'src/main/java',
//                  exclusionPattern: 'src/test*'
//            ])
//        }
//    }

}
