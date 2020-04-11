#!/user/bin/env groovy
import hudson.model.*

pipeline{
   agent{
    docker{
     label 'docker'
     image ''
     args '-u root:root --dns xx.xx.xxx.xx'
    }
   }
   environment {
     EDGE_NODE_HOST = ''
     EDGE_NODE_SSHKEY = credentials('ssh-key')
     EDGE_NODE_USR = ''

     EDGE_NODE_DIR = '/abc/${BRANCH_NAME}/${BUILD_DISPLAY_NAME}'
     DEPLOYEMNT_DIR = ''
     EDGE_NODE_DIR_RESOURCES= "${EDGE_NODE_DIR}/resources"
     EDGE_NODE_JAR_FILE = "${EDGE_NODE_DIR}/abc.jar"

     //the following files are relative to ${WORKSPACE} dir
     WORKSPACE_DIR_RESOURCES = "src/main/resources"
     WORKSPACE_RESOURCES_TAR_FILES = "target/scala-2.11/resources.tar.gz" // will be created as artifact
     WORKSPACE_JAR_FILE = "targes/scala-2.11/abc.jar"

     SBT = 'sbt -Dsbt.override.build.repos=true -Dsbt.log.noformat=true -Dsbt.repository.config=./repositories -batch'
     SCP = "scp -rp -oStrictHostKeyChecking=no -i ${EDGE_NODE_SSHKEY}"
     SSH = "ssh -oStrictHostKeyChecking=no -i ${EDGE_NODE_SSHKEY} ${EDGE_NODE_USR}@${EDGE_NODE_HOST}"
   }
   options {
     timeout(time: 30, unit: 'MINUTES')
   }
   stages {
     stage('Prepare process'){
      steps {
        sh '''
         cp certificate_dir_path_in_git/certificate.cer  /usr/local/share/ca-certificates/certificate.crt
         update-ca-certificate -f
         '''
         }
      }
   }
   stage('Cleanup'){
     steps{
       sh '''
        ${SBT} clean
        ${SBT} cleanFiles
        rm -rf target
        '''
     }
   }
   stage('Build'){
     steps{
      sh '''
       ${SBT} compile
       cat /root/.sbt/boot/update.log
       '''
     }
   }
   stage('Unit Test'){
     steps{
       //TODO: Execute all tests
       //Currently only a single test case is executed as demo, as
       sh '${SBT} "testOnly **.DataSinkTest -- -z"'
       //sh '${SBT} test'
     }format
   }
   stage('Package'){
    steps{
      sh '${SBT} package'
    }
   }
   stage('Integration Test'){
    steps{
      // create subfolder for branch & job
      sh '${SSH} "mkdir -p ${EDGE_NODE_DIR}"'

      //copy over necessary files
      sh '''
       ${SCP} ${WORKSPACE}/${WORKSPACE_DIR_RESOURCES} ${EDGE_NODE_DIR}@${EDGE_NODE_HOST}:${EDGE_NODE_DIR_RESOURCES}
       ${SCP} ${WORKSPACE}/${WORKSPACE_JAR_FILES} ${EDGE_NODE_USR}@${EDGE_NODE_HOST}:${EDGE_NODE_JAR_FILE}
       '''
       //Execute integration test
       // sh ''' ${SSH}  "spark-submit" '''
    }
   }
   stage('Archive Artifacts'){
    steps{
      // Package configuration files
      sh '''
       cd ${WORKSPACE}/${WORKSPACE_DIR_RESOURCES}
       tar -czvf ${WORKSPACE}/${WORKSPACE_RESOURCES_TAR_FILES} *
       '''
       archiveArtifacts artifacts: "${WORKSPACE_JAR_FILE},${WORKSPACE_RESOURCES_TAR_FILES}", onlyIfSuccessful: true, fingerprint: true
    }
   }
   stage('Deployment'){
    when{
     allof{
       branch 'feature/*'
     }
    }
    steps{
      sh '''
        ${SCP} ${WORKSPACE}/${WORKSPACE_DIR_RESOURCES} ${EDGE_NODE_DIR}@${EDGE_NODE_HOST}:${DEPLOYEMNT_DIR}
        ${SCP} ${WORKSPACE}/${WORKSPACE_JAR_FILES} ${EDGE_NODE_USR}@${EDGE_NODE_HOST}:${DEPLOYEMNT_DIR}
      '''
    }
   }

}