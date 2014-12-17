CEP_R
=====

R Integration for WSO2 Complex Event Processor (CEP) using jri


Instruction for setting up the project

Set JRI_HOME
Copy JRI.jar, JRIEngine.jar, REngine.jar files to {CEP_HOME}/repository/components/lib
Include the following line in the wso2server.sh file in {CEP_HOME}/bin in the last do-while loop
    -Djava.library.path=$JRI_HOME \
Use maven clean install

