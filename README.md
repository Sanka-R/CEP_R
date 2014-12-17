CEP_R
=====

R Integration for WSO2 Complex Event Processor (CEP) using jri


<h2>Instruction for setting up the project</h2>

<ol><li>Set JRI_HOME</li>
<li>Copy JRI.jar, JRIEngine.jar, REngine.jar files to {CEP_HOME}/repository/components/lib </li>
<li>Include the following line in the wso2server.sh file in {CEP_HOME}/bin in the last do-while loop
    -Djava.library.path=$JRI_HOME \</li>
<li>Use maven clean install</li>
</ol>

