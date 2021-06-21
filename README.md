# cromwell-utils


A collection of tools for interacting with the cromwell server and the creation, and manipulations of input files for analysis.



running the cromwell server
---------------------------

```
cd <CROMWELL EXECUTION DIR>
(nohup cromwell --java-options "-Dconfig.file=/cluster/lib/nsm-analysis/configs/cromwell_azure.conf " server &)
```
