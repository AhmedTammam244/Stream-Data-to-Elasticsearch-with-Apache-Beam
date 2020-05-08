# Stream Data to Elasticsearch with Apache Beam
May 8 ,2020    
Author : Ahmed Tammam     
In this post I walk through the process of handling unbounded streaming data using Apache Beam, and pushing it to Elasticsearch  as a data warehouse.

 to run project you must know                   
   1.runner (DirctRunner or DataFlowRunner )   
   2. project name , create topic (good) from https://console.cloud.google.com/cloudpubsub/topic    
   3. username , password for elasticsearch
   
# Test ^_^  
     $   #!/usr/bin/env bash     
     $  /home/{machine_name}/miniconda3/envs/rec-3.5/bin/python3  /home/{machine_name}/etl/pubsubToSink.py    \
              --runner DirectRunner     \
              --temp_location gs://bucket_name/temp  \
              --max_num_workers 6    \
              --input projects/{project_name}/topics/good   \
              --region us-central1          \
              --streaming flag     \
              --project {project_name}       \
              --sink_opts "{'host':'localhost','port':9200,'scheme':'http','http_auth':('username', 'password')}"
  
