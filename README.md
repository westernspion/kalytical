# kalytical

Committing to trunk for now  

Something I'm playing with as a means to declaratively orchestrate pipelines, and manage them, on top of Kubernetes  
[Mainfesto](Manifesto.md)

## Goals:  
I want to improve data engineer, ETL engineer or data scientit/analyst productivity by streamlining the creation and deployment process of data pipelines.  
1) Giving a common way to define, declare requirements and dependencies for data pipelines  
2) Have clear visibility into what downstream impact would be when a change to a pipeline, or new pipeline, is introduced     
3) Allow a DAG to be updated ad-hoc by a new pipeline request - rather than having to author or manually change the DAG  
4) Maintaining maximum flexibility of implementation (python, scripts, jq, or outside service - perhaps a cloud AI or data service) by only making them adhere to a container contract  
5) Make the data pipeline runtime self-servicable. Keep friction between developers and devops to a minimum by *safely* empowering developers to control their pipeline deployment  
6) Provide a registry and common interface to that registry for storage and recall of model artifacts (similar to AWS's ``model`` catalog)  
* A bonus would be hosting notebooks as well

## Supported data pipeline scenarios:  
While there are nuances in implementation of ML or traditional batch pipelines - I find it helps to understand that computers are glorified *data-in data-out* machines - and therefore at the top of the hierarchy, I refer to everything as a data pipeline *data-in data-out*. There are mainly 3 flavors of data pipelines, differing by execution context  
* Scheduled pipelines
* On-demand pipelines
* Continuous pipelines

ML training, batch ETL, batch ML, on-demand inference, all of it can be catalogued under those three categories.  

## Basic theory of operation:  
* Declarative syntax for creating pipelines, storing their dependencies in a DAG (Directed Acyclic Graph)  
* Kalytical reads this pipeline.yaml, interprets the requirements and registers the pipeline artifacts necessary for scheduled, on-demand, or continuous execution (streaming)   
* Kaly will be a CLI for interacting with the API facade  
* Kalytical will support ad-hoc execution (by user or by another service via API) or scheduled execution (by either being poised at the start of a pipeline DAG)  


The idea is that someone could define a pipeline syntax as follows,  

A sample pipeline spec -
```
resources:
    kind: data-pipeline
        name: my-pipeline-0
            image: my-packaged-datapipeline
            needs:
                cpu: 1
                memory: 512mi
            run:
                when: 
                    schedule: * * * 15 *
        depends_on: nothing
        metadata:
            input: db.table_name
            output: s3_bucket_name+path

    kind: data-pipeline
        name: my-pipeline-1
            image: my-packaged-datapipeline-downstream
            needs:
                cpu: 1
                memory: 512mi
            run:
                when: 
                    event: upstream_finish
        depends_on: nothing
        metadata:
            input: s3_bucket_path
            output: s3_bucket_path      

```
Kaly would be the CLI tool to create/ad-hoc execute, delete or monitor pipelines from the API  

Kalytical would then interpret this requirement, register the proper resources to Kubernetes (and perhaps outside of it) and/or reconcile the new requirement with what is already there.  


