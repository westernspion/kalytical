An event drive, asynchronous job scheduling system that is Kubernetes native.  
Original use case is to drive data pipeline execution and on-the-fly dependency resolution (DAGS) via configuration, rather than through static dags that are linked to a particular programming language  


TODO: 
* Create CLI Kaly  
* Refactor to use rabbit MQ  
* Include jupyerlab and sample notebooks for people to poke around  
* Docs
* Abstract out IDP  
* Add mongodb image  
* Terraform stack for cloud assets  
* Integrate kubernetes with IdP  
* Secrets injection and storage mechanism  
* Demo pipelines highlighting features  
    * Dynamic pipeline generation  
    * Flexible declaration of downstream dependencies - safe deletes and updates (via feature toggling)  
    * Inline secrets declaration (interpolation by CLI when writing pipelines)  
    * Observability and autoscaling  
    * Integrated pipeline development  
    * Integrated model development  

* Addendums for productionalization  
* Switch from pipeline to job nomenclature
* Gettings started dev
