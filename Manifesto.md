Based on my professional observations, I believe there are several problems with cloud-native analytics
* Lots of buzzwords, samples to whet your apetite
* Companies giving "free" webinars that speak from their ``<insert cloud company or data company here>`` own bibles. And of course they do, it's in their best interest  
* Managemnt confusion, non-comprehensive strategy where everone ends up hitting the same walls
    * How did this datum get here? - Data provenance
    * I'm not touching that (i.e. who knows what will break downstream) - Brittle design patterns
    * Who knows what the schema is, let's cross our fingers - Transparency and code
    * Oh, we've already invested in X product - we'd prefer an FFFFff'd workaround with that tool (inflexible tooling)
    * We already have this data? Can you send me the path? (data cataloging)


As an ex-colleague of mine said ``ML and data are like teenage sex, everyone thinks everyone is doing it but nobody is doing it right``    
There are *lots* of brittle solutions that lack flexibility or completeness of solving the manageent component of this new cloud-native analytics domain.  
One that scales from the smallest ETL workload and expands to clusters of workloads - with proportionate expense and with best practices the whole way.    

I like to look at what Kubernetes has done to ease developer and operations friction - giving developers a *safeish* way of owning their deliverable with reliable replicatable components of infrastructure (pods, services, configmaps, namespaces, etc...). And, at the end of the day, the only real contract your code must adhere to is the *container* - giving you absolute flexibility in what you need your code to do.  

So I naturally seem to think that, as data pipelines and ETL move away from warehouse specific tools and schema on write  (informatica, ab-initio, PL/SQL, oracle ) to things like cloud storage, schema on read and APIs and embrace a more programming language oriented data format - they resemble microservices more-or less - albiet with a more data maniuplation purpose.  

Therefore, with a little help, Kubernetes could be a good place to start and, with a little lifting, become as good of a home for ETL-type data pipelines as it is for the microservice pattern.  

So, goals we'll try to solve (maybe not all of them)  
* Data provenance
* Brittle design patterns
* Transparency (for code and data)
* Flexibility
* Scaling

Data cataloging is difficult and I've yet to come up with a solution that is non-invasive and allows developers to be independent. I will not put in scope for now.