# Setup Environment
1. Enable PubSub API
2. Create [notification channels](https://cloud.google.com/storage/docs/reporting-changes) for the bucket gs://ghack-data-challenge-2023/ to a pubsub topic
3. [Optional] Create a subscription if you want messages to be retained
4. [Optional] Create a bucket for Dataflow if you're planning on using that runner.
5. [Optional] Upload your template to the bucket

# Setup Dataflow job
1. gcloud init / gcloud auth login
2. pip install apache-beam[gcp]
3. To execute it either:
   - Run the local process
   - Upload the template with the command line script and create a new job in the UI based on that template


# Execution
- Local streaming execution


    python .\main.py 
    --runner
    DirectRunner
    --topic
    "projects/<YOUR-PROJECT>/topics/<YOUR-TOPIC>"
    --temp_location
    "gs://<YOUR-BUCKET>/temp_local"
    --project
    "jordi-playground-data"
    --region
    "europe-north1-a"
    --streaming

- Dataflow upload template 

    
    python .\main.py 
    --runner
    DataflowRunner
    --topic
    "projects/<YOUR-PROJECT>/topics/<YOUR-TOPIC>"
    --temp_location
    "gs://<YOUR-BUCKET>/temp_local"
    --template_location
    "gs://<YOUR-BUCKET>/dataflow_template"
    --project
    "<YOUR-PROJECT>"
    --region
    "europe-north1-a"