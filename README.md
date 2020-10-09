# Insight Data Engineer Project

This project showcased the ability of constructing ETL pipeline for processing stack overflow data (250GB) and Full-stack software skill as well as provide data analysis.

# Installation
- Use Ansible to provision EC2, install spark, postgres and Dash UI. (Refer to [DevOp folder](https://github.com/Shawn5141/Stack-Community/tree/master/DevOp) for further instrution)
- Link the DNS of EC2 to public domain (Not done yet)

# ETL Pipeline
- Tech Stack
Upload xml file downloaded from stack exchange data dump to s3. And use spark cluster to preprocessed posts data and user data. After pre-computation, the result table is stored in database which can be access by front end UI. (Ref to [ETLPipeline folder](https://github.com/Shawn5141/Stack-Community/tree/master/ETLPipeline))

- Airflow (Not done yet)
Periodically run spark program 

# App
- After installation and preprocessing. You can run the Dash App by `python app.py` in App directory. Futhur instruction refer to [APP](https://github.com/Shawn5141/Stack-Community/tree/master/App)
