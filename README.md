# Insight Data Engineer Project

This project showcased the ability of constructing ETL pipeline for processing stack overflow data (250GB) and Full-stack software skill as well as provide data analysis.

[Temp Demo website](http://insightdataengineer.online/) | [Slide](https://docs.google.com/presentation/d/1sbWKLwaT2vLml31VI6-uxXzdjK5P2kiTAZ9GfWRD1sY/edit?usp=sharing)

# Installation/DevOp
- Use Ansible to provision EC2, install spark, postgres and Dash UI. (Refer to [DevOp](https://github.com/Shawn5141/Stack-Community/tree/master/DevOp) folder for further instrution)
- Link the DNS of EC2 to public domain (Not done yet)

# ETL Pipeline
- Tech Stack
Upload xml file downloaded from stack exchange data dump to s3. And use spark cluster to preprocessed posts data and user data. After pre-computation, the result table is stored in database which can be access by front end UI. (Ref to [ETLPipeline](https://github.com/Shawn5141/Stack-Community/tree/master/ETLPipeline)) folder.

- Airflow (Not done yet)
Periodically run spark program 

# App
- After installation and preprocessing. You can run the Dash App by `python app.py` in App directory. In [APP](https://github.com/Shawn5141/Stack-Community/tree/master/App), it displays the design of user interface using Dash. 
- The UI implemented with multiple function. Descriptions are shown below 
![UI](./img/UI.PNG)