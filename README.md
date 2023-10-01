The Project involves extracting data from YouTube using the YouTube API. Python is used to transform the collected raw data. The project is deployed on Airflow with EC2 instance for automation. The final transformed data is stored on Amazon S3 for easy access and backup. Additionally, the project loads the structured/tabular data into Amazon Redshift for further analysis and querying, creating a comprehensive data pipeline.

This end-to-end pipeline ensures seamless data extraction, transformation, storage, and integration, enabling valuable insights from YouTube data to be easily accessible and analyzed in a structured format.

# Youtube Data Pipeline Architecture
![yt-dp-1](https://github.com/riti215/Youtube_Data_Pipeline/assets/57587827/ef0732a1-605d-478e-af65-5a84d540a6a7)

# Youtube Data Pipeline DAG (Workflow)
![image](https://github.com/riti215/Youtube_Data_Pipeline/assets/57587827/9e18f305-2796-4ed5-bfe5-8d29f9e7064b)
