### Project Overview: Airflow ETL Pipeline with Postgres and API Integration 

This project involves creating an ETL (Extract, Transform, Load) pipeline using Apache Airflow. The pipeline extracts data from an external API (in this case, NASA's Astronomy Picture of the Day (APOD) API), transforms the data, and loads it into a Postgres database. The entire workflow is orchestrated by Airflow, a platform that allows scheduling, monitoring, and managing workflows.

The project uses Docker to run Airflow and Postgres as services, ensuring an isolated and reproducible environment. We also utilize Airflow hooks and operators to handle the ETL process efficiently.

Key Components of the Project:
Airflow for Orchestration:

Airflow is used to define, schedule, and monitor the entire ETL pipeline. It manages task dependencies, ensuring that the process runs sequentially and reliably.
The Airflow DAG (Directed Acyclic Graph) defines the workflow, which includes tasks like data extraction, transformation, and loading.
Postgres Database:

A PostgreSQL database is used to store the extracted and transformed data.
Postgres is hosted in a Docker container, making it easy to manage and ensuring data persistence through Docker volumes.
We interact with Postgres using Airflow’s PostgresHook and PostgresOperator.
NASA API (Astronomy Picture of the Day):

The external API used in this project is NASA’s APOD API, which provides data about the astronomy picture of the day, including metadata like the title, explanation, and the URL of the image.
We use Airflow’s SimpleHttpOperator to extract data from the API.

Objectives of the Project:

1. Extract (E):
The SimpleHttpOperator is used to make HTTP GET requests to NASA’s APOD API.
The response is in JSON format, containing fields like the title of the picture, the explanation, and the URL to the image.
2. Transform (T):
The extracted JSON data is processed in the transform task using Airflow’s TaskFlow API (with the @task decorator).
This stage involves extracting relevant fields like title, explanation, url, and date and ensuring they are in the correct format for the database.
3. Load (L):
The transformed data is loaded into a Postgres table using PostgresHook.
If the target table doesn’t exist in the Postgres database, it is created automatically as part of the DAG using a create table task.

First i ran everything in my local server and here is the results
<img width="1911" height="1002" alt="Screenshot 2026-05-13 150116" src="https://github.com/user-attachments/assets/c04405bd-48ff-435f-b326-b8e32ea47377" />
and i made sure , changes are made in my local postgres db running in the docker container
<img width="1419" height="802" alt="Screenshot 2026-05-13 150105" src="https://github.com/user-attachments/assets/7ae2e493-dc4d-4b50-a2bc-37e60d5b1928" />

and then i ran the whole pipeline in the cloud with the help of AWS and ASTRONOMER.io and here are the results 
<img width="1919" height="996" alt="db amazon" src="https://github.com/user-attachments/assets/a2cf01bb-088a-4c1c-b014-6044050efa6c" />
<img width="1919" height="1079" alt="Screenshot 2026-05-14 141543" src="https://github.com/user-attachments/assets/3eb4ea74-2d5d-48c2-88e7-dcaae92fbcf6" />
<img width="1919" height="1079" alt="Screenshot 2026-05-14 141524" src="https://github.com/user-attachments/assets/88e7258b-1346-4a74-ab4e-020e59934a24" />
<img width="1915" height="994" alt="Screenshot 2026-05-14 141042" src="https://github.com/user-attachments/assets/69c3c62f-8fe8-4544-8834-3e28f9a26a7f" />
first few triggers werent working coz of my mistake which is i ran the AWS DB instance in the private server

<img width="1919" height="1079" alt="cloud connection successful" src="https://github.com/user-attachments/assets/9a3bd6f0-e83e-4e65-b75a-8c6d68e51422" />








