# github_etl.py (Final Airflow DAG version)

import os
import requests
from neo4j import GraphDatabase
from pendulum import datetime

# Import the Airflow decorators
from airflow.decorators import dag, task

# --- 1. Credentials ---
# These remain the same
NEO4J_URI = "neo4j+s://c3a59bba.databases.neo4j.io"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "OCwwmmUQnl1UGczW-nQm-oRiDEhTBYSJlr8VThWt7Sg"
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")

# --- 2. Project Details ---
REPO_OWNER = "tensorflow"
REPO_NAME = "tensorflow"

# --- 3. Airflow DAG Definition ---

@dag(
    dag_id="github_contributors_etl",
    start_date=datetime(2025, 10, 2),
    schedule="@daily",  # This will run the DAG once every day
    catchup=False,
    tags=["github", "neo4j", "etl"],
)
def github_etl_dag():
    """
    This DAG fetches contributor data from a GitHub repository and loads it into Neo4j.
    """

    # --- 4. Airflow Task Definitions ---
    # By adding the @task decorator, our Python functions become Airflow tasks

    @task
    def fetch_contributors(owner: str, repo: str):
        """Fetches the top contributors for a given GitHub repository."""
        url = f"https://api.github.com/repos/{owner}/{repo}/contributors"
        headers = {"Authorization": f"token {GITHUB_TOKEN}"}
        
        print(f"Fetching contributors from {url}...")
        with requests.Session() as session:
            response = session.get(url, headers=headers)
            response.raise_for_status()
            print("Successfully fetched contributors.")
            return response.json() # The data is automatically passed to the next task

    @task
    def load_contributors_to_neo4j(contributors: list, repo_name: str):
        """Loads a list of contributors into Neo4j."""
        print("Loading data into Neo4j...")
        with GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD)) as driver:
            with driver.session() as session:
                session.run("MERGE (r:Repository {name: $repo})", repo=repo_name)
                for contributor in contributors:
                    login = contributor["login"]
                    session.run(
                        "MERGE (u:User {username: $username})"
                        "MERGE (r:Repository {name: $repo})"
                        "MERGE (u)-[:CONTRIBUTED_TO]->(r)",
                        username=login,
                        repo=repo_name,
                    )
        print("Successfully loaded data into Neo4j.")
    
    # --- 5. Define Task Dependencies ---
    # This defines the order of execution: fetch first, then load
    
    contributors_list = fetch_contributors(REPO_OWNER, REPO_NAME)
    load_contributors_to_neo4j(contributors_list, REPO_NAME)

# --- 6. Instantiate the DAG ---
# This makes the DAG visible to Airflow

github_etl_dag()