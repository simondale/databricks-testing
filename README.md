# Introduction 
This project demonstrates creating a simplistic Databricks pipeline and providing Unit Test coverage of key application logic. 

# Getting Started
In order to get started with this project first, clone the repository into Azure DevOps.

Once cloned, setup Azure DevOps to:
* connect to Azure using a Service Connection
* setup a variable that specifies your Databricks URL
* create a Pipeline that executes the azure-pipelines.yml in the root of the repository

# Build and Test
This project demonstrates testing Databricks through a CI/CD pipeline. Creation of the pipeline above will upload Notebooks to Databricks, run the **tests** Notebook and then download JUnit XML test results that can be published to Azure DevOps.

