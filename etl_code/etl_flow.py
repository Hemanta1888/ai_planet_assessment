import os
from metaflow import FlowSpec, step

class ETLFlow(FlowSpec):

    @step
    def start(self):
        print("Starting ETL Workflow")
        self.next(self.run_data_ingestion)

    @step
    def run_data_ingestion(self):
        # Run data ingestion script
        print("Running data ingestion script...")
        os.system("python data_ingestion.py")
        self.next(self.run_data_transformation)

    @step
    def run_data_transformation(self):
        # Run data transformation script
        print("Running data transformation script...")
        os.system("python data_transformation.py")
        self.next(self.end)

    @step
    def end(self):
        print("ETL Workflow completed")

if __name__ == '__main__':
    ETLFlow()
