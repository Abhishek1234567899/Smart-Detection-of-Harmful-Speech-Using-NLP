import os
import sys
from hate.logger import logging
from hate.exception import CustomException
from hate.configuration.s3_operations import S3Operation  # Make sure this is your S3 operation class
from hate.entity.config_entity import DataIngestionConfig
from hate.entity.artifact_entity import DataIngestionArtifacts
from hate.constants import *
from zipfile import ZipFile


class DataIngestion:
    def __init__(self, data_ingestion_config: DataIngestionConfig):
        self.data_ingestion_config = data_ingestion_config
        self.s3_operations = S3Operation()

    def get_data_from_s3(self) -> None:
        try:
            logging.info("Entered the get_data_from_s3 method of DataIngestion class")
            
            # Ensure the directory for artifacts exists
            os.makedirs(self.data_ingestion_config.DATA_INGESTION_ARTIFACTS_DIR, exist_ok=True)
            logging.info(f"Created directory: {self.data_ingestion_config.DATA_INGESTION_ARTIFACTS_DIR}")

            # Download the data from S3
            self.s3_operations.read_data_from_s3(
                filename=self.data_ingestion_config.ZIP_FILE_NAME,
                bucket_name=self.data_ingestion_config.BUCKET_NAME,
                output_filename=self.data_ingestion_config.ZIP_FILE_PATH
            )
            logging.info(f"Downloaded {self.data_ingestion_config.ZIP_FILE_NAME} from S3 bucket {self.data_ingestion_config.BUCKET_NAME} to {self.data_ingestion_config.ZIP_FILE_PATH}")

            logging.info("Exited the get_data_from_s3 method of DataIngestion class")
        except Exception as e:
            raise CustomException(e, sys) from e

    def unzip_and_clean(self):
        logging.info("Entered the unzip_and_clean method of DataIngestion class")
        try:
            with ZipFile(self.data_ingestion_config.ZIP_FILE_PATH, 'r') as zip_ref:
                zip_ref.extractall(self.data_ingestion_config.ZIP_FILE_DIR)
                logging.info(f"Extracted zip file to: {self.data_ingestion_config.ZIP_FILE_DIR}")

            logging.info("Exited the unzip_and_clean method of DataIngestion class")

            return (
                self.data_ingestion_config.DATA_ARTIFACTS_DIR,
                self.data_ingestion_config.NEW_DATA_ARTIFACTS_DIR
            )
        except Exception as e:
            raise CustomException(e, sys) from e

    def initiate_data_ingestion(self) -> DataIngestionArtifacts:
        logging.info("Entered the initiate_data_ingestion method of DataIngestion class")
        try:
            self.get_data_from_s3()
            logging.info("Fetched the data from S3 bucket")

            imbalance_data_file_path, raw_data_file_path = self.unzip_and_clean()
            logging.info("Unzipped file and split into train and valid directories")

            data_ingestion_artifacts = DataIngestionArtifacts(
                imbalance_data_file_path=imbalance_data_file_path,
                raw_data_file_path=raw_data_file_path
            )

            logging.info("Exited the initiate_data_ingestion method of DataIngestion class")
            logging.info(f"Data ingestion artifact: {data_ingestion_artifacts}")

            return data_ingestion_artifacts

        except Exception as e:
            raise CustomException(e, sys) from e
