import os
import sys
import keras
import pickle
import numpy as np
import pandas as pd
from hate.logger import logging
from hate.exception import CustomException
from keras.utils import pad_sequences
from hate.constants import *
from hate.configuration.s3_operations import S3Operation
from sklearn.metrics import confusion_matrix
from hate.entity.config_entity import ModelEvaluationConfig
from hate.entity.artifact_entity import ModelEvaluationArtifacts, ModelTrainerArtifacts, DataTransformationArtifacts

class ModelEvaluation:
    def __init__(self, model_evaluation_config: ModelEvaluationConfig,
                 model_trainer_artifacts: ModelTrainerArtifacts,
                 data_transformation_artifacts: DataTransformationArtifacts):
        """
        :param model_evaluation_config: Configuration for model evaluation stage
        :param model_trainer_artifacts: Output reference of model trainer artifact stage
        :param data_transformation_artifacts: Output reference of data transformation artifact stage
        """
        self.model_evaluation_config = model_evaluation_config
        self.model_trainer_artifacts = model_trainer_artifacts
        self.data_transformation_artifacts = data_transformation_artifacts
        self.s3_operations = S3Operation()

    def get_best_model_from_s3(self) -> str:
        """
        :return: Fetch best model from S3 storage and store inside best model directory path
        """
        try:
            logging.info("Entered the get_best_model_from_s3 method of Model Evaluation class")
            os.makedirs(self.model_evaluation_config.BEST_MODEL_DIR_PATH, exist_ok=True)
            best_model_path = os.path.join(self.model_evaluation_config.BEST_MODEL_DIR_PATH, self.model_evaluation_config.MODEL_NAME)
            self.s3_operations.read_data_from_s3(self.model_evaluation_config.MODEL_NAME, self.model_evaluation_config.BUCKET_NAME, best_model_path)
            logging.info("Exited the get_best_model_from_s3 method of Model Evaluation class")
            return best_model_path
        except Exception as e:
            raise CustomException(e, sys) from e 

    def evaluate(self):
        """
        :param model: Currently trained model or best model from S3 storage
        :param data_loader: Data loader for validation dataset
        :return: loss
        """
        try:
            logging.info("Entering into the evaluate function of Model Evaluation class")
            x_test = pd.read_csv(self.model_trainer_artifacts.x_test_path, index_col=0)
            y_test = pd.read_csv(self.model_trainer_artifacts.y_test_path, index_col=0)

            with open('tokenizer.pickle', 'rb') as handle:
                tokenizer = pickle.load(handle)

            load_model = keras.models.load_model(self.model_trainer_artifacts.trained_model_path)
            x_test = x_test['tweet'].astype(str)
            x_test = x_test.squeeze()
            y_test = y_test.squeeze()

            test_sequences = tokenizer.texts_to_sequences(x_test)
            test_sequences_matrix = pad_sequences(test_sequences, maxlen=MAX_LEN)

            accuracy = load_model.evaluate(test_sequences_matrix, y_test)
            logging.info(f"The test accuracy is {accuracy}")

            lstm_prediction = load_model.predict(test_sequences_matrix)
            res = [1 if prediction[0] >= 0.5 else 0 for prediction in lstm_prediction]
            logging.info(f"The confusion_matrix is {confusion_matrix(y_test, res)} ")
            return accuracy
        except Exception as e:
            raise CustomException(e, sys) from e

    def initiate_model_evaluation(self) -> ModelEvaluationArtifacts:
        """
            Method Name :   initiate_model_evaluation
            Description :   This function is used to initiate all steps of the model evaluation

            Output      :   Returns model evaluation artifact
            On Failure  :   Write an exception log and then raise an exception
        """
        logging.info("Initiate Model Evaluation")
        try:
            logging.info("Loading currently trained model")
            trained_model = keras.models.load_model(self.model_trainer_artifacts.trained_model_path)
            with open('tokenizer.pickle', 'rb') as handle:
                load_tokenizer = pickle.load(handle)

            trained_model_accuracy = self.evaluate()

            logging.info("Fetch best model from S3 storage")
            best_model_path = self.get_best_model_from_s3()

            logging.info("Check if the best model is present in the S3 storage or not?")
            if not os.path.isfile(best_model_path):
                is_model_accepted = True
                logging.info("S3 storage model is not present, currently trained model is accepted")
            else:
                logging.info("Load best model fetched from S3 storage")
                best_model = keras.models.load_model(best_model_path)
                best_model_accuracy = self.evaluate()

                logging.info("Comparing accuracy between best_model and trained_model")
                is_model_accepted = best_model_accuracy > trained_model_accuracy

            model_evaluation_artifacts = ModelEvaluationArtifacts(is_model_accepted=is_model_accepted)
            logging.info("Returning the ModelEvaluationArtifacts")
            return model_evaluation_artifacts

        except Exception as e:
            raise CustomException(e, sys) from e
