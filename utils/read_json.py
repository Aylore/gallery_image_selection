import json
import jsonschema
from jsonschema import validate


import logging
from utils.logging_config import setup_logging


# Setup logging
setup_logging()
logger = logging.getLogger(__name__)

class JSONFileProcessor:
    """Class for reading JSON and JSONL files."""

    def __init__(self):
        pass



    def read_jsonl(self, file_path , file_scehma_path):
        """
        Reads a JSONL (JSON Lines) file and returns the data as a list of dictionaries.

        :param file_path: Path to the JSONL file.
        :return: List of dictionaries containing the data from the JSONL file.
        """
        logger.info(f"Reading JSONL file: {file_path} with Scehma : {file_scehma_path}")
        schema = self.read_json(file_scehma_path)

        try:
            with open(file_path, 'r') as file:
                data = [json.loads(line) for line in file if  self.validate_jsonl_line(jsonl_line= json.loads(line) , schema= schema)]
            logger.info(f"Successfully read {len(data)} records from {file_path}")
            return data
        except Exception as e:
            logger.error(f"Error reading JSONL file: {file_path} - {e}")
            raise

    def read_json(self, file_path):
        """
        Reads a JSON file and returns the data as a dictionary.

        :param file_path: Path to the JSON file.
        :return: Dictionary containing the data from the JSON file.
        """
        logger.info(f"Reading JSON file: {file_path}")
        try:
            with open(file_path, 'r') as file:
                data = json.load(file)
            logger.info(f"Successfully read JSON file: {file_path}")
            return data
        except Exception as e:
            logger.error(f"Error reading JSON file: {file_path} - {e}")
            raise


        
    def validate_jsonl_line(self , jsonl_line, schema):
        """
        Validates a JSONL file against a JSON schema.

        :param file_path: Path to the JSONL file.
        :param schema: JSON schema to validate against.
        :param transfer_invalid: Whether to write invalid records to a separate file.
        :return: True if validation is successful, otherwise raises a ValidationError.
        """

        logger.info(f"Validating JSONL line")
        record = jsonl_line 



        try:
            validate(instance=record, schema=schema)
            
            logger.info(f"Record is valid.")
            return True
            # valid_data.append(record)
        except jsonschema.exceptions.ValidationError as e:
            logger.error(f"Validation error in record : {e.message}")
            return False

      


def main():

    processor = JSONFileProcessor()

    # Example usage
    jsonl_file = "data/images.jsonl"
    json_file = "schemas/image.json"

    # Reading JSONL file
    try:
        data_jsonl = processor.read_jsonl(jsonl_file , json_file)
        logger.info(f"First record in JSONL: {data_jsonl[0]}")
    except Exception as e:
        logger.error(f"Failed to read JSONL file: {e}")
    return data_jsonl
    # Reading JSON file
    # try:
    #     data_json = processor.read_json(json_file)
    #     logger.info(f"JSON data: {data_json}")
    # except Exception as e:
    #     logger.error(f"Failed to read JSON file: {e}")

if __name__ == "__main__":
    data = main()
