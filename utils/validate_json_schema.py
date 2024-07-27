import json
import jsonschema
from jsonschema import validate
from utils.read_json import JSONFileProcessor


import logging 
from utils.logging_config import setup_logging
setup_logging()
logger = logging.getLogger(__name__)

# def setup_logging():
#     logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def write_jsonl(data, file_path):
    """
    Writes a list of JSON objects to a JSONL file.

    :param data: List of JSON objects.
    :param file_path: Path to the output JSONL file.
    """
    
    with open(file_path, 'w') as file:
        for record in data:
            file.write(json.dumps(record) + '\n')
    logger.info(f"Data written to {file_path}")

def validate_jsonl(file_path, schema, transfer_invalid=False):
    """
    Validates a JSONL file against a JSON schema.

    :param file_path: Path to the JSONL file.
    :param schema: JSON schema to validate against.
    :param transfer_invalid: Whether to write invalid records to a separate file.
    :return: True if validation is successful, otherwise raises a ValidationError.
    """
    json_processor = JSONFileProcessor()

    logger.info(f"Validating JSONL file: {file_path}")
    data = json_processor.read_jsonl(file_path)
    valid_data = []
    invalid_data = []
    if type(schema) == str:
        schema = json_processor.read_json(schema)

    for i, record in enumerate(data):
        try:
            validate(instance=record, schema=schema)
            
            logger.info(f"Record {i + 1} is valid.")
            valid_data.append(record)
        except jsonschema.exceptions.ValidationError as e:
            logger.error(f"Validation error in record {i + 1}: {e.message}")
            invalid_data.append(record)
    
    # Write valid records back to the original file
    write_jsonl(valid_data, file_path)

    if transfer_invalid and len(invalid_data) != 0 :
        # Write invalid records to a new file with '_invalid' appended to the name
        invalid_file_path = file_path.replace('.jsonl', '_invalid.jsonl')
        write_jsonl(invalid_data, invalid_file_path)
        logger.info(f"Invalid records written to {invalid_file_path}")

    logger.info("Validation process completed.")
    return True



def validate_jsonl_line(jsonl_line, schema):
    """
    Validates a JSONL file against a JSON schema.

    :param file_path: Path to the JSONL file.
    :param schema: JSON schema to validate against.
    :param transfer_invalid: Whether to write invalid records to a separate file.
    :return: True if validation is successful, otherwise raises a ValidationError.
    """

    logger.info(f"Validating JSONL line")
    record = jsonl_line #read_jsonl(file_path)
    # valid_data = []
    # invalid_data = []
    # if type(schema) == str:
    #     schema = read_json(schema)



    try:
        validate(instance=record, schema=schema)
        
        logger.info(f"Record is valid.")
        # valid_data.append(record)
    except jsonschema.exceptions.ValidationError as e:
        logger.error(f"Validation error in record : {e.message}")
        # invalid_data.append(record)
    
    # Write valid records back to the original file
    # write_jsonl(valid_data, file_path)



    logger.info("Validation of sinle line  process completed.")
    return True
    

def main():
    setup_logging()

    json_schema_file = "schemas/main_image.json"
    jsonl_file = "data/main_images.jsonl"
    transfer_invalid = False  # Set to False if you don't want to transfer invalid records

    # Load schema
    # schema = read_json(json_schema_file)
    
    # Validate JSONL file against schema
    try:
        validate_jsonl(jsonl_file, json_schema_file, transfer_invalid=transfer_invalid)
        logger.info("Validation process completed.")
    except jsonschema.exceptions.ValidationError:
        logger.error("Validation failed.")
    except Exception as e:
        logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
