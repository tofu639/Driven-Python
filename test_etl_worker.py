import pytest
import pandas as pd
from etl_worker import ETLWorker

def test_etl_worker():
    # Create a sample dataframe to test the transformation
    input_data = pd.DataFrame({
        'column1': [1, 2, None, 4],
        'column2': ['a', 'b', 'c', 'd']
    })

    # Save the dataframe as a CSV file (simulating input)
    input_file = "test_input_data.csv"
    input_data.to_csv(input_file, index=False)

    # Initialize ETLWorker
    etl_worker = ETLWorker(input_file, "test_output_data.csv")

    # Run the ETL process
    etl_worker.run()

    # Load the output data
    output_data = pd.read_csv("test_output_data.csv")

    # Assert if missing values are dropped
    assert output_data.shape[0] == 3  # There should be 3 rows after dropping None

if __name__ == "__main__":
    pytest.main()
