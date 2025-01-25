import pandas as pd

class ETLWorker:
    def __init__(self, input_file, output_file):
        self.input_file = input_file
        self.output_file = output_file

    def extract(self):
        """Extract data from a CSV file."""
        try:
            data = pd.read_csv(self.input_file)
            print(f"Data extracted from {self.input_file}")
            return data
        except Exception as e:
            print(f"Error during extraction: {e}")
            return None

    def transform(self, data):
        """Transform data (example: filter, clean, etc.)."""
        try:
            # Example transformation: Remove rows with missing values
            transformed_data = data.dropna()
            print(f"Data transformed (removed missing values)")
            return transformed_data
        except Exception as e:
            print(f"Error during transformation: {e}")
            return None

    def load(self, data):
        """Load data to a new CSV file."""
        try:
            data.to_csv(self.output_file, index=False)
            print(f"Data loaded to {self.output_file}")
        except Exception as e:
            print(f"Error during loading: {e}")

    def run(self):
        """Run the ETL process."""
        data = self.extract()
        if data is not None:
            transformed_data = self.transform(data)
            if transformed_data is not None:
                self.load(transformed_data)

# Example usage:
if __name__ == "__main__":
    etl_worker = ETLWorker("input_data.csv", "output_data.csv")
    etl_worker.run()
