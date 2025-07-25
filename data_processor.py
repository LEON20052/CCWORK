from utils.dataframe_operations import remove_nulls, remove_duplicates, standardize_numeric
import pandas as pd
import json
import io

class DataProcessor:
    def process_file(self, file, options_json):
        """Process the uploaded file according to specified options."""
        options = json.loads(options_json)
        
        # Read the file
        content = file.read()
        df = pd.read_csv(io.BytesIO(content))
        
        # Apply preprocessing
        if options.get('removeNulls'):
            df = remove_nulls(df)
        if options.get('removeDuplicates'):
            df = remove_duplicates(df)
        if options.get('standardize'):
            df = standardize_numeric(df)
            
        # Save processed data
        output_path = 'processed_data.csv'
        df.to_csv(output_path, index=False)
        
        return {
            'preview': df.head().to_dict(),
            'downloadUrl': f'/download/{output_path}'
        }