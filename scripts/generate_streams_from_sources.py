import yaml
import os
import sys
import inspect
import time

# TODO: Break apart into functions

# Declare initial vars
source_root_path = os.path.join("models","sources")
model_root_path = os.path.join("models","bronze")
model_root_prefix = "bronze"
model_file_list = []


# Declare and start the stopwatch
stopwatchstart = time.time()
stopwatchend = ""
stopwatchtotalsec = ""



# Get the file list
# Declare the file list
yml_file_list = []

# Get each file from source file path
for source_filename in os.listdir(source_root_path):
    source_file_path = os.path.join(source_root_path,source_filename)
    
    if os.path.isfile(source_file_path):
        yml_file_list.append(source_file_path)



# Iterate through files
# Declare file list iterator
yml_files_iter = iter(yml_file_list)

# Iterate through files
for file in yml_file_list:
    # Get the next file from the iterator
    yml_file = next(yml_files_iter)

    # Load the file contents
    with open(yml_file,'r') as yml_stream:
        try:
            yml_contents = yaml.safe_load(yml_stream)
        except yaml.YAMLError as yml_load_exception:
            raise

    # Parse the source details
    for source in yml_contents['sources']:
        # Parse the source details
        source_name = source['name']
        source_database = source['database']
        source_schema = source['schema']

        # Create the target schema folder (snake case) if it doesn't exist
        model_child_path = os.path.join(model_root_path,source_name.replace("-","_"))
        model_child_prefix = model_root_prefix + "-" + source_name.replace("-","_")

        if not os.path.exists(model_child_path):
            try:
                os.makedirs(model_child_path)
            except OSError as makedir_exception:
                raise

        
        # Parse the source table details
        for table in source['tables']:
            # Skip the source table if tagged as excluded
            ## Looks for the excluded tag in the list of available tags and skips, else passes the check
            try:
                if any('is_excluded' in tag for tag in table['tags']):
                    continue
            except KeyError:
                pass
            
            # Set the table_name value
            source_table_name = table['name']

            # Set the complete dbt source relation string
            source_relation_string = "source('" + source_name + "','" + source_table_name + "')"
            
            # Form the contents of the model file
            sql_statement = """
                            {{# Auto-generated stream #}}
                            {{# WARNING: Any manual changes to this model may be overwritten by automated processes. #}}
                            
                            {{# Model configuration #}}
                            {{{{ config(materialized='stream',
                                        stream_source_name='{source_name}',
                                        stream_source_table_name='{source_table_name}') }}}}
                            
                            {{# Model definition #}}
                            --depends_on: {{{{ {source_relation_string} }}}}
                            """.format(source_name=source_name,
                                       source_table_name=source_table_name,
                                       source_relation_string=source_relation_string)

            # Clean up any unnecessary whitespace in the file contents
            file_contents = inspect.cleandoc(sql_statement)

            # Form the complete target path
            filename = model_child_prefix + "-" + source_table_name + "__ct.sql"
            out_file = os.path.join(model_child_path,filename)

            # Write out the file
            with open(out_file,"w") as file_target:
                file_target.write(file_contents)

            # Add to list of generated files
            model_file_list.append(filename)

            

# Stop the stopwatch counter
stopwatchend = time.time()
stopwatchtotalsec = round(stopwatchend - stopwatchstart, 2)

# Print summary
print("=" * 80) # Separator
print("RESULT: Generated " + str(len(model_file_list)) + " stream models in " + str(stopwatchtotalsec) + " seconds.")
