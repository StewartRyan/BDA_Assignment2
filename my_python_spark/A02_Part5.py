# ------------------------------------------
# FUNCTION my_map
# ------------------------------------------
def my_map(my_input_stream, my_output_stream, my_mapper_input_parameters):
    result = {}

    for line in my_input_stream:
        # Process line
        processed_line = process_line(line)

        # Assign all delimited values
        wikimedia_project = processed_line[0]
        web_page_name = processed_line[1]
        language = processed_line[2]
        num_views = int(processed_line[3])

        # Add to the dictionary, using web page name as key
        result[web_page_name] = (wikimedia_project, num_views, language)

    # Print data to file
    # e.g: "Rafael Nadal" ('Wikipedia', 25, 'Spanish')
    # e.g: "The Netherlands" ('WikiVoyage', 5, 'English')
    for key, value in result.items():
        my_output_stream.write(key + "\t" + str(value) + "\n")


# ------------------------------------------
# FUNCTION my_reduce
# ------------------------------------------
def my_reduce(my_input_stream, my_output_stream, my_reducer_input_parameters):
    result = {}

    for item in my_input_stream:
        # Process line
        # NOTE: get_key_value() returns a tuple with a key and value
        # where the value is another tuple: e.g: ('WikiVoyage', 6, 'Dutch')
        processed_entry = get_key_value(item)

        # Extrapolate web page name
        web_page_name = processed_entry[0]
        others = processed_entry[1]

        # Extrapolate the relevant delimited values
        wikimedia_project = others[0]
        num_views = int(others[1])
        language = others[2]

        # Make a key by which we want to determine our results
        key = wikimedia_project + '_' + language

        # Set a default for if there is no entry for given key
        default = (web_page_name, num_views)

        # Get entry if it exists, otherwise set default
        entry = result.get(key, default)

        # Add the number of views together
        result[key] = (entry[0], entry[1] + num_views)

    # Sort the results in decreasing order
    result = {k: v for k, v in sorted(result.items(), key=lambda x: x[1][1], reverse=True)}

    # Print data to file
    # e.g: WikiBooks_Dutch	('Land Ho!', 431)
    # e.g: WikiVoyage_German ('Hawaii', 339)
    for key, value in result.items():
        my_output_stream.write(key + "\t" + str(value) + "\n")

# ------------------------------------------
# FUNCTION my_spark_core_model
# ------------------------------------------
def my_spark_core_model(sc, my_dataset_dir):
    pass

# ------------------------------------------
# FUNCTION my_spark_streaming_model
# ------------------------------------------
def my_spark_streaming_model(ssc, monitoring_dir):
    pass
