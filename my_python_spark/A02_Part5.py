# Additional imports required for get_key_value()
import ast

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


# ---------------------------------------
#  FUNCTION get_key_value
# ---------------------------------------
def get_key_value(line):
    # 1. We create the output variable
    res = ()

    # 2. We remove the end of line char
    line = line.replace('\n', '')

    # 3. We get the key and value
    words = line.split('\t')
    title = words[0]
    value = words[1]

    # 4. We process the value into a valid tuple type
    value = ast.literal_eval(value)

    # 4. We assign res
    res = (title, value)

    # 5. We return res
    return res


# ------------------------------------------
# FUNCTION my_reduce
# ------------------------------------------
def my_reduce(my_input_stream, my_output_stream, my_reducer_input_parameters):
    result = {}

    for item in my_input_stream:
        # Process line
        processed_entry = get_key_value(item)

        title = processed_entry[0]
        others = processed_entry[1]

        wikimedia_project = others[0]
        num_views = int(others[1])
        language = others[2]

        key = wikimedia_project + '_' + language

        # Init addition reduce in dict
        default = (title, num_views)

        # Check to see if a higher number has been found for given key
        if key in result:
            if num_views > result[key][1]:
                result[key] = (title, num_views)
        else:
            result[key] = default

    result = {k: v for k, v in sorted(result.items(), key=lambda item: item[1][1], reverse=True)}

    # Print data to file
    for key, value in result.items():
        my_output_stream.write(key + "\t" + str(value) + "\n")

# ------------------------------------------
# FUNCTION my_spark_core_model
# ------------------------------------------
def my_spark_core_model(sc, my_dataset_dir):
    inputRDD = sc.textFile(my_dataset_dir)

    # Process the raw lines into usable tuples
    mapped_lines = inputRDD.map(process_line)

    # Map to a usable format
    mapped_results = mapped_lines.map(lambda line: (line[0] + "_" + line[2], (line[1], line[3])))

    # Find the highest number of views per project and language (key)
    highest_views = mapped_results.reduceByKey(lambda x1, x2: max(x1, x2, key=lambda x: int(x[1])))

    # Sort the reduced tuples by total run-outs in descending order
    sorted_tuples = highest_views.sortBy(lambda x: x[1][1], ascending=False)

    # Collect the totals
    result = sorted_tuples.collect()

    # # Print the results
    for item in result:
        print(str(item[0]) + "\t" + str(item[1]))

# ------------------------------------------
# FUNCTION my_spark_streaming_model
# ------------------------------------------
def my_spark_streaming_model(ssc, monitoring_dir):
    # input stream textFileStream
    input_stream = ssc.textFileStream(monitoring_dir)

    # Process the raw lines into usable tuples
    mapped_lines = input_stream.map(process_line)

    # Map the processed lines into a usable format
    usable_map = mapped_lines.map(lambda line: (line[0] + "_" + line[2], (line[1], line[3])))

    # Reduce by key to get the web page with the highest views
    highest_views = usable_map.reduceByKey(lambda x1, x2: max(x1, x2, key=lambda x: int(x[1])))

    # Transform to sort results in descending order
    final_stream = highest_views.transform(lambda rdd: rdd.sortBy(lambda item: item[1][1], ascending=False))

    # Persist
    final_stream.persist(pyspark.StorageLevel.MEMORY_AND_DISK)

    # pprint
    final_stream.pprint()
