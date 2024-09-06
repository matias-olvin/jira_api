#!bin/bash

set -exo pipefail
trap 'echo "Error: $BASH_COMMAND failed"' ERR
num_digits=2
#Task 0: Clean and ingest from GCS
clean_and_ingest() {
    dirname=$1
    filename=$2
    bucket=$3
    prefix=$4

    sudo rm -rf "$filename*" "$dirname*"
    sudo mkdir -p "$dirname"
    sudo gsutil -m cp -r gs://$bucket/$prefix/* "$dirname/"

    echo "Starting concatenation"
    sudo rm -rf "$dirname/shell*"
    sudo mkdir -p "$dirname/shell"
    sudo bash -c "awk '(NR == 1) || (FNR > 1)' $dirname/*.csv > $dirname/shell/$filename.csv"
    echo "Finished concatenation"
    sudo chmod -R 777 $dirname/

    # Convert dirname to lowercase for comparison
    dirname_lower=$(echo "$dirname" | tr '[:upper:]' '[:lower:]')
    if [[ "$dirname_lower" =~ visits|ranking|demographic|compatibility ]]; then
        echo "Starting transform"
        awk 'BEGIN{FS=OFS="\t"} {gsub(/\[/,"{",$0); gsub(/\]/,"}",$0); print}' $dirname/shell/$filename.csv > $dirname/shell/temp_file && mv $dirname/shell/temp_file $dirname/shell/$filename.csv
        # sudo bash -c "awk 'BEGIN{FS=OFS=\"\t\"} {gsub(/\\[/,\"{\",$0); gsub(/]/,\"}\",$0); print}' $dirname/shell/$filename.csv > $dirname/shell/temp_file && mv $dirname/shell/temp_file $dirname/shell/$filename.csv"
        #sudo bash -c "awk 'BEGIN{FS=OFS=\"\t\"} {gsub(/\\[/,\"{\",$0); gsub(/\\]/,\"}\",$0); print}' $dirname/shell/$filename.csv > $dirname/shell/temp_file && mv $dirname/shell/temp_file $dirname/shell/$filename.csv"
        #sudo bash -c "awk 'BEGIN{FS=OFS="\t"} {gsub(/\[/,"{",$0); gsub(/\]/,"}",$0); print}' $dirname/shell/$filename.csv > $dirname/shell/temp_file && mv $dirname/shell/temp_file $dirname/shell/$filename.csv"
        echo "Finished transform"
    fi

}

# Task 1: Split CSV file into n files
split_csv() {
    dirname=$1
    filename=$2
    n=$3
    suffix=$4
    
    # Extract header row and store it in header.txt
    sudo bash -c "head -n 1 "$dirname/shell/$filename.csv" > $dirname/shell/header.txt"
    sudo rm -rf $dirname/shell/temp.csv "$dirname/shell/$filename.$suffix*"
    # Remove header row from the original csv file
    sudo bash -c "tail -n +2 "$dirname/shell/$filename.csv" > $dirname/shell/temp.csv"
    
    # Split the original csv file into n files
    total_lines=$(wc -l < $dirname/shell/temp.csv)
    lines_per_file=$(( (total_lines + n - 1) / n ))  # Round up division

    echo "Starting splitting files"
    suffix_split=$((total_lines/lines_per_file))
    {% raw %}
    num_digits=${#suffix_split}
    {% endraw %}
    sudo split -l $lines_per_file -d -a ${num_digits} $dirname/shell/temp.csv "$dirname/shell/$filename.$suffix"
        
    # Get the actual number of split files created (Non csv, non cmd)
    actual_split_files=$(ls $dirname/shell/$filename.$suffix* | grep -vE '\.csv$|\.cmd$' | wc -l)

    # Add header from header.txt at the top of each of the n files
    for i in $(seq -f "%0${num_digits}g" 0 $((actual_split_files - 1))); do
        sudo mv "$dirname/shell/$filename.$suffix$i" "$dirname/shell/$filename.$suffix$i.csv"
        sudo bash -c "cat $dirname/shell/header.txt "$dirname/shell/$filename.$suffix$i.csv" > $dirname/shell/temp && mv $dirname/shell/temp "$dirname/shell/$filename.$suffix$i.csv""
    done
    echo "Finished splitting files"
    # Clean up temporary files
    sudo rm $dirname/shell/temp.csv
}

# Task 2: Run psql copy command to split table into n parts
split_table() {
    table_name=$1
    n=$2
    hostname=$3
    password=$4
    dirname=$5
    filename=$6
    suffix=$7
    temp_table_name="${table_name}_temp"

    echo "Starting unlogged temp table creation"
    PGPASSWORD=$password psql -h $hostname -U almanac_import -d almanac -c "CREATE UNLOGGED TABLE IF NOT EXISTS public.$temp_table_name (LIKE public.$table_name);"
    
    # Get the actual number of split files created (Only csv)
    actual_split_files=$(ls $dirname/shell/$filename.$suffix* | grep '\.csv$' | wc -l)

    for i in $(seq -f "%0${num_digits}g" 0 $((actual_split_files - 1))); do
        PGPASSWORD=$password psql -h $hostname -U almanac_import -d almanac -c "DROP TABLE IF EXISTS public.$temp_table_name$i;"
        PGPASSWORD=$password psql -h $hostname -U almanac_import -d almanac -c "CREATE UNLOGGED TABLE IF NOT EXISTS public.$temp_table_name$i (LIKE public.$temp_table_name);"
    done
    echo "Finished unlogged temp table creation"
}

# Task 3: Run psql copy commands in parallel
run_copy_commands() {
    dirname=$1
    suffix=$2
    filename=$3
    table_name=$4
    n=$5  # Detect number of CPU cores
    hostname=$6
    password=$7

    temp_table_name="${table_name}_temp"
    sudo chmod -R 777 $dirname/
    
    # Read the column headers from the file
    headers=$(head -n 1 "$dirname/shell/header.txt" | sed 's/\t/,/g')
    echo "Starting copy into unlogged temp tables"

    # Get the actual number of split files created (Only csv)
    actual_split_files=$(ls $dirname/shell/$filename.$suffix* | grep '\.csv$' | wc -l)

    for i in $(seq -f "%0${num_digits}g" 0 $((actual_split_files - 1))); do
        #sudo bash -c "echo \"PGPASSWORD=$password psql -h $hostname -U almanac_import -d almanac -c '\\\copy $temp_table_name$i FROM $dirname/shell/$filename.$suffix$i.csv DELIMITER E'\t' CSV HEADER;'\" > $dirname/shell/$filename.$suffix$i.cmd"
        echo "PGPASSWORD=$password psql -h $hostname -U almanac_import -d almanac -c \"\copy $temp_table_name$i($headers) FROM $dirname/shell/$filename.$suffix$i.csv DELIMITER E'\t' CSV HEADER;\"" > $dirname/shell/$filename.$suffix$i.cmd
    done
    sudo chmod -R 777 $dirname/
    sudo bash -c "parallel -j $n -q bash {} ::: $(ls "$dirname/shell/$filename.$suffix"*.cmd)"
    sudo rm "$dirname/shell/$filename.$suffix"*.cmd
    echo "Finished copy into unlogged temp tables"
}

# Task 4: Union all tables into a single table
union_tables() {
    table_name=$1
    temp_table_name="${table_name}_temp"
    n=$2
    hostname=$3
    password=$4
    dirname=$5
    filename=$6
    suffix=$7

    PGPASSWORD=$password psql -h $hostname -U almanac_import -d almanac -c "DROP TABLE IF EXISTS public.$temp_table_name;"
    sql_script="BEGIN;\n"

    # Get the actual number of split files created (Only csv)
    actual_split_files=$(ls $dirname/shell/$filename.$suffix* | grep '\.csv$' | wc -l)

    for i in $(seq -f "%0${num_digits}g" 0 $((actual_split_files - 1))); do
        if [ $i -eq 0 ]; then
            sql_script+="CREATE UNLOGGED TABLE IF NOT EXISTS public.$temp_table_name AS SELECT * FROM $temp_table_name$i;\n"
        else
            sql_script+="INSERT INTO public.$temp_table_name SELECT * FROM $temp_table_name$i;\n"
        fi
    done
    sql_script+="COMMIT;"
    echo -e "$sql_script" > union_script.sql

    echo "Starting Union of unlogged temp tables"
    PGPASSWORD=$password psql -h $hostname -U almanac_import -d almanac -f union_script.sql
    sudo rm union_script.sql
    echo "Finished Union of unlogged temp tables"
    # for i in $(seq -f "%0${num_digits}g" 0 $((n - 1))); do
    #     if [ $i -eq 0 ]; then
    #         PGPASSWORD=Z2AHgfwpZkSe9cohc4JM5KECCEk psql -h 34.86.25.48 -U almanac_import -d almanac -c "CREATE UNLOGGED TABLE IF NOT EXISTS public.$temp_table_name AS SELECT * FROM $temp_table_name$i;"
    #     else
    #         PGPASSWORD=Z2AHgfwpZkSe9cohc4JM5KECCEk psql -h 34.86.25.48 -U almanac_import -d almanac -c "INSERT INTO public.$temp_table_name SELECT * FROM $temp_table_name$i;"
    #     fi
    # done
}

# Task 4: Merge temp table into actual table
merge_tables() {
    table_name=$1
    hostname=$2
    password=$3

    temp_table_name="${table_name}_temp"
    test_table_name="${table_name}_test"

    echo "Starting insert into true table"
    # PGPASSWORD=$password psql -h $hostname -U almanac_import -d almanac -c "INSERT INTO public.$test_table_name SELECT * FROM $temp_table_name;"
    PGPASSWORD=$password psql -h $hostname -U almanac_import -d almanac -c "INSERT INTO public.$table_name SELECT * FROM $temp_table_name;"
    echo "Finished insert into true table"

}

# Task 4: Drop temp tables
drop_tables() {
    table_name=$1
    n=$2
    hostname=$3
    password=$4
    dirname=$5
    filename=$6
    suffix=$7

    temp_table_name="${table_name}_temp"
    echo "Starting to drop temp tables"
    PGPASSWORD=$password psql -h $hostname -U almanac_import -d almanac -c "DROP TABLE IF EXISTS public.$temp_table_name;"
    
    # Get the actual number of split files created (Only CSV)
    actual_split_files=$(ls $dirname/shell/$filename.$suffix* 2>/dev/null | grep '\.csv$' | wc -l || echo $n)

    for i in $(seq -f "%0${num_digits}g" 0 $((actual_split_files - 1))); do
        PGPASSWORD=$password psql -h $hostname -U almanac_import -d almanac -c "DROP TABLE IF EXISTS public.$temp_table_name$i;"
    done
    echo "Finished to drop temp tables"
}

# Example usage:
echo "{{ params['DIRNAME'] }}"
echo {{ params['DIRNAME'] }}
echo "{{ params['FILENAME'] }}"
echo {{ params['FILENAME'] }}

drop_tables {{ params['TABLE'] }} $(nproc) {{ params['HOSTNAME'] }} {{ params['PASSWORD'] }}
clean_and_ingest {{ params['DIRNAME'] }} {{ params['FILENAME'] }} {{ params['staging_bucket'] }} {{ params['GCS_PREFIX'] }}
split_csv {{ params['DIRNAME'] }} {{ params['FILENAME'] }} $(nproc) {{ params['FILE_SUFFIX'] }}
split_table {{ params['TABLE'] }} $(nproc) {{ params['HOSTNAME'] }} {{ params['PASSWORD'] }} {{ params['DIRNAME'] }} {{ params['FILENAME'] }} {{ params['FILE_SUFFIX'] }}
run_copy_commands {{ params['DIRNAME'] }} {{ params['FILE_SUFFIX'] }} {{ params['FILENAME'] }} {{ params['TABLE'] }} $(nproc) {{ params['HOSTNAME'] }} {{ params['PASSWORD'] }}
union_tables {{ params['TABLE'] }} $(nproc) {{ params['HOSTNAME'] }} {{ params['PASSWORD'] }} {{ params['DIRNAME'] }} {{ params['FILENAME'] }} {{ params['FILE_SUFFIX'] }}
merge_tables {{ params['TABLE'] }} {{ params['HOSTNAME'] }} {{ params['PASSWORD'] }}
drop_tables {{ params['TABLE'] }} $(nproc) {{ params['HOSTNAME'] }} {{ params['PASSWORD'] }} {{ params['DIRNAME'] }} {{ params['FILENAME'] }} {{ params['FILE_SUFFIX'] }}
sudo rm -rf *