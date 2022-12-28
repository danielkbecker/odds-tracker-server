from sqlalchemy import create_engine
import hashlib
import boto3
from io import StringIO


# ***** MATH HELPERS *****
# Rounds the odds to the lowest hour/min where is a multiple of x. I'm using 15 minute scraper intervals in this script.
# Example Inputs/Outputs:
#    1. num = 5, x = 15 returns 0
#    2. num = 16, x = 15 returns 15
#    3. num = 15, x = 15 returns 15
#    4. num = 0, x = 15 returns 0
def round_down(num, divisor):
    return num - (num % divisor)


# This function is essentially used to flag duplicate rows in a given dataframe.It returns a new dataframe
# which is the original dataframe, plus a new column called "hash". Hash is a 256-bit hash of each row's data.
# Example Inputs/Outputs:
#    1. passed_df = pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]}), columns = ['a', 'b']), returns
#    2. passed_df = pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6], 'c': [7, 8, 9]}), columns = ['a', 'b', 'c']), returns
def sha_256_hash(passed_df, columns, name="hash"):
    # Copy the dataframe
    new_df = passed_df.copy()

    # Lambda function to perform the hashing
    def func(row, cols):
        col_data = []
        for col in cols:
            col_data.append(str(row.at[col]))

        col_combined = ''.join(col_data).encode()
        hashed_col = hashlib.sha256(col_combined).hexdigest()
        return hashed_col

    # Hash each row, and add the hash to the new dataframe
    new_df[name] = new_df.apply(lambda row: func(row, columns), axis=1)
    return new_df


# ***** Database HELPERS *****
# Save any rows data to a MySQL database.
def save_diff_to_mysql(username, password, db_endpoint, db_port, db_name, dataframe, temp_name, target_table_name):
    db_url = "mysql+pymysql://" + username + ":" + password + "@" + db_endpoint + ":" + db_port + "/" + db_name
    engine = create_engine(db_url)
    total_rowcount = 0
    with engine.connect() as connection:
        if not dataframe.empty:
            try:
                # frame = dataframe before change
                dataframe.to_sql(target_table_name, connection, index=False)
            except ValueError:
                # The table already exists, so we need to append only new data to the existing table.
                # temp_frame = dataframe before change
                dataframe.to_sql(temp_name, connection, if_exists='replace')
                # This will break if the table does not exist
                # before_rows_query = "SELECT COUNT(*) FROM " + target_table_name
                # before_rows = connection.execute(before_rows_query).fetchone()[0]
                sql_string = "INSERT INTO " + target_table_name + " (" + ", ".join(
                    dataframe.columns.tolist()) + ") " + "SELECT " + ", ".join(
                    dataframe.columns.tolist()) + " FROM " + temp_name + \
                                                  " WHERE (" + "hash" + ")" \
                                                  " NOT IN (SELECT " + "hash" + " FROM " + target_table_name + ")"
                diff = connection.execute(sql_string)
                # after_rows_query = "SELECT COUNT(*) FROM " + target_table_name
                # after_rows = connection.execute(after_rows_query).fetchone()[0]
                print(str(diff.rowcount) + ' row(s) added to ' + target_table_name)
                total_rowcount += diff.rowcount
            except Exception as ex:
                print(ex)
            else:
                print(f"New table {target_table_name} created successfully.")
    return total_rowcount


# Function saves a pandas dataframe to a s3 bucket.
def save_diff_to_s3(dataframe, timestamp, sport, bet_type, aws_access_key, aws_secret_key):
    bucket = 'oddstracker'
    # Need the date and time rounded down to the nearest 15 min interval here
    month = str(timestamp.month)
    day = str(timestamp.day)
    hour = str(timestamp.hour)
    minute = timestamp.minute
    # Not sure whether to store by bet_type or day first.
    filename = 'scraped_data' + '/' + sport + '/' + bet_type + '/' + month + '/' + day + '/' + hour + ':' \
               + str(round_down(minute, 15)) + '.csv'
    csv_buffer = StringIO()
    # odds_dataframe = dataframe...
    dataframe.to_csv(csv_buffer, index=False)
    s3csv = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key)
    s3csv.put_object(Body=csv_buffer.getvalue(), Bucket=bucket, Key=filename)
    # print(response)
    # print(csv_buffer.getvalue())
    # new_rows = s3csv.put_object(Body=csv_buffer.getvalue(), Bucket=bucket, Key=filename)
    # len(new_rows.get_object(Bucket=bucket, Key=filename)['Body'].read().decode('utf-8').splitlines())
