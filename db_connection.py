import mysql.connector
from mysql.connector import Error


def create_connection():
    try:
        connection = mysql.connector.connect(
            host = 'localhost',
            user = 'root',
            password = 'Tigersingh',
            database = 'SEC_Docs'
        )

        if connection.is_connected():
            print("Connected to MySQL Database")
            return connection
    except Error as e:
        print(f"Error Connection to MySQL: {e}")
        return None

def close_connection(connection):
    if connection.is_connected():
        connection.close()
        print("MySQL connection closed")

# # Example usage (to test):
# if __name__ == "__main__":
#     conn = create_connection()
#     if conn:
#         close_connection(conn)