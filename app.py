import os
import psycopg2  # PostgreSQL connector
import psycopg2.extras  # For RealDictCursor
from flask import Flask, render_template, request, jsonify
import requests
from io import StringIO
from faker import Faker
import math
import random
import string
from datetime import datetime, timedelta

app = Flask(__name__)
fake = Faker()

# PostgreSQL connection setup using environment variables
def get_db_connection():
    host = os.getenv("DB_HOST", "localhost")  # Default to 'localhost' if not set
    user = os.getenv("DB_USER", "flask_user")  # Default user
    password = os.getenv("DB_PASSWORD", "flask_password")  # Default password
    dbname = os.getenv("DB_NAME", "dataset_db")  # Default database name
    
    # Connect to PostgreSQL using environment variables
    return psycopg2.connect(
        host=host,
        user=user,
        password=password,
        dbname=dbname
    )

# Serve the HTML form on the root URL
@app.route('/')
def index():
    try:
        # Pagination settings (page and per_page)
        page = int(request.args.get('page', 1))
        per_page = int(request.args.get('per_page', 10))

        # Connect to PostgreSQL
        conn = get_db_connection()
        
        # Use RealDictCursor to fetch rows as dictionaries
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # Count the total number of records in the transactions table
        cursor.execute("SELECT COUNT(*) FROM transactions")
        total_records = cursor.fetchone()['count']  # Changed from 'COUNT(*)' to 'count'

        # Calculate total pages
        total_pages = math.ceil(total_records / per_page)

        # Calculate the offset for the SQL query
        offset = (page - 1) * per_page

        # Query the transactions table with LIMIT and OFFSET for pagination
        cursor.execute(
            "SELECT * FROM transactions ORDER BY date LIMIT %s OFFSET %s",
            (per_page, offset)
        )
        transactions = cursor.fetchall()

        cursor.close()
        conn.close()

        # Render the template and pass the transactions data, pagination info
        return render_template(
            'index.html',
            transactions=transactions,
            page=page,
            per_page=per_page,
            total_pages=total_pages,
            total_records=total_records
        )

    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Route to accept dataset URL
@app.route('/upload', methods=['POST'])
def upload_data():
    # Get URL from the incoming request
    url = request.json.get('url')

    if not url:
        return jsonify({"error": "No URL provided"}), 400

    try:
        # Step 1: Download the CSV dataset from the URL
        response = requests.get(url)
        if response.status_code != 200:
            return jsonify({"error": "Failed to download data from the URL"}), 400

        # Step 2: Load the JSON data
        data = response.json()

        # Step 3: Write data into PostgreSQL
        conn = get_db_connection()
        cursor = conn.cursor()

        # Iterate over the incoming JSON data and insert into PostgreSQL
        for transaction in data:
            cursor.execute(
                "INSERT INTO transactions (date, transaction_id, item, amount, location) "
                "VALUES (%s, %s, %s, %s, %s)",
                (transaction['date'], transaction['transaction_id'], transaction['item'],
                 transaction['amount'], transaction['location'])
            )

        conn.commit()
        cursor.close()
        conn.close()

        return jsonify({"message": "Data uploaded successfully!"}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

# New API endpoint to fetch all transactions
@app.route('/api/transactions', methods=['GET'])
def get_all_transactions():
    try:
        conn = get_db_connection()
        
        # Use RealDictCursor to fetch rows as dictionaries
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # Query to fetch all transactions from the database
        cursor.execute("SELECT * FROM transactions ORDER BY date")
        transactions = cursor.fetchall()

        cursor.close()
        conn.close()

        # Return the transactions as a JSON response
        return jsonify(transactions), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

# New API endpoint to delete all transactions
@app.route('/api/transactions/delete', methods=['GET'])
def delete_all_transactions():
    try:
        conn = get_db_connection()

        # Use RealDictCursor to fetch rows as dictionaries
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # Query to fetch all transactions from the database
        cursor.execute("DELETE FROM transactions")
        conn.commit()

        cursor.close()
        conn.close()

        # Return the transactions as a JSON response
        return "success", 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

# API endpoint to fetch a single transaction by ID
@app.route('/api/transaction/<transaction_id>', methods=['GET'])
def get_transaction_by_id(transaction_id):
    try:
        conn = get_db_connection()
        
        # Use RealDictCursor to fetch rows as dictionaries
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # Query to fetch a transaction by its ID
        cursor.execute("SELECT * FROM transactions WHERE transaction_id = %s", (transaction_id,))
        transaction = cursor.fetchone()

        cursor.close()
        conn.close()

        if transaction:
            return jsonify(transaction), 200
        else:
            return jsonify({"error": "Transaction not found"}), 404

    except Exception as e:
        return jsonify({"error": str(e)}), 500

# API endpoint to fetch transactions within a date range
@app.route('/api/transactions/range', methods=['GET'])
def get_transactions_by_range():
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')

    if not start_date or not end_date:
        return jsonify({"error": "Both start_date and end_date are required"}), 400

    try:
        conn = get_db_connection()
        
        # Use RealDictCursor to fetch rows as dictionaries
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # Query to fetch transactions within the date range
        cursor.execute(
            "SELECT * FROM transactions WHERE date BETWEEN %s AND %s ORDER BY date",
            (start_date, end_date)
        )
        transactions = cursor.fetchall()

        cursor.close()
        conn.close()

        return jsonify(transactions), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/generate/<int:num_transactions>', methods=['GET'])
def generate_data(num_transactions):
    data = [generate_random_transaction() for _ in range(num_transactions)]
    return jsonify(data)

# Function to generate random data
def generate_random_transaction():
    ITEMS = ['Laptop', 'Smartphone', 'Headphones', 'Keyboard', 'Monitor']
    LOCATIONS = ['New York', 'San Francisco', 'Los Angeles', 'Chicago', 'Houston']

    transaction_id = ''.join(random.choices(string.ascii_uppercase + string.digits, k=10))
    item = random.choice(ITEMS)
    amount = round(random.uniform(10, 1000), 2)  # Random amount between 10 and 1000
    location = random.choice(LOCATIONS)
    date = fake.date_this_year()  # Random date this year

    transaction = {
        "date": date.strftime('%Y-%m-%d'),
        "transaction_id": transaction_id,
        "item": item,
        "amount": amount,
        "location": location
    }

    return transaction

if __name__ == "__main__":
    # Read the port from the environment variable, default to 5001 if not set
    port = int(os.getenv("PORT", 5001))
    app.run(debug=True, port=port)