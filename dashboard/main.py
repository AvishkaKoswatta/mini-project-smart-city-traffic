# from flask import Flask, jsonify, render_template
# from flask_cors import CORS
# import pandas as pd
# import glob

# app = Flask(__name__)
# CORS(app)  # <-- Enable CORS for all routes

# @app.route("/")
# def index():
#     return render_template("index.html")

# @app.route("/traffic")
# def traffic_data():
#     files = glob.glob("/home/avishka/data/projects/mini-project-traffic/traffic-v1/data/processed/traffic_aggregates/*.parquet")
    
#     if not files:
#         return jsonify([])
    
#     try:
#         df = pd.concat([pd.read_parquet(f) for f in files])
#         data = df.to_dict(orient="records")
#         return jsonify(data)
#     except Exception as e:
#         return jsonify({"error": str(e)}), 500

# if __name__ == "__main__":
#     app.run(debug=True, host='0.0.0.0', port=5000)



from flask import Flask, jsonify, render_template
from flask_cors import CORS
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# -------------------------------
# PostgreSQL Configuration
# -------------------------------
PG_HOST = "localhost"
PG_PORT = 5432
PG_DB = "postgres"
PG_USER = "postgres"
PG_PASSWORD = "abc"
PG_TABLE = "traffic_aggregates"

# -------------------------------
# Routes
# -------------------------------
@app.route("/")
def index():
    return render_template("index.html")

@app.route("/traffic")
def traffic_data():
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            database=PG_DB,
            user=PG_USER,
            password=PG_PASSWORD
        )

        # Use RealDictCursor to get dicts instead of tuples
        query = f"SELECT * FROM {PG_TABLE} ORDER BY window_start ASC;"
        df = pd.read_sql(query, conn)
        conn.close()

        # Convert to JSON
        data = df.to_dict(orient="records")
        return jsonify(data)

    except Exception as e:
        return jsonify({"error": str(e)}), 500

# -------------------------------
# Entry Point
# -------------------------------
if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=5000)
