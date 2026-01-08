from flask import Flask, jsonify, render_template
from flask_cors import CORS
import pandas as pd
import glob

app = Flask(__name__)
CORS(app)  # <-- Enable CORS for all routes

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/traffic")
def traffic_data():
    files = glob.glob("/home/avishka/data/projects/mini-project-traffic/traffic-v1/data/processed/traffic_aggregates/*.parquet")
    
    if not files:
        return jsonify([])
    
    try:
        df = pd.concat([pd.read_parquet(f) for f in files])
        data = df.to_dict(orient="records")
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=5000)
