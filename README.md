Flight Delay Prediction Pipeline V2
Use of **Python+Pyspark+ML+Kafka+DB**
The pipeline leverages Kafka for streaming, PySpark for scalable processing, and SQLite for persistent storage, with Plotly visualizations and real-time delay alerts.
The Flight Delay Prediction Pipeline V2 is a real-time data processing system that generates synthetic flight data, trains a machine learning model to predict flight delays, streams data via Kafka, and stores predictions in SQLite. Built with Python 3.9.13, PySpark, and Kafka, it simulates flight scenarios, predicts delays based on weather and scheduling, and provides statistical insights.

Project Overview
This project demonstrates a streaming data pipeline for flight delay prediction:
Data Generation: Creates synthetic flight data (e.g., 1000 records in flight_data_sample.json) with attributes like flight number, airports, weather, and delays.
Model Training: Trains a RandomForestClassifier using PySpark MLlib to predict delays (>15 minutes) based on features like temperature and wind speed.
Real-Time Streaming: Streams flight data via Kafka, applies the trained model, and stores predictions in SQLite (//flight_predictions.db).
Analytics: Calculates delay statistics (e.g., 24.03% delay rate for x airport) and model accuracy.

File_Structure (this project uses D:/)
├── kafka_control.py               # Manages Kafka/Zookeeper servers
├── generate_flight_data.py        # Generates synthetic flight data
├── train_flight_model.ipynb       # Trains RandomForestClassifier
├── flight_pipeline.ipynb          # Streams data, predicts, and analyzes
├── flight_data_sample.json        # Synthetic dataset (1000 records)
├── flight_db/
│   ├── flight_predictions.db      # SQLite database for predictions
│   ├── flight.sqbpro              #db file
├── flight_delay_model/
│   ├── Metadata/   
│   ├── Stages/

Contributions and suggestions are welcome!

License
This project is licensed under the MIT License.
Contact ankur3103verma@gmail.com

Pls refer to project documentation : Flight_Delay_Prediction_V2.docx
