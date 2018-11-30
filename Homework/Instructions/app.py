import numpy as np
import pandas as pd
import datetime as dt

# Python SQL toolkit and Object Relational Mapper
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

from flask import Flask, jsonify
from sqlalchemy.pool import StaticPool

# Create Engine.
#engine = create_engine("sqlite:///Resources/hawaii.sqlite")

engine = create_engine("sqlite:///Resources/hawaii.sqlite",
   connect_args={'check_same_thread':False},
   poolclass=StaticPool)
# Reflect an existing database into a new model
Base = automap_base()
# Reflect the tables
Base.prepare(engine, reflect=True)
# Save references to each table
Measurement = Base.classes.measurement
Station = Base.classes.station
# Create our session (link) from Python to the DB
session = Session(engine)

app = Flask(__name__)



@app.route("/")
def main():
    """List all available api routes."""
    return (
        f"<h1>WELCOME TO THE HAWAII WEATHER API</h1>"
        f"<br>"
        F"<br>"
        f"Available API Routes:<br>"
        f"<br>"
        f"<br>" 
        f"<strong>/api/v1.0/precipitation</strong><br>"
        f"- Display the prior year rain totals from all stations"
        f"<br>"
        f"<br>" 
        f"<strong>/api/v1.0/stations</strong><br>"
        f"- List of Station numbers and names"
        f"<br>"
        f"<br>" 
        f"<strong>/api/v1.0/tobs</strong><br>"
        f"- List of prior year temperatures from all stations"
        f"<br>"
        f"<br>"       
        f"<strong>/api/v1.0/start</strong><br>"
        f"- When given the start date (YYYY-MM-DD), calculates the MIN/AVG/MAX temperature for all dates greater than and equal to the start date"
        f"<br>"
        f"<br>"
        f"<strong>/api/v1.0/start/end</strong><br>"
        f"- Provide the start date and end date in the YYYY-MM-DD format." 
        f"- Return the MIN/AVG/MAX temperature for dates between the start and end date inclusive"
    )
@app.route("/api/v1.0/precipitation")
def precipitation():
    """Return a list of rain fall for prior year"""
#    * Query for the dates and precipitation observations from the last year.
#           * Convert the query results to a Dictionary using `date` as the key and `prcp` as the value.
#           * Return the json representation of your dictionary.
    last_date = session.query(Measurement.date).order_by(Measurement.date.desc()).first()
    last_year = dt.date(2017, 8, 23) - dt.timedelta(days=365)
    rain = session.query(Measurement.date, Measurement.prcp).\
        filter(Measurement.date > last_year).\
        order_by(Measurement.date).all()
# Create a list of dicts with `date` and `prcp` as the keys and values
    rain_totals = []
    for result in rain:
        row = {}
        row["date"] = rain[0]
        row["prcp"] = rain[1]
        rain_totals.append(row)

    return jsonify(rain_totals)

@app.route("/api/v1.0/stations")
def stations():
    stations_query = session.query(Station.name, Station.station)
    stations = pd.read_sql(stations_query.statement, stations_query.session.bind)
    return jsonify(stations.to_dict())

@app.route("/api/v1.0/tobs")
def tobs():
    """Return a list of temperatures for prior year"""
#  * query for the dates and temperature observations from a year from the last data point.
#  * Return a JSON list of Temperature Observations (tobs) for the previous year.
    
    
    lastdate = session.query(Measurement.date).order_by(Measurement.date.desc()).first()
    lastdate_str = str(lastdate)[2:-3]
    last_year = str(eval(lastdate_str[0:4])-1) + lastdate_str[4:]
    temperature = session.query(Measurement.date, Measurement.tobs).\
        filter(Measurement.date > last_year).\
        order_by(Measurement.date).all()

    temperatures = []
    for temp in temperature:
        row = {}
        row["date"] = temperature[0]
        row["tobs"] = temperature[1]
        temperatures.append(row)

    return jsonify(temperatures)

#  * Return a JSON list of the minimum temperature, the average temperature, and the max temperature for a given start or start-end range.
#  * When given the start only, calculate `TMIN`, `TAVG`, and `TMAX` for all dates greater than and equal to the start date.
#  * When given the start and the end date, calculate the `TMIN`, `TAVG`, and `TMAX` for dates between the start and end date inclusive.

@app.route("/api/v1.0/<start>")
@app.route("/api/v1.0/<start>/<end>")
def start_end(start=None, end=None):
    """Return a list of min, avg, max for specific dates"""
    functions = [func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)]
    if not end:
        results= session.query(*functions).filter(Measurement.date >= start).all()
        temps = list(np.ravel(results))
        return jsonify(temps)
    
    results = session.query(*functions).filter(Measurement.date >= start).filter(Measurement.date <= end).all()
    temps2 = list(np.ravel(results))
    return jsonify(temps2)    

    
if __name__ == "__main__":
    app.run(debug=True)