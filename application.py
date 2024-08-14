from flask import Flask
from functions import upload_file,hires_per_quarter_2021,hires_greater_than_mean_2021

app = Flask(__name__)

@app.route('/')
def index():
    text = 'Hello! These are your available options: <br/><br/>\
            <b>/insert records:</b> insert csv files.<br/><br/>\
            <b>/get_hires_per_quarter_2021:</b> returns the number of employees hired for each job and department in 2021 divided by quarter ordered alphabetically by department and job.<br/><br/>\
            <b>/get_hires_greater_than_mean_2021:</b> returns a list of ids, name and number of employees hired of each department that hired more employees than the mean of employees hired in 2021 for all the departments, ordered by the number of employees hired (descending).'
    return text

@app.route('/insert_records')
def insert_records():
    try:
        upload_file()
        return 'Carga completada.'
    except Exception as err:
        return 'Carga fallida.<br/><br/>', str(err)
    
@app.route('/get_hires_per_quarter_2021')
def get_hires_per_quarter_2021():
    try:
        response = hires_per_quarter_2021()
        return response
    except Exception as err:
        return str(err)

@app.route('/get_hires_greater_than_mean_2021')
def get_hires_greater_than_mean_2021():
    try:
        response = hires_greater_than_mean_2021()
        return response
    except Exception as err:
        return str(err)

if __name__ == "__main__":
    app.run()