import csv
import datetime
import logging as myLog
import os
import requests
import smtplib
import pytz

# https://openweathermap.org/forecast5
FORECAST_URL = "https://api.openweathermap.org/data/2.5/forecast"

# settings from Environment
API_KEY = os.environ.get("openweather_api_key")
SENDING_EMAIL = os.environ.get("email")
PASSWORD = os.environ.get("password")

# logging
logFilePath = "default.log"
logLevel = myLog.DEBUG

myLog.basicConfig(filename=logFilePath,
                  filemode='a',
                  format='%(asctime)s - %(levelname)s - %(message)s',
                  datefmt='%d-%b-%y %H:%M:%S')

logger = myLog.getLogger(__name__)
logger.setLevel(logLevel)

logger.info('Starting')


class NoUrlFilter(myLog.Filter):
    def filter(self, record):
        print(record.getMessage())
        return 'http' not in record.getMessage()


logger.addFilter(NoUrlFilter())

# used to convert Kelvin to Celsuis
to_Celsuis = -273.15

# Load the user data
with open('data.csv') as fs:
    reader = csv.DictReader(fs)
    users = [row for row in reader]


# used to convert the epoch time to a date
def do_date(temp_date, tz):
    # logger.info(datetime.datetime.fromtimestamp(temp_date).astimezone().tzinfo)
    # print(datetime.datetime.fromtimestamp(temp_date).astimezone(pytz.timezone(tz)).tzinfo)
    return datetime.datetime.fromtimestamp(temp_date).astimezone(pytz.timezone(tz)).strftime('%Y/%d/%m %H:%M:%S')


# Get the forecast for a lat and long
def get_forecast(lat, long):
    parameters = {
        "lat": lat,
        "lon": long,
        "exclude": "current,minutely,daily",
        "appid": API_KEY
    }

    response = requests.get(url=FORECAST_URL, params=parameters)
    response.raise_for_status()

    data = response.json()

    logger.info(f'Got weather for {lat}, {long} => {response.status_code}')

    return data


# Send the message to an address
def send_message(subject, body_message, rec_email):
    body = f'''Content-type: text/html
    MIME-Version: 1.0
Subject:{subject}
{body_message}
    '''

    with smtplib.SMTP('smtp.gmail.com', 587) as connection:
        connection.starttls()
        connection.login(user=SENDING_EMAIL, password=PASSWORD)
        connection.sendmail(
            from_addr=SENDING_EMAIL,
            to_addrs=rec_email,
            msg=body)
        connection.close()

    logger.info(f'Sent email to {rec_email}')


# wrap the data into an HTML Email
def wrap_forecast(data, tz):
    results = f"""
    <h1 style="text-align:center">Weather</h1>
    <h3>Starting at {do_date(data['list'][0]['dt'], tz)}</h3>
    <h4>Sunrise {do_date(data['city']['sunrise'], tz)} - Sunset {do_date(data['city']['sunset'], tz)}</h4>
    <hr>
    """

    for item in data['list']:
        results += f"""
        <h3>{do_date(item['dt'], tz)}</h3>
        <ul>
        <li>{item['weather'][0]['main']}</li>
        <li>Temperature {item['main']['temp'] + to_Celsuis:,.2f}.</li>
        <li>Feels like {item['main']['feels_like'] + to_Celsuis:,.2f}.</li>
        <li>Low {item['main']['temp_min'] + to_Celsuis:,.2f}.</li>
        <li>High {item['main']['temp_max'] + to_Celsuis:,.2f}.</li>
        <li>Wind Speed {item['wind']['speed']}</li>

        </ul>
        """

    return results


# loop over
for user in users:
    my_weather = get_forecast(user['lat'], user['long'])
    message = wrap_forecast(my_weather, user['tz'])
    send_message('Weather', message, user['email'])

logger.info('Finished')
