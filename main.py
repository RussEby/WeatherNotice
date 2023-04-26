import csv
import datetime
import logging as myLog
import os
from psycopg2 import connect, sql
from psycopg2.extras import RealDictCursor
import pytz
import requests
import smtplib
import urllib.parse as up

# Base URL for openweather API
FORECAST_URL = "https://api.openweathermap.org/data/2.5/forecast"

# settings from Environment
API_KEY = os.environ.get("openweather_api_key")
SENDING_EMAIL = os.environ.get("email")
PASSWORD = os.environ.get("password")
DB_URL = os.environ.get("database_url")

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
    """
    Filtering class for the logging function.
    Logging normally outputs the URL that is being accessed during an API, but the URL contains the API KEY and
    I don't want that logged since it'll go into the Repo
    Inherits from logging.Filter
    """

    def filter(self, record):
        return 'http' not in record.getMessage()


# adds the NoUrlFilter to the logger
logger.addFilter(NoUrlFilter())

# used to convert Kelvin to Celsuis
to_Celsuis = -273.15


def get_users():
    """
    Retrieves the User information from the Database.

    :returns: dictionary with the users.
    """
    up.uses_netloc.append("postgres")
    ele_url = up.urlparse(DB_URL)

    conn = connect(
        user=ele_url.username,
        password=ele_url.password,
        host=ele_url.hostname,
        port=ele_url.port,
        database=ele_url.path[1:]
    )
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    cursor.execute('SELECT * FROM users WHERE active = True;')
    results = cursor.fetchall()

    conn.close()

    return results


def do_date(temp_date, tz):
    """
    Formats a epoch value into proper formatting

    :param temp_date: epoch date to be formatted
    :param tz: string: Timezone to be formatted into
    :return string: formatted date
    """
    return datetime.datetime.fromtimestamp(temp_date).astimezone(pytz.timezone(tz)).strftime('%b %d, %Y %H:%M')


def get_forecast(lat, long):
    """
    Hits the API and retrieves the 5-day forecast

    :param lat: (float) latitude of the location
    :param long: (float) longitude of the location
    :returns (dict) of the weather forecast
    """
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
    """
    preps and sends email message

    :param subject: (str) text for Subject line on email
    :param body_message: (str) text for the body of the email
    :param rec_email: (str) email address to send to
    """

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


def wrap_forecast(data, tz):
    """
    Builds the email together.

    :param data: (dict) the weather forcast
    :param tz: (str) timezone of the user
    """
    bg_colors = {
        'Clear': 'lightblue',
        'Snow': 'white',
        'Clouds': 'lightgrey',
        'Rain': 'darkgrey'
    }

    results = f"""
    <h1 style="text-align:center">Weather</h1>
    <h3>Starting at {do_date(data['list'][0]['dt'], tz)}, {tz}</h3>
    <h4>Sunrise {do_date(data['city']['sunrise'], tz)} - Sunset {do_date(data['city']['sunset'], tz)}</h4>
    <hr>
    <div style="display:flex;flex-wrap:wrap;">
    """

    for item in data['list']:
        bg_color = bg_colors.get(item['weather'][0]['main'], 'lightgreen')

        results += f"""
        <div style="width:300px;border:1px solid green;padding:3px;margin:3px;border-radius:5px;background-color:{bg_color};">
        <p style="text-align:center;">{do_date(item['dt'], tz)}</p>
        <div style="width:100%;text-align:center;padding:0;margin:0;">
        <img src="https://openweathermap.org/img/wn/{item['weather'][0]['icon']}.png"
            style="margin:0 auto;padding:0;"/>
        </div>
        <ul>
        <li>{item['weather'][0]['main']} - {item['weather'][0]['description']}</li>
        <li>Temperature {item['main']['temp'] + to_Celsuis:,.2f} C.</li>
        <li>Feels like {item['main']['feels_like'] + to_Celsuis:,.2f} C.</li>
        <li>Low {item['main']['temp_min'] + to_Celsuis:,.2f} C.</li>
        <li>High {item['main']['temp_max'] + to_Celsuis:,.2f}C.</li>
        <li>Wind Speed {item['wind']['speed']}</li>
        </ul>
        </div>
        """

    results += """</div>"""

    return results


# get user's information, place in a list of dictionary
users = [dict(user) for user in get_users()]

# loop over the users
for user in users:
    my_weather = get_forecast(user['latitude'], user['longitude'])
    message = wrap_forecast(my_weather, user['timezone'])
    send_message('Weather', message, user['email'])

logger.info('Finished')
