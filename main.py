import csv
import datetime
import logging
import os
from psycopg2 import connect, sql
from psycopg2.extras import RealDictCursor
import pytz
import requests
import smtplib
import urllib.parse as up


class NoUrlFilter(logging.Filter):
    """
    Filtering class for the logging function.
    Logging normally outputs the URL that is being accessed during an API, but the URL contains the API KEY and
    I don't want that logged since it'll go into the Repo
    Inherits from logging.Filter
    """

    def filter(self, record):
        return 'http' not in record.getMessage()


def logging_setup(filter_fun, log_file_path="default.log"):
    """
    Set up the logging system

    :param filter_fun: (filter Class) A filter to apply the logging system
    :param log_file_path: (str, optional) Filename to place the logging
    :returns: filter object to use for logging
    """
    log_level = logging.DEBUG

    logging.basicConfig(filename=log_file_path,
                        filemode='a',
                        format='%(asctime)s - %(levelname)s - %(message)s',
                        datefmt='%d-%b-%y %H:%M:%S')

    logger_ = logging.getLogger(__name__)
    logger_.setLevel(log_level)

    logger_.info('Starting')

    # adds the NoUrlFilter to the logger
    logger_.addFilter(filter_fun())

    return logger_


def get_users(db_url):
    """
    Retrieves the User information from the Database.

    :param db_url: (str) URL of the Postgres database
    :returns: dictionary with the users.
    """
    up.uses_netloc.append("postgres")
    ele_url = up.urlparse(db_url)

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


def get_forecast(lat, long, app_id):
    """
    Hits the API and retrieves the 5-day forecast

    :param lat: (float) latitude of the location
    :param long: (float) longitude of the location
    :param app_id: (str) Application ID for OpenWeather
    :returns (dict) of the weather forecast
    """
    parameters = {
        "lat": lat,
        "lon": long,
        "exclude": "current,minutely,daily",
        "appid": app_id
    }

    # Base URL for OpenWeather API
    forecast_url = "https://api.openweathermap.org/data/2.5/forecast"

    response = requests.get(url=forecast_url, params=parameters)
    response.raise_for_status()

    data = response.json()

    logger.info(f'Got weather for {lat}, {long} => {response.status_code}')

    return data


# Send the message to an address
def send_message(subject, body_message, rec_email, sending_email):
    """
    preps and sends email message

    :param subject: (str) text for Subject line on email
    :param body_message: (str) text for the body of the email
    :param rec_email: (str) email address to send to
    :param sending_email: (dtr) email address sending from
    """

    body = f'''Content-type: text/html
    MIME-Version: 1.0
Subject:{subject}
{body_message}
    '''

    with smtplib.SMTP('smtp.gmail.com', 587) as connection:
        connection.starttls()
        connection.login(user=sending_email, password=os.environ.get("password"))
        connection.sendmail(
            from_addr=sending_email,
            to_addrs=rec_email,
            msg=body)
        connection.close()

    logger.info(f'Sent email to {rec_email}')


def wrap_forecast(data, tz):
    """
    Builds the email together.

    :param data: (dict) the weather forecast
    :param tz: (str) timezone of the user
    """
    bg_colors = {
        'Clear': 'lightblue',
        'Snow': 'white',
        'Clouds': 'lightgray',
        'Rain': 'darkgray'
    }

    # used to convert Kelvin to Celsius
    to_celsius = -273.15

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
        <div style=
        "width:300px;border:1px solid green;padding:3px;margin:3px;border-radius:5px;background-color:{bg_color};">
        <p style="text-align:center;">{do_date(item['dt'], tz)}</p>
        <div style="width:100%;text-align:center;padding:0;margin:0;">
        <img src="https://openweathermap.org/img/wn/{item['weather'][0]['icon']}.png"
            style="margin:0 auto;padding:0;"/>
        </div>
        <ul>
        <li>{item['weather'][0]['main']} - {item['weather'][0]['description']}</li>
        <li>Temperature {item['main']['temp'] + to_celsius:,.2f} C.</li>
        <li>Feels like {item['main']['feels_like'] + to_celsius:,.2f} C.</li>
        <li>Low {item['main']['temp_min'] + to_celsius:,.2f} C.</li>
        <li>High {item['main']['temp_max'] + to_celsius:,.2f}C.</li>
        <li>Wind Speed {item['wind']['speed']}</li>
        </ul>
        </div>
        """

    results += """</div>"""

    return results


# start logging
logger = logging_setup(NoUrlFilter)

# get user's information, place in a list of dictionary
users = [dict(user) for user in get_users(os.environ.get("database_url"))]

# loop over the users
for user in users:
    my_weather = get_forecast(user['latitude'], user['longitude'], os.environ.get("openweather_api_key"))
    message = wrap_forecast(my_weather, user['timezone'])
    send_message('Weather', message, user['email'], os.environ.get("email"))

logger.info('Finished')
