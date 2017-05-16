import configparser
import datetime
import sys
import time
import csv
import ast

import logging

import mysql.connector
from mysql.connector import errorcode

from twilio.rest import Client
from twilio.rest import TwilioException

'''
Author: BECurrie @https://github.com/becurrie

program connects to rocket map mysql db and retrieves any rare pokemon specified by the user and sends the pokemon
through sms to the specified numbers.
'''

Config = configparser.ConfigParser()
# attempt to parse config.ini file
try:
    Config.read('config/config.ini')
except configparser.Error as err:
    sys.stderr.write(str(err))

# init database auth args
database_auth = {
    'user': Config.get('Database', 'user'),
    'password': Config.get('Database', 'password'),
    'host': Config.get('Database', 'host'),
    'database': Config.get('Database', 'database'),
    'raise_on_warnings': Config.getboolean('Database', 'raise_on_warnings')}

# init twilio api auth args
account_sid = Config.get('Twilio', 'account_sid')
auth_token = Config.get('Twilio', 'auth_token')
num_from = Config.get('Twilio', 'from')


# set up logging
timestamp = (datetime.datetime.now().strftime("%a-%m-%d_%H-%M-%S"))
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s --- %(levelname)s --- %(message)s')

fh = logging.FileHandler("log/" + str(timestamp) + ".log")
fh.setLevel(logging.INFO)
fh.setFormatter(formatter)
logger.addHandler(fh)

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)
logger.addHandler(ch)


def auth_twilio():
    try:
        logger.debug("Authenticating Twilio API...")
        client = Client(account_sid, auth_token)
        return client
    except TwilioException as err:
        logger.critical(str(err))
        logger.critical("Failed Twilio Authentication...")
        quit()


def send_messages(uniq_id, poke_id, poke_name, url, time_remaining):
    for phone_number, number in Config.items('Phone_Numbers'):
        try:
            client = Client(account_sid, auth_token)
            client.messages.create(
                to=number,
                from_=num_from,
                body="Rare Pokemon Found:\n" + poke_name + " [" + str(poke_id) + "]\n" + "Expires In: " +
                     str(time_remaining) + "\n" + "Directions To Pokemon: " + url + " \n\nUnique Identifier:\n" + str(uniq_id)
            )
            logger.debug("Message STATUS: SUCCESS | TO: " + str(number) + " | FROM: " + str(num_from))
            time.sleep(1)
        except TwilioException as err:
            logger.critical("Message STATUS: FAILURE | TO: " + str(number) + " | FROM: " + str(num_from) + " ERROR: " + str(err))


def sql_connect():
    # attempt to connect to mysql database
    try:
        sql_cnx = mysql.connector.connect(**database_auth)
    except mysql.connector.Error as con_err:
        if con_err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            logger.critical("bad username/password")
        elif con_err.errno == errorcode.ER_BAD_DB_ERROR:
            logger.critical("database " + "'" + database_auth['database'] + "'" + " does not exist")
        else:
            logger.critical(str(con_err))

    # return cnx object
    return sql_cnx


def print_pokemon(uniq_id, poke_id, name, lat, lng, disap_time, sent):
    logger.debug("-------------------------------")
    logger.info("Unique Encounter ID: " + uniq_id)
    logger.info("Latitude: " + str(lat))
    logger.info("Longitude: " + str(lng))
    logger.info("Time Disappears (UTC): " + str(disap_time))
    logger.info("Pokemon ID: " + str(poke_id))
    logger.info("Pokemon Name: " + name)
    logger.info("Is Sent: " + str(sent))
    logger.debug("-------------------------------")
def print_pokemon_lite(uniq_id, poke_id, name, lat, lng, disap_time, sent):
    logger.info("Pokemon ID: " + str(poke_id) + " ][ " + "Pokemon Name: " + name + " ][ " + "Latitude: " + str(lat) +
                " ][ " + "Longitude: " + str(lng) + " ][ " + "Time Disappears (UTC): " + str(disap_time) + " ][ "
                + "Pokemon ID: " + str(poke_id) + " ][ " + "Unique Encounter ID: " + str(uniq_id) + " ][ "
                + "Is Sent: " + str(sent) + " ]")



def print_rare_pokemon(uniq_id, poke_id, name, lat, lng, disap_time, sent, url, time):
    logger.debug("-------------------------------")
    logger.info("Unique Encounter ID: " + uniq_id)
    logger.info("Latitude: " + str(lat))
    logger.info("Longitude: " + str(lng))
    logger.info("Directions: " + url)
    logger.info("Time Disappears (UTC): " + str(disap_time))
    logger.info("Time Remaining: " + str(time))
    logger.info("Pokemon ID: " + str(poke_id))
    logger.info("Pokemon Name: " + name)
    logger.info("Is Sent: " + str(sent))
    logger.debug("-------------------------------")
def print_rare_pokemon_lite(uniq_id, poke_id, name, lat, lng, disap_time, sent, url, time):
    logger.info("Pokemon ID: " + str(poke_id) + " | " + "Pokemon Name: " + str(name) + " | " + "Longitude: " + str(lng)
                + " | " + "Directions: " + url + " | " + "Directions: " + url + " | " + "Time Remaining: " +
                str(disap_time) + " | " + "Time Remaining: " + str(time) + " | " + "Unique Encounter ID: " +
                str(uniq_id) + " | " + name + "Is Sent: " + str(sent))


def calc_time_remaining(expires):
    timestamp = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    current_time = datetime.datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
    expire_time = expires
    return expire_time - current_time

# SQL queries
get_all_pokemon = "SELECT encounter_id, pokemon_id, pokemon_name, latitude, longitude, disappear_time, sent FROM pokemon"
get_rare_pokemon_from_id = "SELECT encounter_id, pokemon_id, pokemon_name, latitude, longitude, disappear_time, sent FROM pokemon WHERE pokemon_id ="
delete_expired_pokemon = "DELETE FROM pokemon WHERE disappear_time < UTC_TIMESTAMP OR sent = 1"
set_pokemon_sent = "UPDATE pokemon SET sent = 1 WHERE encounter_id = "


def main():
    cnx = sql_connect()
    cursor = cnx.cursor(buffered=True)

    # delete all pokemon that have expired from db or where sent = True
    cursor.execute(delete_expired_pokemon)
    cnx.commit()

    # run query to grab all pokemon from db and store in var 'row'
    cursor.execute(get_all_pokemon)
    row = cursor.fetchall()

    # change names of all pokemon not already named
    for (encounter_id, pokemon_id, pokemon_name, latitude, longitude, disappear_time, sent) in row:
        with open('pokemon.csv', 'r') as file:
            reader = csv.reader(file, delimiter=',')
            file.readline()
            for row in reader:
                if pokemon_id == int(row[0]):
                    cursor.execute("UPDATE pokemon SET pokemon_name = " + "'" + row[1] + "'" + " WHERE pokemon_id = " + "'" + row[0] + "'"
                                   + " AND pokemon_name is Null")
                    cnx.commit()

    # print all pokemon to console
    amount = 0
    cursor.execute(get_all_pokemon)
    row = cursor.fetchall()

    for (encounter_id, pokemon_id, pokemon_name, latitude, longitude, disappear_time, sent) in row:
        amount += 1
        print_pokemon_lite(encounter_id, pokemon_id, pokemon_name, latitude, longitude, disappear_time, sent)

    logger.debug("Pokemon in Database: " + str(amount))
    logger.debug("Printing Rare Pokemon Found...")

    rare_id_list = ast.literal_eval(Config.get("Filter", "rare_ids"))

    for rare in rare_id_list:
        cursor.execute(get_rare_pokemon_from_id + "'" + str(rare) + "'")
        row = cursor.fetchall()
        for(encounter_id, pokemon_id, pokemon_name, latitude, longitude, disappear_time, sent) in row:
            url = "https://www.google.com/maps/dir/Current+Location/" + str(latitude) + "," + str(longitude)
            time_remaining = calc_time_remaining(disappear_time)

            cursor.execute(set_pokemon_sent + "'" + encounter_id + "'")
            cnx.commit()
            print_rare_pokemon_lite(encounter_id, pokemon_id, pokemon_name, latitude, longitude, disappear_time, 1, url, time_remaining)
            logger.debug("Attempting To Send SMS Alerts...")

            auth_twilio()
            send_messages(encounter_id, pokemon_id, pokemon_name, url, time_remaining)

    cnx.close()
    cursor.close()

    logger.debug("")
    logger.debug("")

    logger.debug("RESUMING IN 10 SECONDS...")
    time.sleep(10)

    logger.debug("")
    logger.debug("")

    main()


if __name__ == '__main__':
    main()

