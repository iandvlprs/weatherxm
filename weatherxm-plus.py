#!/usr/bin/python3
## Imports we need to function properly
# On linux, add the relevant libraries using your package manager.
# On Windows (spit), I've got no idea how you add the relevant dependancies. Google may help.
import csv
import json
import mysql.connector
import requests
import sqlite3
from optparse import OptionParser
from optparse import OptionGroup
from dateutil import parser

## Variable Section
# I intend running from cron - meaning output is rather pointless.
# If you want output printed to STDOUT, use the "-v" flag from the command line
prn_verb = False
# API Endpoints
api_aurl = 'https://api.weatherxm.com/api/v1/auth/login'
api_durl = 'https://api.weatherxm.com/api/v1/me/devices'
api_hurl = 'https://api.weatherxm.com/api/v1/me/devices/$$$/history'

## Function to print pretty banner
def PrintHeader():
    debug_print("weathermx-plus.py")
    debug_print("Ian de Villiers")
    debug_print("---------------------------------------------")

## Function to print to STDOUT if debug flag is set.
# Arguments:
#    s = String to print if debug flag is set.
def debug_print(s):
    if prn_verb:
        print(s)

## Function to write data to CSV file
# Arguments:
#    f - Filename to write to
#    a - Array of history data dict
def writecsvrecords(f, a):
    try:
        with open(f, 'w', newline='') as csvfile:
            # These will be the titles.
            fieldnames = ["timestamp", "temperature", "feels_like", "dew_point", "precipitation_accumulated", "precipitation", "wind_speed", "wind_gust", "wind_direction", "humidity", "pressure", "uv_index", "solar_irradiance", "illuminance", "icon"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(a)
    except:
        print(" - Error writing CSV file")

## Function to write data to sqlite
# Arguments:
#    f - Filename of sqlite database
#    a - Array of history data dict
def writesqliterecords(f, a):
    try:
        con = sqlite3.connect(f)
        cur = con.cursor()
        # We first want to see whether this is a new database...
        res = cur.execute("SELECT name FROM sqlite_master WHERE name = 'station'")
        if res.fetchone() is None:
            debug_print(" + New sqlite database. Creating tables")
            cur.execute("CREATE TABLE station (stdtt TEXT PRIMARY KEY, sttmp REAL, sthum REAL, stwsn REAL, stwsg REAL, stwdr REAL, stsol REAL, stuvi REAL, strmn REAL, strma REAL, stprs REAL, stdew REAL, strlf REAL, still REAL, stico TEXT);")
        # Since the times are excluded from the FROM / TO date in the API, if we're running this regularly (like on the hour), we will get duplicates.
        # So we replace into to prevent key violations.
        cur.executemany("REPLACE INTO station VALUES(:timestamp, :temperature, :humidity, :wind_speed, :wind_gust, :wind_direction, :solar_irradiance, :uv_index, :precipitation, :precipitation_accumulated, :pressure, :dew_point, :feels_like, :illuminance, :icon)", a)
        con.commit()
        con.close()
    except:
        print(" - Error writing to sqlite database")

## Function to write data to mysql database
# Arguments:
#    h - Database host
#    u - Database user
#    p - Database password
#    d - Database name
#    a - Array of history data dict
def writemysqlrecords(h, u, p, d, a):
    try:
        mydb = mysql.connector.connect(host=h, user=u, password=p, database=d)
        cursor = mydb.cursor(prepared=True,)
        # Since the times are excluded from the FROM / TO date in the API, if we're running this regularly (like on the hour), we will get duplicates.
        # So we replace into to prevent key violations.
        sql = """
        REPLACE INTO
        station (stdtt, sttmp, sthum, stwsn, stwsg, stwdr, stsol, stuvi, strmn, strma, stprs, stdew, strlf, still, stico)
        VALUES  (%s   , %s   , %s   , %s   , %s   , %s   , %s   , %s   , %s   , %s   , %s   , %s   , %s   , %s    , %s);
        """
        for i in a:
            insertdata = (str(i["timestamp"]), i["temperature"], i["humidity"], i["wind_speed"], i["wind_gust"], i["wind_direction"], i["solar_irradiance"], i["uv_index"], i["precipitation"], i["precipitation_accumulated"], i["pressure"], i["dew_point"], i["feels_like"], i["illuminance"], i["icon"])
            cursor.execute(sql, insertdata)
        mydb.commit()
        cursor.close()
        mydb.close()
    except:
        debug_print("   - Error writing to database")

# Function to authenticate
# Arguments:
#    u - username
#    p - password
# Returns:
#    Bearer Token if successful
#    String "Error" if unsuccessful
def auth(u, p):
    try:
        data = { "username": u, "password": p}
        s = requests.post(api_aurl, json=data)
        s.raise_for_status()
        return s.json()['token']
    except:
        return "Error"

# Function to list devices of associated account
# Arguments:
#    t - Bearer Token
# Returns
#    List of devices owned or followed by the account if successful
#    String "Error" if unsuccessful
def devices(t):
    try:
        headers = {'Authorization': 'Bearer ' + t}
        s = requests.get(api_durl, headers=headers)
        s.raise_for_status()
        return s.json()
    except:
        return "Error"

# Function to return history for a specific device
# Arguments:
#    t - Bearer Token
#    d - Device ID
#    s - Start Date
#    e - End Date
def history(t, d, s, e):
    # We replace the place holder of the history URL with our device ID.
    url = str(api_hurl).replace("$$$", d)
    # We add an arbitrary time on to the from and to dates. This is because the API uses the dates only and ignores the time.
    # But... the call will fail if a time is not specified.
    querystring = {'fromDate': str(s) + ' 01:00:00', 'toDate': str(e) + ' 01:00:00'}
    try:
        headers = {'Authorization': 'Bearer ' + t}
        s = requests.get(url, querystring, headers=headers)
        return s.json()
    except:
        return "Error"


## Main function
def main():
    opusg = "  %prog [options]"
    parse = OptionParser(usage=opusg)
    # Main app options...
    syst_group = OptionGroup(parse, "Main Script Settings")
    syst_group.add_option("-v", "--verbose",   dest="verbose",    help="Verbose Mode", action="store_true", default=False)
    syst_group.add_option("-u", "--username",  dest="username",   help="API User")
    syst_group.add_option("-p", "--password",  dest="password",   help="API Pass")
    syst_group.add_option("-m", "--mode",      dest="mode",       help="Get DeviceID (id) | Get History (history)")
    parse.add_option_group(syst_group)
    # Output options
    outp_group = OptionGroup(parse, "Output Settings")
    outp_group.add_option("-o", "--output",    dest="output",     help="CSV (csv) | MySQL (mysql) | SQLite (sqlite)")
    outp_group.add_option("-f", "--file",      dest="filename",   help="Output Filename (for CSV or SQLite")
    parse.add_option_group(outp_group)
    # Database Options
    data_group = OptionGroup(parse, "Database Settings")
    data_group.add_option("-d", "--datahost",  dest="datahost",   help="Database Host")
    data_group.add_option("-b", "--database",  dest="database",   help="Database Name")
    data_group.add_option("-n", "--datauser",  dest="datauser",   help="Database User")
    data_group.add_option("-w", "--datapass",  dest="datapass",   help="Database Pass")
    parse.add_option_group(data_group)
    # History Options
    hist_group = OptionGroup(parse, "History Settings")
    hist_group.add_option("-i", "--id",         dest="device",     help="Device GUID")
    hist_group.add_option("-s", "--start",      dest="strdate",    help="Start Date (YYYY-MM-DD)")
    hist_group.add_option("-e", "--end",        dest="enddate",    help="End Date (YYYY-MM-DD)")
    parse.add_option_group(hist_group)
    # Just my morbid sense of humour
    evil_group = OptionGroup(parse, "Other Arb Settings")
    evil_group.add_option("-x", "--ss",         dest="super",      help="Super Select", action="store_true", default=False)
    # Parse command line arguments
    (opt, arg) = parse.parse_args()
    # We need to reference the global debug variable in order to set it.
    # Does not do much - just sets the flag on whether we are going to print to STDOUT or not...
    global prn_verb
    prn_verb = opt.verbose
    # I needed to include a reference to SS here. So I over-ride a few things firs.
    if opt.super:
        prn_verb = True


    # Print a pretty header if debug is set.
    PrintHeader()
    if opt.super:
        debug_print("A question for Oom Gerhard Fourie:")
        debug_print("What makes Super Select[nn] so special anyways ? :)")
        return
    # We need a username, password and mode at the least. If they are not there, we fail things...
    if not opt.username:
        parse.error("No API user specified. Check settings with -h")
        return
    if not opt.password:
        parse.error("No API pass specified. Check settings with -h")
        return
    if not opt.mode or (opt.mode != "id" and opt.mode != "history"):
        parse.error("Mode not specified or wrong mode. Should be id or history. Check settings with -h")
        return
    # Here we get the bearer token using the username and password
    debug_print(" + Getting authorisation token.")
    token = auth(opt.username, opt.password)
    if token == "Error":
        # This is an error - so I make a direct call to print as I want it displayed whether debug is set or not.
        print(" - Error authenticating.")
        return
    debug_print(" + Authorisation token obtained.")
    # We are not sure what our station ID is, so we dump a list of all station names and id's associated with our account. This includes ones we have "favourited".
    if opt.mode == "id":
        # Querying for the station ID is informational only. So I want to print to STDOUT.
        # So I set debug to true.
        prn_verb = True
        debug_print(" + Querying linked devices.")
        s = devices(token)
        if s == "Error":
            debug_print(" - Error getting devices.")
            return
        for i in s:
            debug_print(" + Station Name (ID) : " + str(i['name']) + " (GUID: " + str(i['id']) + ")")
        return
    # We want the station history data
    if opt.mode == "history":
        # We first need to sanity check some stuff. ie: We will need device ID, start date and end date
        if not opt.device or not opt.strdate or not opt.enddate:
            parse.error("No device id, start date or end date specified. Check settings with -h")
            return
        # We also need to check the output parameters
        if opt.output == "csv" or opt.output == "sqlite":
            if not opt.filename:
                parse.error("Output CSV filename or SQLIte database not specified. Check settings with -h")
                return
        if opt.output == "mysql":
            if not opt.datahost or not opt.database or not opt.datauser or not opt.datapass:
                parse.error("Database server settings not specified. Check settings with -h")
                return
        debug_print(" + Getting station history for station")
        s = history(token, opt.device, opt.strdate, opt.enddate)
        if s == "Error":
            prn_verb = True
            debug_print(" - Error getting station history.")
            return
        arraydata = []
        # Iterate through the days.
        for i in s:
            debug_print(" + Station data for " + str(i['date']))
            # Iterate through the hours.
            for j in i['hourly']:
                # The timestamp returned by the API is incompatible with some RDBMS. So I do some horrible haxory to fix it.
                # Probably better ways to do it, but care not do I...
                timestamp = str(parser.parse(j["timestamp"])).split("+")[0]
                timestamp = str(timestamp).replace("T", " ").replace("+", " ")
                j["timestamp"] = timestamp
                debug_print("    Timestamp                 : " + str(j["timestamp"]))
                debug_print("    Temperature               : " + str(j["temperature"]))
                debug_print("    Feels like                : " + str(j["feels_like"]))
                debug_print("    Dew point                 : " + str(j["dew_point"]))
                debug_print("    Accumulated precipitation : " + str(j["precipitation_accumulated"]))
                debug_print("    Current precipitation     : " + str(j["precipitation"]))
                debug_print("    Wind speed                : " + str(j["wind_speed"]))
                debug_print("    Gust speed                : " + str(j["wind_gust"]))
                debug_print("    Wind direction            : " + str(j["wind_direction"]))
                debug_print("    Humidity                  : " + str(j["humidity"]))
                debug_print("    Pressure                  : " + str(j["pressure"]))
                debug_print("    UV Index                  : " + str(j["uv_index"]))
                debug_print("    Solar Irradiance          : " + str(j["solar_irradiance"]))
                debug_print("    Illuminance               : " + str(j["illuminance"]))
                debug_print("    Icon                      : " + str(j["icon"]))
                # Not the most streamlined code as I will be iterating through data here and again when I write it.
                # But I want an array of the history data dict when I write data.
                arraydata.append(j)
        # Output is to csv.
        if opt.output == "csv":
            writecsvrecords(opt.filename, arraydata)
        # Output is to sqlite.
        if opt.output == "sqlite":
            writesqliterecords(opt.filename, arraydata)
        # Output is to mysql.
        if opt.output == "mysql":
            writemysqlrecords(opt.datahost, opt.datauser, opt.datapass, opt.database, arraydata)

# Main script entry point
if __name__ == "__main__":
    main()
