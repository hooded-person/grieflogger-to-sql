import time, sys, os, sqlite3, re, datetime, json
from dotenv import load_dotenv

# get the current working directory
current_working_directory = os.getcwd()
# get terminal size
width, height = os.get_terminal_size()

# settings
load_dotenv()
PATH_TO_ZIP_DIR = os.getenv('PATH_TO_ZIP_DIR')
DIRECTORY_TO_EXTRACT_TO = os.getenv('DIRECTORY_TO_EXTRACT_TO')
SQLITE3_DB_FILE = os.getenv('SQLITE3_DB_FILE')
try:
    SQLITE3_DB_TABLES = json.loads(os.getenv('SQLITE3_DB_TABLES'))
except json.JSONDecodeError as e:
    raise ValueError(f"Invalid JSON in SQLITE3_DB_TABLES environment variable. Error: {e}") from e
PROGRESS_LOG_FILE= os.getenv('PROGRESS_LOG_FILE')
try:
    BATCH_SIZE = int(os.getenv('BATCH_SIZE'))
except ValueError as e:
    raise ValueError(f"Invalid integer in BATCH_SIZE environment variable. Error: {e}") from e

LOG_NONE = os.getenv('LOG_NONE').lower() in ("true", "t", "1")
LOG_BATCH = os.getenv('LOG_BATCH').lower() in ("true", "t", "1") and not LOG_NONE
LOG_EVERY = os.getenv('LOG_EVERY').lower() in ("true", "t", "1") and not LOG_NONE
regexPattern = r'(-?\d+)#(-?\d+)#(-?\d+)#(\w)#([^#]*)#(\d{2}\/\d{2}\/\d{2}) (\d{2}:\d{2}:\d{2})#([^,\]]*)'

# assert valid environment variables
assert os.path.isdir(PATH_TO_ZIP_DIR), "PATH_TO_ZIP_DIR environment variable does not lead to a valid directory"
assert os.path.isdir(DIRECTORY_TO_EXTRACT_TO), "DIRECTORY_TO_EXTRACT_TO environment variable does not lead to a valid directory"
assert not os.path.isdir(SQLITE3_DB_FILE) and SQLITE3_DB_FILE[-3:] == ".db", "SQLITE3_DB_FILE environment variable is not a valid .db file"
assert isinstance(SQLITE3_DB_TABLES, list) and all(isinstance(item, str) for item in SQLITE3_DB_TABLES), "SQLITE3_DB_TABLES environment variable should be a list containing strings"
assert not os.path.isdir(PROGRESS_LOG_FILE) and PROGRESS_LOG_FILE[-5:] == ".json", "PROGRESS_LOG_FILE environment variable is not a valid .json file"
assert os.getenv('LOG_NONE').lower() in ("true", "t", "1") or os.getenv('LOG_NONE').lower() in ("false", "f", "0"), "LOG_NONE environment variable is not an accepted boolean ('true','t','1'/'false','f','0')"
assert os.getenv('LOG_BATCH').lower() in ("true", "t", "1") or os.getenv('LOG_BATCH').lower() in ("false", "f", "0"), "LOG_BATCH environment variable is not an accepted boolean ('true','t','1'/'false','f','0')"
assert os.getenv('LOG_EVERY').lower() in ("true", "t", "1") or os.getenv('LOG_EVERY').lower() in ("false", "f", "0"), "LOG_EVERY environment variable is not an accepted boolean ('true','t','1'/'false','f','0')"

# terminal colors
class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

# STARTING CODE
# create SQLite db connection
conn = sqlite3.connect(SQLITE3_DB_FILE)
cursor = conn.cursor()

# pregened sql queries
ALL_TABLES = "UNION ALL ".join(map(lambda table: f"SELECT * FROM {table} ", SQLITE3_DB_TABLES))

# exit best as posible
def exit_prgm():
    conn.close()
    print("Exiting...")
    sys.exit(0)

def draw_menu(options,name=None,offset=0):
    if name:
        print(name)
    for i, option in enumerate(options):
        print(f"{i+offset}. {option}")

def option_menu(options,name=None, offset=0):
    while True:
        draw_menu(options,name=name,offset=offset)
        choice = input()
        try:
            choice = int(choice)
        except:
            print(f"{bcolors.WARNING}Not a valid choice{bcolors.ENDC}")
            time.sleep(1)
            continue
        if choice - offset in range(len(options)):
            return (choice, options[choice-offset])
        print(f"{bcolors.WARNING}Not a valid choice{bcolors.ENDC}")
        time.sleep(1)

# param parser
identifiers = [
    "Action Identifier",
    "Object Identifier",
    "Source Name",
    "Time Duration",
]
paramsSettings = {
    "action":{
        "type":str,
        "value":"Action Identifier",
        "allowMultiple":True,
        "allowNegative":True,
    },"world":{
        "type":str,
        "value":SQLITE3_DB_TABLES,
        "allowMultiple":True,
        "allowNegative":True,
    },"object":{
        "type":str,
        "value":"Object Identifier",
        "allowMultiple":True,
        "allowNegative":True,
    },"range":{
        "type":int,
        "value":"int > 1",
        "allowMultiple":False,
        "allowNegative":False,
    },"source":{
        "type":str,
        "value":"Source Name",
        "allowMultiple":True,
        "allowNegative":True,
    },"before":{
        "type":str,
        "value":"Time Duration",
        "allowMultiple":False,
        "allowNegative":False,
    },"after":{
        "type":str,
        "value":"Time Duration",
        "allowMultiple":False,
        "allowNegative":False,
}}
def identifierParser(identifier, value):

    # if value[0] == "!" and identifier['allowNegative']:
    #     negative = True
    #     value = value[1:]
    # else:
    #     negative = False

    # if type(value) != identifier['type']:
    #     match identifier['type']:
    #         case "int":
    #             try: 
    #                 value = int(value)
    #             except ValueError:
    #                 # success, error_message
    #                 return False, "Could not convert to correct type"

    # if type(identifier['value']) == list:
    #     if value in identifier['value']:
    #         if negative:
    #             # success, [(target, value, value_prefix),...]
    #             return True, [("table", "*"),("table", value, "NOT")]

    match identifier:
        case "Action Identifier":
            valid = {
                "block-break":"b",
                "block-place":"p",
                "block-change":None,
                "item-insert":None,
                "item-remove":None,
                "entity-killed":None,
            }
            if value in valid:
                return not not valid[value], valid[value] or "This Action Identifier is not supported"
            else:
                return False, "Not a valid Action Identifier"
        case "Object Identifier":
            if ":" in value:
                value = value.split(":",maxsplit=1)[1]
            return True, value
        case "Source Name":
            return True, value
        case "Time Duration":
            (week, day, hour, minute, second) = re.search(r'(?:(\d+)w)?(?:(\d+)d)?(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?', value).groups()
            print(week, day, hour, minute, second)
            day = (day or 0) + 7*(week or 0); print(day)
            hour = (hour or 0) + 24*(day or 0); print(hour)
            minute = (minute or 0) + 60*(hour or 0); print(minute)
            second = (second or 0) + 60*(minute or 0); print(second)
            epoch = round(time.time())
            return True, str(epoch - second)



def process_params(paramsInput):
    paramsList = paramsInput.split(" ")
    parsedParams = {}
    for param in paramsList:
        key, value = param.split(":")
        identifierParser(key, value)



# Main menu options
# <search|inspect> <x> <y> <z> <params>
def query():
    queryInput = input("Enter query:\n")
    type, pos, paramsInput = re.search( r'(search|inspect)(?: (-?\d+)){3} (.*)',queryInput)
    params = process_params(paramsInput)

    print(pos)
    print(flags)
    baseQuery = f"SELECT * FROM {flags['dimension']} "
    posQuery = f" WHERE x={pos[0]} AND y={pos[1]} AND z={pos[2]} "
    flagQuery = handle_flags

def player():
    while True:
        username = input("Input username: ")
        cursor.execute(f"SELECT username FROM ({ALL_TABLES}) WHERE username='{username}'")
        if len(cursor.fetchall()) > 0:
            break
        print(f"{bcolors.WARNING}Username not found in database{bcolors.ENDC}")
    
    print("===[ PLAYER INFO ]===")
    cursor.execute(f"""SELECT username, COUNT(*) AS value_count 
                   FROM ({ALL_TABLES})
                   WHERE username="{username}"
                   GROUP BY username
                   ORDER BY value_count DESC
                   """) 
    print(f"total occurences: {cursor.fetchone()[1]}")
    cursor.execute(f"""SELECT username, COUNT(*) AS value_count 
                   FROM ({ALL_TABLES})
                   WHERE username=="{username}" AND interaction=="p"
                   GROUP BY username
                   ORDER BY value_count DESC
                   """) 
    print(f"blocks placed: {cursor.fetchone()[1]}")
    cursor.execute(f"""SELECT username, COUNT(*) AS value_count 
                   FROM ({ALL_TABLES})
                   WHERE username=="{username}" AND interaction=="b"
                   GROUP BY username
                   ORDER BY value_count DESC
                   """) 
    print(f"blocks broken: {cursor.fetchone()[1]}")
    cursor.execute(f"""SELECT username, COUNT(*) AS value_count 
                   FROM ({ALL_TABLES})
                   WHERE username=="{username}" AND interaction=="o"
                   GROUP BY username
                   ORDER BY value_count DESC
                   """) 
    print(f"containers opened: {cursor.fetchone()[1]}")
    
def overview():
    print("===[ DATABASE OVERVIEW ]===")

    # username info
    cursor.execute(f"""SELECT COUNT(DISTINCT x.username)
        FROM (SELECT username FROM ({ALL_TABLES})) x""")
    usernames = cursor.fetchone()[0]
    print(f"distinct usernames: {usernames}")

    cursor.execute(f"""SELECT username, COUNT(*) AS value_count 
                   FROM ({ALL_TABLES})
                   GROUP BY username 
                   ORDER BY value_count DESC
                   LIMIT 3
                   """)
    username_entries = cursor.fetchall()
    for i, table_entry in enumerate(username_entries):
        char = "└" if i+1 == len(username_entries) else "├"
        print(f"{char} {table_entry[0]}: {table_entry[1]}")
    
    # dimension/table info
    cursor.execute(f"SELECT COUNT(*) FROM ({ALL_TABLES})")
    totalEntries = cursor.fetchone()[0]
    print(f"total entries: {totalEntries}")

    sql_query = " UNION ALL ".join(
        [f"SELECT '{table}' AS dimension, COUNT(*) AS entries FROM {table}" for table in SQLITE3_DB_TABLES]
    )
    cursor.execute(f"""SELECT dimension, entries
        FROM (
            {sql_query}
        ) AS combined
        ORDER BY entries DESC
        LIMIT 4;
    """)
    table_entries = cursor.fetchall()
    for i, table_entry in enumerate(table_entries):
        char = "└" if i+1 == len(table_entries) else "├"
        print(f"{char} {table_entry[0]}: {table_entry[1]}")
    

def main():

    (choice, option) = option_menu(["query","player","overview","exit"],name="QUERY DATABASE",offset=1)
    
    match choice:
        case 1:
            query()
        case 2:
            player()
        case 3:
            overview()
        case 4:
            exit_prgm()

if __name__ == "__main__":
    (choice, option) = option_menu(["main","process_params","parse identifier"], offset=1)
    match choice:
        case 1:
            main()
        case 2:
            print(
                process_params(
                    input("params:\n")
            ))
            
        case 3:
            print(
                identifierParser(
                    input("identifier key:\n"),
                    input("identifier value:\n")
            ))