import tarfile, os, sqlite3, re, datetime, json
from dotenv import load_dotenv
from tqdm import tqdm

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
LOG_EVERY = os.getenv('LOG_EVERY').lower() in ("true", "t", "1") and not LOG_NONE
LOG_BATCH = os.getenv('LOG_BATCH').lower() in ("true", "t", "1") and not LOG_NONE
LOG_FILE = os.getenv('LOG_FILE').lower() in ("true", "t", "1") and not LOG_NONE

SHOW_MATCH_BAR = os.getenv('SHOW_MATCH_BAR').lower() in ("true", "t", "1")
SHOW_FILE_FOLDER_BAR = os.getenv('SHOW_FILE_FOLDER_BAR').lower() in ("true", "t", "1")


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
def viewColors():
    print(f"{bcolors.HEADER}HEADER{bcolors.ENDC}")
    print(f"{bcolors.OKBLUE}OKBLUE{bcolors.ENDC}")
    print(f"{bcolors.OKCYAN}OKCYAN{bcolors.ENDC}")
    print(f"{bcolors.OKGREEN}OKGREEN{bcolors.ENDC}")
    print(f"{bcolors.WARNING}WARNING{bcolors.ENDC}")
    print(f"{bcolors.FAIL}FAIL{bcolors.ENDC}")
    print(f"{bcolors.BOLD}BOLD{bcolors.ENDC}")
    print(f"{bcolors.UNDERLINE}UNDERLINE{bcolors.ENDC}")
# viewColors()

files_in_dir = [f for f in os.listdir(PATH_TO_ZIP_DIR) if os.path.isfile(os.path.join(PATH_TO_ZIP_DIR, f))]

print(f"""unzipping
{bcolors.BOLD}from: {bcolors.ENDC}"""+PATH_TO_ZIP_DIR+f"""
{bcolors.BOLD}to: {bcolors.ENDC}"""+DIRECTORY_TO_EXTRACT_TO)
for fileName in (pBarFile := tqdm(files_in_dir)):
    fileStem, fileExtension = os.path.splitext(fileName)
    path_to_zip_file = PATH_TO_ZIP_DIR + fileName
    pBarFile.write(f"extracting {fileName}")

    with tarfile.open(path_to_zip_file, 'r') as tar:
        tar.extractall(DIRECTORY_TO_EXTRACT_TO)
    pBarFile.write("unzipped: " + fileName)

       
print(f"{bcolors.OKBLUE}unzipped all{bcolors.ENDC}")

# create SQLite db connection
conn = sqlite3.connect(SQLITE3_DB_FILE)
cursor = conn.cursor()

for table in SQLITE3_DB_TABLES:
    cursor.execute(f"""CREATE TABLE if not exists "{table}" (
        `x` INTEGER NOT NULL, 
        `y` INTEGER NOT NULL, 
        `z` INTEGER NOT NULL, 
        `interaction` TEXT NOT NULL, 
        `username` TEXT NOT NULL,
        `lower_username` TEXT NOT NULL,
        `UUID` TEXT, 
        "UNIX_time" INTEGER NOT NULL, 
        `block` TEXT,
        UNIQUE(x, y, z, interaction, username, UNIX_time, block))
        """)

entriesAdded = 0
batchCount = 0

dimensionData = {
    "dim":{},
    "total_files":0,
}
for dimension in SQLITE3_DB_TABLES: # calculate total number of files in all dimensions
    dimensionDir = os.path.join(DIRECTORY_TO_EXTRACT_TO, dimension + "/")
    try:
        with open(PROGRESS_LOG_FILE, "r") as file:
            progress=json.load(file)
    except:
        progress = json.loads(os.getenv("EMPTY_PROGRESS_LOG"))
        with open(PROGRESS_LOG_FILE, "w") as file:
            file.write(os.getenv("EMPTY_PROGRESS_LOG"))
    try:
        logs_in_dir = [f for f in os.listdir(dimensionDir) if os.path.isfile(os.path.join(dimensionDir, f))] 
        todo_logs_in_dir = [file for file in logs_in_dir if dimensionDir+file not in progress["files"]]
    except:
        logs_in_dir = []
        todo_logs_in_dir = []
    dimensionData["dim"][dimension] = {}
    dimensionData["dim"][dimension]["logs_in_dir"] = logs_in_dir
    dimensionData["dim"][dimension]["todo_logs_in_dir"] = todo_logs_in_dir
    dimensionData["total_files"] += len(todo_logs_in_dir)


pBarMain = tqdm(total=dimensionData["total_files"])
for i, dimension in enumerate(SQLITE3_DB_TABLES): # actually parse the files
    dimsDone = str(i+1)
    dimsTotal = str(len(SQLITE3_DB_TABLES))
    padZero = len(dimsTotal) - len(dimsDone)
    dimsDone = padZero*"0" + dimsDone
    pBarMain.set_description_str(f"{dimsDone}/{dimsTotal} Dimensions")
    dimensionDir = os.path.join(DIRECTORY_TO_EXTRACT_TO, dimension + "/")
    
    logs_in_dir = dimensionData["dim"][dimension]["logs_in_dir"]
    todo_logs_in_dir = dimensionData["dim"][dimension]["todo_logs_in_dir"]
    skippedFiles = len(logs_in_dir) - len(todo_logs_in_dir)
    if skippedFiles > 0:
        print(f"{bcolors.BOLD + bcolors.WARNING}Skipped {skippedFiles} files, already parsed{bcolors.ENDC}")

    batch = []

    loopIter = tqdm(todo_logs_in_dir) if SHOW_FILE_FOLDER_BAR  else todo_logs_in_dir
    for fileName in loopIter:
        filePath = dimensionDir + fileName
        if LOG_FILE:
            pBarMain.write(f"Parsing {filePath}")
        with open(filePath, "r") as f:
            content = f.read()
            totalMatches = len(re.findall(regexPattern, content))
            matches = re.finditer(regexPattern, content)
            if SHOW_MATCH_BAR:
                pBarMatch = tqdm(matches, total=totalMatches, leave=False)
            for match in (not SHOW_MATCH_BAR and matches or pBarMatch):
                groups = match.groups() # x:0 y:1 z:2 interaction:3 username:4 date:5 time:6 block:7\n
                logData = {
                    'x':        groups[0],
                    'y':        groups[1],
                    'z':        groups[2],
                    'interaction':   groups[3],
                    'username': groups[4],
                    'lower_username': groups[4].lower(),
                    'UNIX':     datetime.datetime.strptime(f"{groups[5]} {groups[6]}","%m/%d/%y %H:%M:%S").timestamp(),
                    'block':    groups[7],
                }
                
                batch.append(logData)
                if LOG_EVERY:
                    pBarMain.write(f"Added [{groups[5]} {groups[6]}] {groups[4]} '{groups[3]}' {groups[7]} at {groups[0]} {groups[1]} {groups[2]} to batch")

                if len(batch) > BATCH_SIZE:
                    cursor.executemany(f"INSERT OR IGNORE INTO {dimension} VALUES (:x, :y, :z, :interaction, :username, :lower_username, NULL, :UNIX, :block)", batch)
                    conn.commit()
                    entriesAdded += len(batch)
                    if LOG_BATCH:
                        pBarMain.write(f"Executed batch {batchCount} with {len(batch)} queries")
                    batchCount+=1
                    batch = []
                if SHOW_MATCH_BAR:
                    pBarMatch.close()
        
        if batch:
            cursor.executemany(f"INSERT OR IGNORE INTO {dimension} VALUES (:x, :y, :z, :interaction, :username, :lower_username, NULL, :UNIX, :block)", batch)
            conn.commit()
            entriesAdded += len(batch)
            if LOG_BATCH:
                pBarMain.write(f"Executed batch {batchCount} with {len(batch)} queries")
            batchCount+=1
            batch = []

        with open(PROGRESS_LOG_FILE, "r") as file:
            progress=json.load(file)
        progress["files"].append(filePath)
        with open(PROGRESS_LOG_FILE, "w") as file:
            json.dump(progress, file)
        pBarMain.update(1)
pBarMain.close()
        

print(f"Successfully added {entriesAdded} entries to {len(SQLITE3_DB_TABLES)} tables")
# print(f"{duplicatesSkipped != 0 and bcolors.WARNING or ""}Skipped {duplicatesSkipped} duplicate entries")
# tying up loose ends
conn.close()