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
SQLITE3_DB_TABLES = json.loads(os.getenv('SQLITE3_DB_TABLES'))
PROGRESS_LOG= os.getenv('PROGRESS_LOG')
regexPattern = r'(-?\d+)#(-?\d+)#(-?\d+)#(\w)#([^#]*)#(\d{2}\/\d{2}\/\d{2}) (\d{2}:\d{2}:\d{2})#([^,\]]*)'

# assert valid .env
assert os.path.isdir(PATH_TO_ZIP_DIR), "PATH_TO_ZIP_DIR in .env file does not lead to a valid directory"
assert os.path.isdir(DIRECTORY_TO_EXTRACT_TO), "DIRECTORY_TO_EXTRACT_TO in .env file does not lead to a valid directory"

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
        `UUID` TEXT, 
        "UNIX_time" INTEGER NOT NULL, 
        `block` TEXT)""")

entriesAdded = 0
duplicatesSkipped = 0

for dimension in SQLITE3_DB_TABLES:
    dimensionDir = os.path.join(DIRECTORY_TO_EXTRACT_TO, dimension + "/")
    logs_in_dir = [f for f in os.listdir(dimensionDir) if os.path.isfile(os.path.join(dimensionDir, f))]
    for fileName in (pBarFile := tqdm(logs_in_dir)):
        filePath = dimensionDir + fileName
        with open(PROGRESS_LOG, "r") as file:
            progress=json.load(file)
        if filePath in progress:
            pBarFile.write(f"{bcolors.BOLD + bcolors.WARNING}Skipped {filePath}, already parsed{bcolors.ENDC}")
            continue

        pBarFile.write(f"Parsing {filePath}")
        with open(filePath, "r") as f:
            content = f.read()
            totalMatches = len(re.findall(regexPattern, content))
            for match in (pbarMatch := tqdm(re.finditer(regexPattern, content), total=totalMatches)):
                groups = match.groups() # x:0 y:1 z:2 interaction:3 username:4 date:5 time:6 block:7\n
                logData = {
                    'x':        groups[0],
                    'y':        groups[1],
                    'z':        groups[2],
                    'interaction':   groups[3],
                    'username': groups[4],
                    'UNIX':     datetime.datetime.strptime(f"{groups[5]} {groups[6]}","%m/%d/%y %H:%M:%S").timestamp(),
                    'block':    groups[7],
                }
                cursor.execute("SELECT * FROM overworld WHERE x=:x AND y=:y AND z=:z AND interaction=:interaction AND username=:username AND UNIX_time=:UNIX AND block=:block", logData)
                duplicates = cursor.fetchall()
                if len(duplicates) == 0:
                    cursor.execute("INSERT INTO overworld VALUES (:x, :y, :z, :interaction, :username, NULL, :UNIX, :block)", logData)
                    conn.commit()
                    entriesAdded += 1
                    pbarMatch.write(f"Added [{groups[5]} {groups[6]}] {groups[4]} '{groups[3]}' {groups[7]} at {groups[0]} {groups[1]} {groups[2]}")
                else:
                    duplicatesSkipped += 1
                    pbarMatch.write(f"{bcolors.BOLD + bcolors.WARNING}Skipped duplicate [{groups[5]} {groups[6]}] {groups[4]} '{groups[3]}' {groups[7]} at {groups[0]} {groups[1]} {groups[2]}{bcolors.ENDC}")
        with open(PROGRESS_LOG, "r") as file:
            progress=json.load(file)
        progress["files"].append(filePath)
        with open(PROGRESS_LOG, "w") as file:
            json.dump(progress, file)

print(f"Successfully added {entriesAdded} entries to {len(SQLITE3_DB_TABLES)} tables")
print(f"{duplicatesSkipped != 0 and bcolors.WARNING or ""}Skipped {duplicatesSkipped} duplicate entries")
# tying up loose ends
conn.close()