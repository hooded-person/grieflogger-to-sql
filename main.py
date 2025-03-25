import zipfile, gzip, os, sqlite3, re, datetime #, tqdm
# get the current working directory
current_working_directory = os.getcwd()
# get terminal size
width, height = os.get_terminal_size()

# settings
path_to_zip_dir = current_working_directory + "/logs/archives/"
directory_to_extract_to = current_working_directory + "/logs/files/"
regexPattern = r'(-?\d+)#(-?\d+)#(-?\d+)#(\w)#([^#]*)#(\d{2}\/\d{2}\/\d{2}) (\d{2}:\d{2}:\d{2})#([^,\]]*)'

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


files_in_dir = [f for f in os.listdir(path_to_zip_dir) if os.path.isfile(os.path.join(path_to_zip_dir, f))]
# setup progress for file unzippping
# file progress
# file log
# file_progress = tqdm.tqdm(
#     total=len(files_in_dir), 
#     desc="Files",
#     postition= 0
# )
# file_log = tqdm.tqdm(total=0, position=1, bar_format='{desc}')

print(f"""unzipping
{bcolors.BOLD}from: {bcolors.ENDC}"""+path_to_zip_dir+f"""
{bcolors.BOLD}to: {bcolors.ENDC}"""+directory_to_extract_to)
for filename in files_in_dir:
    fileStem, fileExtension = os.path.splitext(filename)
    
    # file_log.set_description_str(f'Current file: {filename}')
    
    path_to_zip_file = path_to_zip_dir + filename
    if fileExtension == ".zip":
        with zipfile.ZipFile(path_to_zip_file, 'r') as zip_ref:
            zip_ref.extractall(directory_to_extract_to)
        print("unzipped: " + filename)
    elif fileExtension == ".gz":
        with gzip.open(path_to_zip_file, 'rb') as fIn:
            fOut = open(directory_to_extract_to + fileStem + ".txt", "wb")
            fOut.write( fIn.read() )
            fOut.close()
        print("ungzipped: " + filename)
    # file_progress.update(1)

        
print(f"{bcolors.OKBLUE}unzipped all{bcolors.ENDC}")
# create SQLite db connection
conn = sqlite3.connect("logs.db")
cursor = conn.cursor()

# cursor.execute("""CREATE TABLE "overworld" (
#     `x` INTEGER NOT NULL, 
#     `y` INTEGER NOT NULL, 
#     `z` INTEGER NOT NULL, 
#     `interinteraction` TEXT NOT NULL, 
#     `username` TEXT NOT NULL,
#     `UUID` TEXT, 
#     "UNIX_time" INTEGER NOT NULL, 
#     `block` TEXT)""")

entriesAdded = 0
duplicatesSkipped = 0

logs_in_dir = [f for f in os.listdir(directory_to_extract_to) if os.path.isfile(os.path.join(directory_to_extract_to, f))]
# setup progress for file parsing
# file progress 
# file log
# log progress (total=len(matches))
# log info log (current match info)
# file_progress = tqdm.tqdm(
#     total=len(logs_in_dir), 
#     desc="Files",
#     postition= 0
# )
# file_log = tqdm.tqdm(total=0, position=1, bar_format='{desc}')
# log_info_log = tqdm.tqdm(total=0, position=3, bar_format='{desc}')
time_start = datetime.datetime.now()
file_progress = 0
file_total = len(logs_in_dir)


def log(msg):
    time_elapsed = datetime.datetime.now() - time_start
    time_guess = 0
    if file_progress > 0:
        time_guess = (time_elapsed / file_progress) * (file_total - file_progress)
    print(f"{str(time_guess)[:-7]}|{file_progress}/{file_total}-{round(file_progress/file_total*100)}%|{log_progress}/{log_total}-{round(log_progress/log_total*100)}%|{msg}")

for filename in logs_in_dir:
    # update log bar to new file
    # file_log.set_description_str(f'Current file: {filename}')
    filePath = directory_to_extract_to + filename
    with open(filePath, "r") as f:
        content = f.read()
        matches = re.findall(regexPattern, content)
        # log_progress = tqdm.tqdm(
        #     total=len(matches), 
        #     desc="Logs",
        #     postition= 2
        # )
        log_progress = 0
        log_total = len(matches)
        for match in re.finditer(regexPattern, content):
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
                
                infoStr = f"Added [{groups[5]} {groups[6]}] {groups[4]} '{groups[3]}' {groups[7]} at {groups[0]} {groups[1]} {groups[2]}"
                # log_info_log.set_description_str(infoStr)
                log(infoStr)
            else:
                duplicatesSkipped += 1
                infoStr = f"{bcolors.BOLD + bcolors.WARNING}Skipped duplicate [{groups[5]} {groups[6]}] {groups[4]} '{groups[3]}' {groups[7]} at {groups[0]} {groups[1]} {groups[2]}{bcolors.ENDC}"
                # log_info_log.set_description_str(infoStr)
                log(infoStr)
            log_progress += 1
    # increment progress bar
    # file_progress.update(1)
    file_progress += 1
    

print(f"Successfully added {entriesAdded} entries")
skippedColor = duplicatesSkipped != 0 and bcolors.WARNING or ""
print(f"{skippedColor}Skipped {duplicatesSkipped} duplicate entries")
# tying up loose ends
conn.close()