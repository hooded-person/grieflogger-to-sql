import zipfile, gzip, os, sqlite3, re, datetime
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

print(f"""unzipping
{bcolors.BOLD}from: {bcolors.ENDC}"""+path_to_zip_dir+f"""
{bcolors.BOLD}to: {bcolors.ENDC}"""+directory_to_extract_to)
for fileName in files_in_dir:
    fileStem, fileExtension = os.path.splitext(fileName)
    print("extracting type " + fileExtension)
    path_to_zip_file = path_to_zip_dir + fileName
    if fileExtension == ".zip":
        with zipfile.ZipFile(path_to_zip_file, 'r') as zip_ref:
            zip_ref.extractall(directory_to_extract_to)
        print("unzipped: " + fileName)
    elif fileExtension == ".gz":
        with gzip.open(path_to_zip_file, 'rb') as fIn:
            fOut = open(directory_to_extract_to + fileStem + ".xml", "wb")
            fOut.write( fIn.read() )
            fOut.close()
        print("ungzipped: " + fileName)

        
print(f"{bcolors.OKBLUE}unzipped all{bcolors.ENDC}")
# create SQLite db connection
conn = sqlite3.connect("logs.db")
cursor = conn.cursor()


logs_in_dir = [f for f in os.listdir(directory_to_extract_to) if os.path.isfile(os.path.join(directory_to_extract_to, f))]
for fileName in logs_in_dir:
    filePath = directory_to_extract_to + fileName
    with open(filePath, "r") as f:
        content = f.read()
        for match in re.finditer(regexPattern, content):
            groups = match.groups() # x:0 y:1 z:2 action:3 username:4 date:5 time:6 block:7\n
            print(f"[{groups[5]} {groups[6]}] {groups[4]} '{groups[3]}' {groups[7]} at {groups[0]} {groups[1]} {groups[2]}")
            logData = {
                'x':        groups[0],
                'y':        groups[1],
                'z':        groups[2],
                'action':   groups[3],
                'username': groups[4],
                'UNIX':     datetime.datetime.strptime(f"{groups[5]} {groups[6]}","%m/%d/%y %H:%M:%S"),
                'block':    groups[7],
            }
            cursor.execute("INSERT INTO overworld VALUES (:x, :y, :z, :action, :username, NULL, :UNIX, :block)", logData)
            conn.commit()
    
# tying up loose ends
conn.close()