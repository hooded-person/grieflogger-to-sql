import time, sys

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

def draw_menu(options,name=None):
    if name:
        print(name)
    for i, option in enumerate(options):
        print(f"{i}. {option}")

def option_menu(options,name=None):
    while True:
        draw_menu(options,name=name)
        choice = input()
        try:
            choice = int(choice)
        except:
            print("Not a valid choice")
        if choice in range(len(options)):
            return (choice, options[choice])
        print(f"{bcolors.WARNING}Not a valid choice{bcolors.ENDC}")
        time.sleep(1)
        
    


def main():
    (choice, option) = option_menu(["inspect","player","overview","exit"],name="QUERY DATABASE")
    match choice:
        case 3:
            print("Exiting...")
            sys.exit(0)
    print("doin shi")

if __name__ == "__main__":
    main()