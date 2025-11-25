import os
import time
from util.resume import detect_last_resume_position
from util.paths import AUTOSAVE_FOLDER   # already defined in your project
from modes import update_store, cdp_mode
#print(">>> LOADING UPDATE STORE FILE:", update_store.__file__)
#time.sleep(1.5)
#print(">>> LOADING CD MODE FILE:", cdp_mode.__file__)
#time.sleep(1.5)
from pricing import pricing_engine
#print(">>> LOADING PRICING ENGINE FILE:", pricing_engine.__file__)
#time.sleep(1.5)

RESET  = "\033[0m"
BOLD   = "\033[1m"
DIM    = "\033[2m"

RED    = "\033[31m"
GREEN  = "\033[32m"
YELLOW = "\033[33m"
BLUE   = "\033[34m"
MAGENTA= "\033[35m"
CYAN   = "\033[36m"
WHITE  = "\033[37m"


def clear_screen():
   os.system("cls" if os.name == "nt" else "clear")


def run_menu():
    while True:    # <-- THE FIX: wrap whole menu in a loop
        clear_screen()
        print("\n")
        print(f"{BOLD}{CYAN}MONTEREY SPORTS CARDS {RESET}{BOLD}{WHITE}–{RESET} {BOLD}{CYAN}MAIN MENU{RESET}")
        print(f"{BLUE}{'=' * 33}{RESET}")
        print("\n" * 0)
        print(f"{CYAN}{BOLD}Make your selection:{RESET}")
        print(f"{BLUE}1){RESET} {BOLD}Main eBay Store (LIVE Listings){RESET}")
        print(f"{BLUE}2){RESET} {BOLD}Card Dealer Pro (CSV Pricing){RESET}")
        print(f"{BLUE}X){RESET} {BOLD}Exit{RESET}")

        choice = input(f"{CYAN}Enter choice:{RESET} ").strip().lower()

        if choice == "1":
            clear_screen()
            update_store.main()   # pricing engine runs, then RETURNS here
            continue              # <-- Important: redisplay the menu

        elif choice == "2":
            clear_screen()
            cdp_mode.main()       # after CDP, redisplay menu
            continue

        elif choice == "x":
            print(f"{WHITE}Exiting.{RESET}")
            return                # <-- exit the whole program

        else:
            print(f"{RED}Invalid selection. Try again.{RESET}")
            input("Press Enter to continue...")

if __name__ == "__main__":
    run_menu()
