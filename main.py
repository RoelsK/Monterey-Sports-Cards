import os
from modes import update_store, cdp_mode
from util.resume import detect_last_resume_position
from util.paths import AUTOSAVE_FOLDER   # already defined in your project

RESET  = "\033[0m"
BOLD   = "\033[1m"
DIM    = "\033[2m"

# Colors
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
    clear_screen()
    print("\n" )
    print(f"{BOLD}{CYAN}MONTEREY SPORTS CARDS {RESET}{BOLD}{WHITE}–{RESET} {BOLD}{CYAN}CONTROL PANEL{RESET}")
    print(f"{BLUE}{'=' * 40}{RESET}")
    print("\n" * 0)
    print(f"{CYAN}{BOLD}Make your selection:{RESET}")
    print(f"{BLUE}1){RESET} {BOLD}Main eBay Store (LIVE Listings){RESET}")
    print(f"{BLUE}2){RESET} {BOLD}Card Dealer Pro (CSV Pricing){RESET}")
    print(f"{BLUE}X){RESET} {BOLD}Exit{RESET}")

    # THIS LINE MUST STAY INSIDE THE FUNCTION
    choice = input(f"{CYAN}Enter choice:{RESET} ").strip().lower()

    # THESE MUST ALSO STAY INSIDE run_menu()
    if choice == "1":
        clear_screen()
        update_store.main()

    elif choice == "2":
        clear_screen()
        cdp_mode.main()

    else:
        print(f"{WHITE}Exiting.{RESET}")


if __name__ == "__main__":
    run_menu()