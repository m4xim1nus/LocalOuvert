import argparse
import subprocess

def runtest():
    subprocess.run(["python", "scripts/idf_scraper.py"])

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Gestionnaire du projet LocalOuvert")
    parser.add_argument("command", choices=["runtest"], help="La commande à exécuter.")
    args = parser.parse_args()

    if args.command == "runtest":
        runtest()