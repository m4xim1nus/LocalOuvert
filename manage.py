import argparse
import subprocess
from pathlib import Path

def run_all():
    run_communities_extraction()
    run_communities_selection()
    run_datasets_selection()
    run_datafiles_selection()


def run_communities_extraction():
    subprocess.run(["python", "scripts/communities/communities_extraction.py"])

def run_communities_selection():
    subprocess.run(["python", "scripts/communities/communities_selection.py"])

def run_datasets_selection():
    subprocess.run(["python", "scripts/datasets/datasets_selection.py"])

def run_datafiles_selection():
    subprocess.run(["python", "scripts/datasets/datafiles_selection.py"])



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Gestionnaire du projet LocalOuvert")
    parser.add_argument("command", choices=["run_all", "run_communities_extraction", "run_communities_selection", "run_datafiles_selection", "run_datasets_selection"], help="La commande à exécuter.")
    args = parser.parse_args()

    if args.command == "run_communities_extraction":
        run_communities_extraction()
    elif args.command == "run_communities_selection":
        run_communities_selection()
    elif args.command == "run_datafiles_selection":
        run_datafiles_selection()
    elif args.command == "run_datasets_selection":
        run_datasets_selection()
    elif args.command == "run_all":
        run_all()
