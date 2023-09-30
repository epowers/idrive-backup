# idrive-backup
IDrive Online Backup (work in progress)

sudo apt install python3 python3-pip python3-venv sqlite3
python -m venv .venv

. .venv/bin/activate
pip install build
pip install -e .
python -m build
