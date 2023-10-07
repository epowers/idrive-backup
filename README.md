# idrive-backup
IDrive Online Backup

(NO LONGER MAINTAINED OR DEVELOPED.)
(IDrive has been so amazing with support, linux aarch64 on OrangePi5 Backups are working.)

sudo apt install python3 python3-pip python3-venv sqlite3
python -m venv .venv

. .venv/bin/activate
pip install build
pip install -e .
python -m build
