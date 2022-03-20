pyenv local 3.10.0
python -m venv .venv
./.venv/Scripts/activate
pip install --upgrade pip
pip install -r requirements.txt

mkdir .data