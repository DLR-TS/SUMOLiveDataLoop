# SUMO Live Data Loop

# Setup
## Database inspection
A good idea is to use DBeaver which is cross platform and can handle other databases than oracle as well:
https://dbeaver.io/download/.

## Python
- Create a virtualenv (`python -m venv sumo_ldl_env`) and activate it (`source sumo_ldl_env/bin/activate` or `sumo_ldl_env\Scripts\activate`).
- Install the requirements `python -m pip install -r requirements.txt`.
```
python3 -m venv sumo_ldl_env
. sumo_ldl_env/bin/activate
python3 -m pip install -U pip
git clone https://github.com/DLR-TS/SUMOLiveDataLoop
python3 -m pip install -r SUMOLiveDataLoop/requirements.txt
python3 -m pip install eclipse-sumo
cd SUMOLiveDataLoop
python3 -m pip install -e .
```

# Differences to the original dsp

- checkData.py has been removed (primary focus: PSM interaction)
- scenarioClient.py and scenarioTrigger.py ar enot integrarted (focus: running different scenarios, may be re-added later)
