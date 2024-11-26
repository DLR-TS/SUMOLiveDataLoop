# SUMO Live Data Loop

# Setup
## Database inspection
A good idea is to use DBeaver which is cross platform and can handle other databases than oracle as well:
https://dbeaver.io/download/.

## Python
- Create a virtualenv (`python -m venv sumoldlenv`) and activate it (`source sumoldlenv/bin/activate` or `sumoldlenv\Scripts\activate`).
- Install the requirements `python -m pip install -r requirements.txt`.


# Differences to the original dsp

- checkData.py has been removed (primary focus: PSM interaction)
- scenarioClient.py and scenarioTrigger.py ar enot integrarted (focus: running different scenarios, may be re-added later)
