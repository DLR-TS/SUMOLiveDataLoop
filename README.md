# SUMO Live Data Loop

This python package allows the repeated execution of [Eclipse SUMO](https://sumo.dlr.de/) in the context of regularly updated
traffic data such as speeds from floating car data or counts and speeds from induction loops.

Currently it requires a Postgres database as a backend which provides regular updates.
The configuration is done with a schema python file which encapsulates the database access (gives details on the queries,
see src/default_schema.py for a template) and a config file in ini format (see default.cfg).

There are two major loops implemented. One for detector data correction and one for the simulation run.

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
- scenarioClient.py and scenarioTrigger.py are not integrarted (focus: running different scenarios, may be re-added later)


# Good to know
- Traffic demand (Routes) for different days fo the weekday (Monday, Tuesday...) can be set under "routesPrefix" in the configurtion file, for example,  ./%(region)s/infra/pkw/pkw_Mon,  ./%(region)s/infra/pkw/pkw_Tue and so on.
- Currently, only one calibrator.rou.xml is used even when traffic demand on different weekdays is considered.
- Only routes, extracted from the given traffic demand, on the edges where detector data are available are considered by the calibrator.
