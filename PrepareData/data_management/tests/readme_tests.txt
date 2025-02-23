To run for all
Go to data_management folder
python3 -m pytest

To run only a subset of tests, options are
python3 -m pytest tests/test_from_api.py
python3 -m pytest tests/test_from_api.py::test_OpenDaysExchange

