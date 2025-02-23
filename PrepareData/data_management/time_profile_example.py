# pylint: disable=ALL
@profile
def my_function():
    for i in range(1000000):
        pass


if __name__ == "__main__":
    my_function()

"""
add @profile wherever required

Usage
kernprof -l -v -u 1 trash.py
python -m kernprof -l -v -u 1 companywise.py

python -m line_profiler companywise.py.lprof

"""
