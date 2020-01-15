
Concurrency
===========

General resources on concurrency
    - https://pymotw.com/3/concurrency.html
    - David Beazley
        - http://www.dabeaz.com/talks.html

The Global Interpreter Lock (GIL)
---------------------------------

Any understanding of concurrency in python is not complete without a review
of the GIL (for C-python). These resources `must` be consulted; spending a
few hours with these resources will save countless hours of misunderstanding.
There are few prerequisites, although it might help to know a little about
reference counting and posix threads and locks.

- https://wiki.python.org/moin/GlobalInterpreterLock
- https://realpython.com/python-gil/
- `Understanding the GIL` by David Beazley
    - https://youtu.be/Obt-vMVdM8s
    - https://speakerdeck.com/dabeaz/understanding-the-python-gil
    - http://www.dabeaz.com/GIL/gilvis/index.html
    - .. image:: _static/python_thread_execution_GIL.png
        :target: https://speakerdeck.com/dabeaz/understanding-the-python-gil
- Also listen to Larry Hastings talks, e.g.
    - https://youtu.be/KVKufdTphKs
    - learn a bit about the `GILectomy`
- Last but not least, listen to Raymond Hettinger, PyBay 2017
    - https://youtu.be/9zinZmE3Ogk
- C extensions can release the GIL
    - https://cython.readthedocs.io/en/latest/src/userguide/external_C_code.html#acquiring-and-releasing-the-gil
    - https://scipy-cookbook.readthedocs.io/items/ParallelProgramming.html
