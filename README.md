# CANConv

I created this script to decode this data:
University of Turku. ”CAN bus dataset collected from a heavy-duty truck”. University of Turku, 2021. https://doi.org/10.23729/3160254e-85e9-4268-a636-5b3e54091706

1️⃣ Create a Virtual Environment
    Run the following command:

        python -m venv venv
    This creates a virtual environment named venv in your current directory.

2️⃣ Activate the Virtual Environment

    On Windows (Command Prompt):
        venv\Scripts\activate

    On Windows (PowerShell):
        venv\Scripts\Activate.ps1

    (If execution is restricted, run: Set-ExecutionPolicy Unrestricted -Scope Process)

    On macOS / Linux:
        source venv/bin/activate

3️⃣ Install Dependencies
    pip install -r requirements.txt


4️⃣ Deactivate the Virtual Environment
    When done, deactivate it using:

        deactivate

After running your script, you can access the Dask dashboard (GUI) at:

http://127.0.0.1:8787
