# KONSORT OPC UA Gateway

Python gateway that reads multiple PLCs via OPC UA and Modbus TCP and exposes one OPC UA server for SCADA.

## 1. Clean virtual environment

From PowerShell in the project folder:

```powershell
deactivate
Remove-Item -Recurse -Force .venv
py -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

This is the safest way to remove old/unneeded libraries: delete the old `.venv` and recreate it.

If you want to keep the current `.venv`, remove only the old OPC UA library:

```powershell
pip uninstall opcua
pip uninstall freeopcua
pip install -r requirements.txt
```

Check installed packages:

```powershell
pip list
```

## 2. Configure PLCs

Edit `devices.yaml`.

For each PLC:
- `protocol: opcua` for OPC UA
- `protocol: modbus_tcp` for Modbus TCP

Each PLC gets its own file in the `tags` folder.

## 3. OPC UA tags

Example:

```yaml
tags:
  - name: "Motor_Run"
    nodeid: "ns=4;s=Application.GVL.xMotorRun"
    type: "Boolean"
```

## 4. Modbus tags

Example:

```yaml
tags:
  - name: "Run"
    area: "coil"
    address: 0
    type: "Boolean"

  - name: "Speed"
    area: "holding_register"
    address: 100
    type: "Float32"
    word_order: "big"
```

Supported Modbus areas:
- coil
- discrete_input
- holding_register
- input_register

Supported data types:
- Boolean
- Int16
- UInt16
- Int32
- UInt32
- Float32

IMPORTANT: Modbus register addressing differs between PLC vendors.
The `address` field here is the protocol offset used by PyModbus, usually zero-based.

## 5. Run

```powershell
py .\main.py
```

Default OPC UA output server:

```text
opc.tcp://0.0.0.0:4841/KONSORT/
```

On the same PC in UaExpert:

```text
opc.tcp://localhost:4841/KONSORT/
```

Tree:

```text
Objects
└── KONSORT
    ├── Line_1
    │   ├── _Status
    │   │   ├── Online
    │   │   └── LastError
    │   └── TestBool
    └── Packaging
        ├── _Status
        │   ├── Online
        │   └── LastError
        ├── Run
        ├── Counter
        └── Speed
```

## Notes

- Each controller has an independent connection.
- A failed controller does not stop the gateway.
- The gateway retries disconnected devices automatically.
- `Online` and `LastError` are exposed for each PLC.
