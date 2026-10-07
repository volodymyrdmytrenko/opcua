# KONSORT OPC UA Gateway v2.1

Головна зміна: вихідні OPC UA вузли тепер мають явні стабільні String NodeId.

Приклад:

```text
ns=2;s=KONSORT.Line_1
ns=2;s=KONSORT.Line_1._Status.Online
ns=2;s=KONSORT.Line_1.dwZoneReady
ns=2;s=KONSORT.Line_1.xManualMode
```

Namespace index (`ns=2`) може залежати від конфігурації сервера, але string-частина
після `s=` стабільна.

Архітектура:

```text
OPC UA PLC -> Subscription -> Python Gateway -> OPC UA -> SCADA
Modbus PLC -> Polling ------> Python Gateway -> OPC UA -> SCADA
```

Запуск:

```powershell
py .\main.py
```

UaExpert:

```text
opc.tcp://localhost:4841/KONSORT/
```

Після заміни версії бажано видалити старе підключення до Gateway в UaExpert
і додати його заново, щоб браузер не тримав старі NodeId.
