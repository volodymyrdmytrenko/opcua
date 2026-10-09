# KONSORT PLC REST Gateway v4

## Архітектура
```text
OPC UA PLC -> Subscription --┐
Modbus PLC -> Polling -------+--> Python REST Gateway
                                   ├--> RAM cache
                                   ├--> PostgreSQL
                                   └--> HTTP REST API
```

Python більше не є OPC UA Server.

## API
Тільки адресна робота з конкретним тегом.

### GET
```http
GET /api/tags/AAA
```

### PUT
```http
PUT /api/tags/BBB
Content-Type: application/json

{"value":100}
```

`GET /api/tags` навмисно відсутній.

## Теги
```yaml
- name: "AAA"
  nodeid: "ns=2;s=SR_Main.qOverflowReadyToReceive (%QX0.0)"
  type: "Boolean"
  writable: false
```

```yaml
- name: "BBB"
  nodeid: "ns=2;s=SR_Main.SomeSetpoint"
  type: "UInt32"
  writable: true
```

REST-імена тегів глобально унікальні.

## PostgreSQL
Єдина БД. SQL схема:
`database/schema.sql`

Таблиці:
- devices
- tags
- tag_history
- write_log

За замовчуванням PostgreSQL вимкнений:
```yaml
postgres:
  enabled: false
```

## Історія
```yaml
history:
  enabled: true
  mode: "onchange"
```

або:
```yaml
history:
  enabled: true
  mode: "cyclic"
  interval_ms: 1000
```

GET читає актуальне значення з RAM cache, не з PostgreSQL.

## Встановлення
```powershell
pip install -r requirements.txt
```

## Запуск
```powershell
py .\run.py
```

API:
```text
http://localhost:8080
```

Swagger:
```text
http://localhost:8080/docs
```

PowerShell GET:
```powershell
Invoke-RestMethod http://localhost:8080/api/tags/dwZoneReady
```

PowerShell PUT:
```powershell
Invoke-RestMethod -Method Put `
  -Uri http://localhost:8080/api/tags/BBB `
  -ContentType "application/json" `
  -Body '{"value":123}'
```

Коди:
- 200 success
- 403 read-only
- 404 tag not found
- 422 invalid value type
- 502 PLC write failed
- 503 device/tag unavailable
