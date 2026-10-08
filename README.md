# KONSORT OPC UA Gateway v3

## Що додано
- MySQL: довідники `devices` і `tags`.
- TimescaleDB: історія тегів у hypertable `tag_history`.
- `onchange` і `cyclic` архівація.
- Асинхронна черга + batch writer для TimescaleDB.
- OPC UA лишається на subscriptions; Modbus — polling.
- Стабільні String NodeId на вихідному OPC UA сервері.

## Встановлення Python-залежностей
```powershell
pip install -r requirements.txt
```

## Перший запуск
За замовчуванням обидві БД у `config.yaml` вимкнені:
```yaml
mysql:
  enabled: false
timescale:
  enabled: false
```
Спочатку перевір шлюз як раніше. Потім увімкнемо БД по одній.

## MySQL
Виконай:
`database/schema/mysql.sql`

## TimescaleDB
TimescaleDB працює поверх PostgreSQL. Створи БД `konsort_history`, потім виконай:
`database/schema/timescale.sql`

## Архівація тега
On change:
```yaml
history:
  enabled: true
  mode: "onchange"
```

Cyclic:
```yaml
history:
  enabled: true
  mode: "cyclic"
  interval_ms: 1000
```

Приклад є у `tags/m241_line1_history_example.yaml`.
