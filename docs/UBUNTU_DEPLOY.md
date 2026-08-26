# Ваёбыж на Ubuntu: production-запуск

## Требования к серверу

Для первого production-релиза достаточно:

- Ubuntu Server 22.04 LTS или 24.04 LTS, архитектура x86_64;
- минимум: 2 vCPU, 4 ГБ RAM, 30 ГБ SSD и 2 ГБ swap;
- рекомендуемо: 4 vCPU, 8 ГБ RAM, 60+ ГБ SSD;
- публичный IPv4, домен с A-записью на этот IP;
- открытые входящие порты 22/TCP, 80/TCP, 443/TCP и 443/UDP;
- отдельное место для резервных копий базы, желательно вне сервера.

4 ГБ — разумный минимум для одновременной сборки контейнеров. Если сборка выполняется в CI и на сервер приезжают готовые образы, API и PostgreSQL могут работать на 2 ГБ, но такой размер не рекомендуется для устойчивого production.

## Первый запуск

```bash
sudo mkdir -p /opt/vayobyzh
sudo chown "$USER":"$USER" /opt/vayobyzh
git clone --branch rc-1.0.1 https://github.com/senseigus1-stack/events_soft.git /opt/vayobyzh
cd /opt/vayobyzh
sudo ./deploy/ubuntu/install-docker.sh
sudo usermod -aG docker "$USER"
```

Перезайдите по SSH, затем подготовьте конфигурацию:

```bash
cd /opt/vayobyzh
cp .env.production.example .env
nano .env
chmod 600 .env
./deploy/ubuntu/up.sh
```

Все секреты с `CHANGE_ME` нужно заменить. Быстрый способ создать каждый секрет:

```bash
openssl rand -hex 32
```

## SSL и домен

В `.env` достаточно указать:

```dotenv
DOMAIN=events.example.com
SSL_EMAIL=admin@example.com
CORS_ORIGINS=https://events.example.com
OAUTH_PUBLIC_BASE_URL=https://events.example.com
OAUTH_FRONTEND_URL=https://events.example.com
```

Caddy автоматически получает и продлевает TLS-сертификат, а HTTP перенаправляет на HTTPS. До запуска проверьте DNS и доступность портов 80/443. Данные сертификатов находятся в Docker volume `caddy_data`; команда `down.sh` их не удаляет.

## Вход через внешние платформы

Создайте OAuth-приложения в кабинетах Google, Яндекс ID и GitHub. Для каждого включённого сервиса добавьте точный callback URL из `.env.production.example` и заполните пару `CLIENT_ID` / `CLIENT_SECRET`. Пустая пара безопасно отключает кнопку провайдера.

Ваёбыж не получает пароль пользователя. OAuth использует одноразовый `state`, PKCE и минимальные профильные права, а токены провайдеров не сохраняются в базе.

## Обновление, остановка и backup

```bash
./deploy/ubuntu/update.sh
./deploy/ubuntu/down.sh
./deploy/ubuntu/backup.sh
```

Для автозапуска скопируйте `deploy/systemd/vayobyzh.service` в `/etc/systemd/system/`, затем выполните `sudo systemctl daemon-reload && sudo systemctl enable --now vayobyzh`.

Скрипт backup хранит дампы 14 дней. Для внешнего каталога укажите `VAYOBYZH_BACKUP_DIR`. Не храните единственную копию на том же диске, что PostgreSQL.

## Проверка после запуска

```bash
curl -I https://events.example.com
curl https://events.example.com/healthz
docker compose -f docker-compose.yml -f docker-compose.prod.yml ps
```

Ожидается HTTP 200 и состояние `healthy` у `db`, `api` и `web`. Логи: `docker compose -f docker-compose.yml -f docker-compose.prod.yml logs -f --tail=200`.
