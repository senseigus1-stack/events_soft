# Ваёбыж в Kubernetes

Helm chart в `deploy/helm/vayobyzh` разворачивает web, API, Кытчи-workers, миграции, Ingress/TLS, NetworkPolicy, probes, PDB и опциональный HPA. Секреты не хранятся в chart values и не попадают в историю Helm.

## Какой вариант выбрать

### Один новый Linux-сервер

Подходит для первого релиза и умеренной нагрузки: K3s, встроенные Traefik и local-path storage, один PostgreSQL StatefulSet. Минимум — 2 vCPU, 4 ГБ RAM и 50 ГБ SSD; рекомендуемо — 4 vCPU, 8 ГБ RAM и 80+ ГБ SSD. Локальный диск и один узел не дают высокой доступности, поэтому дампы обязательно копировать во внешнее хранилище.

### Настоящий HA production

Используйте минимум три control-plane узла, два worker-узла, внешний managed PostgreSQL с PITR, CSI storage с репликацией и внешний балансировщик. Рекомендуемый стартовый размер узла приложения — 4 vCPU/8 ГБ RAM. `deploy/kubernetes/values.production.example.yaml` включает три реплики и HPA; размеры уточняются после нагрузочного теста.

## До установки

- Ubuntu 24.04 LTS или Debian 13, x86_64/arm64, SSD и синхронизация времени;
- A/AAAA-запись домена указывает на сервер или load balancer;
- извне открыты 80/TCP и 443/TCP, SSH ограничен доверенными адресами;
- Kubernetes API 6443/TCP не публикуется всему интернету;
- для multi-node K3s между узлами разрешены порты из документации K3s, включая 6443/TCP и Flannel 8472/UDP;
- контейнеры `events_soft-api` и `events_soft-web` в GHCR доступны кластеру. Для публичного сайта проще один раз сделать packages публичными; для private packages задайте `imagePullSecrets`.

GitHub Actions собирает два образа при push в `rc-*`. Первый workflow должен завершиться успешно до первого деплоя.

## Установка на чистый сервер

```bash
sudo mkdir -p /opt/vayobyzh
sudo chown "$USER":"$USER" /opt/vayobyzh
git clone --branch rc-1.0.3 https://github.com/senseigus1-stack/events_soft.git /opt/vayobyzh
cd /opt/vayobyzh
sudo ./deploy/kubernetes/install-k3s.sh
```

Скрипт ставит K3s из поддерживаемого канала `v1.36`, сохраняет закрытый kubeconfig пользователю и устанавливает Helm с проверкой SHA-256. Версии можно переопределить: `sudo K3S_CHANNEL=v1.36 HELM_VERSION=v4.2.4 ./deploy/kubernetes/install-k3s.sh`.

Перезайдите по SSH и подготовьте секреты:

```bash
cd /opt/vayobyzh
cp .env.kubernetes.example .env.kubernetes
chmod 600 .env.kubernetes
openssl rand -hex 32
nano .env.kubernetes
```

Один и тот же случайный пароль должен находиться в `POSTGRES_PASSWORD` и URL-encoded части `DATABASE_URL`. Затем:

```bash
./deploy/kubernetes/deploy.sh
./deploy/kubernetes/status.sh
```

`deploy.sh` безопасно повторяем: он применяет Secret, устанавливает или обновляет cert-manager 1.21, затем выполняет атомарный `helm upgrade --install`. При неготовом rollout Helm откатывает релиз.

## Внешний PostgreSQL

Для production-кластера создайте базу и пользователя у провайдера, включите TLS, автоматические backups/PITR и заполните `DATABASE_URL` в `.env.kubernetes`. Не включайте `POSTGRES_*`, если ими управляет отдельный оператор секретов. Пример URL:

```dotenv
DATABASE_URL=postgresql+psycopg://vayobyzh:URL_ENCODED_PASSWORD@db.example.net:5432/vayobyzh?sslmode=require
```

Создайте Secret и установите chart:

```bash
./deploy/kubernetes/create-secret.sh .env.kubernetes
helm upgrade --install vayobyzh ./deploy/helm/vayobyzh \
  -n vayobyzh --create-namespace \
  -f deploy/kubernetes/values.production.example.yaml \
  --set-string ingress.host=events.example.com \
  --atomic --timeout 15m
```

ClusterIssuer может быть общим для всего кластера. Тогда заранее установите cert-manager и issuer, а `certManager.createClusterIssuer` оставьте `false`. На одиночном сервере bootstrap создаёт `letsencrypt-production` автоматически.

## Миграции и обновление

Перед запуском каждого API/worker init-контейнер выполняет `alembic upgrade head`. PostgreSQL advisory lock не позволяет двум репликам менять схему одновременно. Обновление:

```bash
cd /opt/vayobyzh
git pull --ff-only origin rc-1.0.3
VAYOBYZH_IMAGE_TAG=rc-1.0.3 ./deploy/kubernetes/deploy.sh
kubectl rollout status deployment -n vayobyzh --timeout=10m
```

Для полностью неизменяемого rollout запишите digest обоих образов в values вместо tag. Откат:

```bash
helm history vayobyzh -n vayobyzh
./deploy/kubernetes/rollback.sh PREVIOUS_REVISION
```

Откат приложения не отменяет миграции БД автоматически. Миграции должны быть обратно совместимыми по схеме expand/migrate/contract.

## Проверка, диагностика и TLS

```bash
kubectl get pods,ingress,certificate -n vayobyzh
kubectl logs -n vayobyzh deployment/vayobyzh-api --tail=200
kubectl logs -n vayobyzh deployment/vayobyzh-sync --tail=200
curl -fsS https://events.example.com/healthz
curl -fsSI https://events.example.com
```

Если сертификат не готов, проверьте DNS, доступность 80/443 и `kubectl describe certificate -n vayobyzh`. Не переключайтесь на production ACME многократно: у Let's Encrypt есть rate limits; для отладки временно используйте staging server в values.

## Резервное копирование

В single-node values CronJob каждый день создаёт custom-format dump и удаляет локальные копии старше 14 дней:

```bash
kubectl get cronjob,jobs -n vayobyzh
kubectl create job -n vayobyzh --from=cronjob/vayobyzh-backup backup-manual
```

PVC не является внешним backup. Настройте снапшоты у провайдера или копирование dump в S3-совместимое хранилище и регулярно проверяйте восстановление. Для managed PostgreSQL используйте его PITR и независимый логический dump.

## Production-чеклист

- GitHub Actions зелёный, образы привязаны к digest и packages доступны кластеру;
- секреты переданы через Secret/external-secrets, `.env.kubernetes` имеет mode 600 и не закоммичен;
- OAuth callback URL совпадают с `https://DOMAIN/api/v1/auth/oauth/.../callback`;
- TLS `Ready=True`, HTTP перенаправляется на HTTPS, DNS не содержит старых адресов;
- внешний PostgreSQL имеет TLS, PITR и проверенное восстановление;
- есть метрики узлов/pods, централизованные логи и алерты на 5xx, restarts, заполнение диска и срок сертификата;
- проведены smoke- и нагрузочный тесты, заданы requests/limits и capacity запас не меньше 30%;
- задокументированы ответственные, ротация секретов, rollback и аварийное восстановление.
