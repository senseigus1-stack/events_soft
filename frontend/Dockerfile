FROM node:24-alpine AS build

ARG RELEASE_VERSION=rc-1.0.3
ARG VCS_REF=unknown

ENV CI=true
RUN corepack enable
WORKDIR /app
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml ./
RUN pnpm install --frozen-lockfile
ARG VITE_PUBLIC_URL=__PUBLIC_URL__
ENV VITE_PUBLIC_URL=${VITE_PUBLIC_URL}
COPY . .
RUN pnpm test
RUN pnpm build

FROM nginx:1.29-alpine

ARG RELEASE_VERSION=rc-1.0.3
ARG VCS_REF=unknown

LABEL org.opencontainers.image.title="Vayobyzh Web" \
      org.opencontainers.image.description="Multilingual Vayobyzh event discovery interface" \
      org.opencontainers.image.source="https://github.com/senseigus1-stack/events_soft" \
      org.opencontainers.image.version="${RELEASE_VERSION}" \
      org.opencontainers.image.revision="${VCS_REF}"

COPY nginx-main.conf /etc/nginx/nginx.conf
COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/dist/client /usr/share/nginx/html
RUN chown -R nginx:nginx /var/cache/nginx /var/run /usr/share/nginx/html
USER nginx
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD wget -qO- http://127.0.0.1:8080/healthz || exit 1
