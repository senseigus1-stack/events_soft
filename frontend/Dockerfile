FROM node:24-alpine AS build

ENV CI=true
RUN corepack enable
WORKDIR /app
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml ./
RUN pnpm install --frozen-lockfile
ARG VITE_PUBLIC_URL=http://localhost:8080
ENV VITE_PUBLIC_URL=${VITE_PUBLIC_URL}
COPY . .
RUN pnpm test
RUN pnpm build

FROM nginx:1.29-alpine
COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/dist /usr/share/nginx/html
EXPOSE 80
