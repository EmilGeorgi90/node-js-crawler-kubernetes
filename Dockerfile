# syntax=docker/dockerfile:1.7
# ---- base ----
FROM node:22-alpine AS base
ENV PNPM_HOME=/usr/local/pnpm
ENV PATH=$PNPM_HOME:$PATH
RUN corepack enable && apk add --no-cache chromium nss dumb-init
# Puppeteer uses system chromium:
ENV PUPPETEER_SKIP_DOWNLOAD=true
ENV PUPPETEER_EXECUTABLE_PATH=/usr/bin/chromium

WORKDIR /app
COPY package.json pnpm-lock.yaml ./
RUN pnpm fetch
# (installs both prod+dev deps; fine for dev stage)
RUN pnpm install -r --frozen-lockfile

# ---- dev ----
FROM base AS dev
# Copy sources for dev
COPY tsconfig.json ./
COPY src ./src
# Default command can be overridden by compose
ENTRYPOINT ["dumb-init","--"]

# ---- prod builder ----
FROM base AS build
COPY tsconfig.json ./
COPY src ./src
RUN pnpm build

# ---- prod runtime ----
FROM node:22-alpine AS prod
RUN apk add --no-cache chromium nss dumb-init
ENV PUPPETEER_SKIP_DOWNLOAD=true
ENV PUPPETEER_EXECUTABLE_PATH=/usr/bin/chromium
ENV NODE_ENV=production
WORKDIR /app
COPY --from=base /app/node_modules /app/node_modules
COPY package.json ./
COPY --from=build /app/dist /app/dist
ENTRYPOINT ["dumb-init","--"]
