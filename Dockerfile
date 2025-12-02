FROM node:22-slim

# Install system Chromium for Puppeteer
RUN apt-get update && apt-get install -y \
    chromium \
    ca-certificates \
    dumb-init \
    && rm -rf /var/lib/apt/lists/*

ENV PUPPETEER_EXECUTABLE_PATH=/usr/bin/chromium
ENV PUPPETEER_SKIP_DOWNLOAD=true

# pnpm
RUN corepack enable && corepack prepare pnpm@9.12.0 --activate

WORKDIR /app
COPY package.json pnpm-lock.yaml* ./
RUN pnpm install --frozen-lockfile

COPY tsconfig.json ./
COPY src ./src

RUN pnpm build

ENV NODE_ENV=production
ENV HEADLESS=true

ENTRYPOINT ["dumb-init", "--"]
CMD ["node", "dist/src/worker.js"]