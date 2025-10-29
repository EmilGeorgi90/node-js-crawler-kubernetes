# --- Build stage ---
FROM node:22-alpine AS build
WORKDIR /app

# Enable pnpm via Corepack and prefetch deps for layer caching
RUN corepack enable && corepack prepare pnpm@latest --activate

# Copy manifest + lockfile first for better caching
COPY package.json ./
COPY pnpm-lock.yaml ./

# Pre-fetch deps to the pnpm store (no node_modules yet)
RUN pnpm fetch

# Now add sources and compile
COPY tsconfig.json ./
COPY src ./src

# Install from the store (offline) and build
RUN pnpm install --offline --frozen-lockfile \
 && pnpm run build

# --- Runtime stage ---
FROM node:22-alpine
WORKDIR /app
ENV NODE_ENV=production

# Install system Chromium & fonts so Puppeteer can launch it
RUN apk add --no-cache \
      chromium \
      nss \
      freetype \
      harfbuzz \
      ca-certificates \
      ttf-freefont
# Tell Puppeteer where Chromium lives
ENV PUPPETEER_EXECUTABLE_PATH=/usr/bin/chromium

# Copy only what we need
COPY --from=build /app/package.json ./
COPY --from=build /app/pnpm-lock.yaml ./
COPY --from=build /app/node_modules ./node_modules
COPY --from=build /app/dist ./dist

# Default to worker, override with CMD in k8s manifests
CMD ["node", "dist/worker.js"]